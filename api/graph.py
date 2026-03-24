"""Module to handle the graph data loading into the database."""

import asyncio
import json
import logging
from itertools import combinations
from typing import Any, Dict, List

from litellm import completion
from pydantic import BaseModel

from api.config import Config
from api.extensions import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# pylint: disable=broad-exception-caught

class collectionDescription(BaseModel):
    """collection Description"""

    name: str
    description: str


class ColumnDescription(BaseModel):
    """Column Description"""

    name: str
    description: str


class Descriptions(BaseModel):
    """List of collections"""

    collections_descriptions: list[collectionDescription]
    fields_descriptions: list[ColumnDescription]


async def get_db_description(graph_id: str) -> tuple[str, str]:
    """Get the database description from the graph."""
    graph = db.select_graph(graph_id)
    query_result = await graph.query(
        """
        MATCH (d:Database)
        RETURN d.description, d.url
        """
    )

    if not query_result.result_set:
        return ("No description available for this database.",
                "No URL available for this database.")

    return (query_result.result_set[0][0],
            query_result.result_set[0][1])  # Return the first result's description


async def get_user_rules(graph_id: str) -> str:
    """Get the user rules from the graph."""
    graph = db.select_graph(graph_id)
    query_result = await graph.query(
        """
        MATCH (d:Database)
        RETURN d.user_rules
        """
    )

    if not query_result.result_set or not query_result.result_set[0][0]:
        return ""

    return query_result.result_set[0][0]


async def set_user_rules(graph_id: str, user_rules: str) -> None:
    """Set the user rules in the graph."""
    graph = db.select_graph(graph_id)
    await graph.query(
        """
        MERGE (d:Database)
        SET d.user_rules = $user_rules
        """,
        {"user_rules": user_rules}
    )

async def _query_graph(
    graph,
    query: str,
    params: Dict[str, Any] = None,
    timeout: int = 300
) -> List[Any]:
    """
    Run a graph query asynchronously and return the result set.

    Args:
        graph: The graph database instance.
        query: The query string to execute.
        params: Optional parameters for the query.
        timeout: Query timeout in seconds.

    Returns:
        The result set from the query.
    """
    result = await graph.query(query, params or {}, timeout=timeout)
    return result.result_set

async def _find_collections(
    graph,
    embeddings: List[List[float]]
) -> List[Dict[str, Any]]:
    """
    Find collections based on pre-computed embeddings.

    Args:
        graph: The graph database instance.
        embeddings: Pre-computed embeddings for the collection descriptions.

    Returns:
        List of matching collection information.
    """
    query = """
        CALL db.idx.vector.queryNodes('Collection','embedding',3,vecf32($embedding))
        YIELD node, score
        MATCH (node)-[:BELONGS_TO]-(columns:Column)
        RETURN node.name, node.description, node.foreign_keys, collect({
            columnName: columns.name,
            description: columns.description,
            dataType: columns.type,
            keyType: columns.key_type,
            nullable: columns.nullable
        })
    """

    tasks = [
        _query_graph(graph, query, {"embedding": embedding})
        for embedding in embeddings
    ]

    results = await asyncio.gather(*tasks)
    return [row for rows in results for row in rows]


async def _find_collections_by_columns(
    graph,
    embeddings: List[List[float]]
) -> List[Dict[str, Any]]:
    """
    Find collections based on pre-computed embeddings for column descriptions.

    Args:
        graph: The graph database instance.
        embeddings: Pre-computed embeddings for the column descriptions.

    Returns:
        List of matching collection information.
    """
    query = """
        CALL db.idx.vector.queryNodes('Column','embedding',3,vecf32($embedding))
        YIELD node, score
        MATCH (node:Column)-[:BELONGS_TO]-(collection:Collection)-[:BELONGS_TO]-(columns:Column)
        RETURN
            collection.name,
            collection.description,
            collection.foreign_keys,
            collect({
                columnName: columns.name,
                description: columns.description,
                dataType: columns.type,
                keyType: columns.key_type,
                nullable: columns.nullable
            })
    """

    tasks = [
        _query_graph(graph, query, {"embedding": embedding})
        for embedding in embeddings
    ]

    results = await asyncio.gather(*tasks)
    return [row for rows in results for row in rows]


async def _find_collections_sphere(
    graph,
    collections: List[str]
) -> List[Dict[str, Any]]:
    """
    Find collections in the sphere of influence of given collections.

    Args:
        graph: The graph database instance.
        collections: List of collection names to find connections for.

    Returns:
        List of connected collection information.
    """
    query = """
        MATCH (node:Collection {name: $name})
        MATCH (node)-[:BELONGS_TO]-(column)-[:REFERENCES]-()-[:BELONGS_TO]-(collection_ref)
        WITH collection_ref
        MATCH (collection_ref)-[:BELONGS_TO]-(columns)
        RETURN collection_ref.name, collection_ref.description, collection_ref.foreign_keys,
               collect({
                   columnName: columns.name,
                   description: columns.description,
                   dataType: columns.type,
                   keyType: columns.key_type,
                   nullable: columns.nullable
               })
    """
    try:
        tasks = [_query_graph(graph, query, {"name": name}) for name in collections]
        results = await asyncio.gather(*tasks)
    except Exception as e:
        logging.error("Error finding collections in sphere: %s", e)
        results = []

    return [row for rows in results for row in rows]


async def _find_connecting_collections(
    graph,
    collection_names: List[str]
) -> List[Dict[str, Any]]:
    """
    Find all collections that form connections between pairs of collections.

    Args:
        graph: The graph database instance.
        collection_names: List of collection names to find connections between.

    Returns:
        List of connecting collection information.
    """
    pairs = [list(pair) for pair in combinations(collection_names, 2)]
    if not pairs:
        return []

    query = """
    UNWIND $pairs AS pair
    MATCH (a:Collection {name: pair[0]})
    MATCH (b:Collection {name: pair[1]})
    WITH a, b
    MATCH p = allShortestPaths((a)-[*..6]-(b))
    UNWIND nodes(p) AS path_node
    WITH DISTINCT path_node
    WHERE 'Collection' IN labels(path_node) OR
          ('Column' IN labels(path_node) AND path_node.key_type = 'PRI')
    WITH path_node,
         'Collection' IN labels(path_node) AS is_collection,
         'Column' IN labels(path_node) AND path_node.key_type = 'PRI' AS is_pri_column
    OPTIONAL MATCH (path_node)-[:BELONGS_TO]->(parent_collection:Collection)
    WHERE is_pri_column
    WITH CASE
           WHEN is_collection THEN path_node
           WHEN is_pri_column THEN parent_collection
           ELSE null
         END AS target_collection
    WHERE target_collection IS NOT NULL
    WITH DISTINCT target_collection
    MATCH (col:Column)-[:BELONGS_TO]->(target_collection)
    WITH target_collection,
         collect({
            columnName: col.name,
            description: col.description,
            dataType: col.type,
            keyType: col.key_type,
            nullable: col.nullable
         }) AS columns
    RETURN target_collection.name, target_collection.description, target_collection.foreign_keys, columns
    """
    try:
        result = await _query_graph(graph, query, {"pairs": pairs}, timeout=500)
    except Exception as e:
        logging.error("Error finding connecting collections: %s", e)
        result = []

    return result


async def find( # pylint: disable=too-many-locals
    graph_id: str,
    queries_history: List[str],
    db_description: str = None
) -> List[List[Any]]:
    """
    Find the collections and columns relevant to the user's query.

    Args:
        graph_id: The identifier for the graph database.
        queries_history: List of previous queries, with the last one being current.
        db_description: Optional description of the database.

    Returns:
        Combined list of relevant collections.
    """
    graph = db.select_graph(graph_id)
    user_query = queries_history[-1]
    previous_queries = queries_history[:-1]

    logging.info("Calling LLM to find relevant collections/columns for query")

    completion_result = completion(
        model=Config.COMPLETION_MODEL,
        response_format=Descriptions,
        messages=[
            {
                "role": "system",
                "content": Config.FIND_SYSTEM_PROMPT.format(
                    db_description=db_description
                )
            },
            {
                "role": "user",
                "content": json.dumps({
                    "previous_user_queries": previous_queries,
                    "user_query": user_query
                })
            },
        ],
        temperature=0,
    )

    json_data = json.loads(completion_result.choices[0].message.content)
    descriptions = Descriptions(**json_data)
    descriptions_text = ([desc.description for desc in descriptions.collections_descriptions] +
                         [desc.description for desc in descriptions.fields_descriptions])
    if not descriptions_text:
        return []

    embedding_results = Config.EMBEDDING_MODEL.embed(descriptions_text)

    # Split embeddings back into collection and field embeddings
    collection_embeddings = embedding_results[:len(descriptions.collections_descriptions)]
    field_embeddings = embedding_results[len(descriptions.collections_descriptions):]

    main_tasks = []

    if collection_embeddings:
        main_tasks.append(_find_collections(graph, collection_embeddings))
    if field_embeddings:
        main_tasks.append(_find_collections_by_columns(graph, field_embeddings))

    # Execute the main embedding-based searches in parallel
    results = await asyncio.gather(*main_tasks)

    # Unpack results based on what tasks we ran
    collections_des = []
    collections_by_fields_des = []
    
    idx = 0
    if collection_embeddings:
        collections_des = results[idx]
        idx += 1
    if field_embeddings:
        collections_by_fields_des = results[idx]
        idx += 1

    # Extract collection names once for reuse
    found_collection_names = [t[0] for t in collections_des] if collections_des else []

    # Only run sphere and connecting searches if we found collections
    if found_collection_names:
        secondary_tasks = [
            _find_collections_sphere(graph, found_collection_names),
            _find_connecting_collections(graph, found_collection_names)
        ]
        collections_by_sphere, collections_by_route = await asyncio.gather(*secondary_tasks)
    else:
        collections_by_sphere, collections_by_route = [], []

    combined_collections = _get_unique_collections(
        collections_des + collections_by_fields_des + collections_by_route + collections_by_sphere
    )

    return combined_collections

def _get_unique_collections(collections_list):
    # Dictionary to store unique collections with the collection name as the key
    unique_collections = {}

    for collection_info in collections_list:
        collection_name = collection_info[0]  # The first element is the collection name

        # Only add if this collection name hasn't been seen before
        try:
            if collection_name not in unique_collections:
                collection_info[3] = [dict(od) for od in collection_info[3]]
                collection_info[2] = "Foreign keys: " + collection_info[2]
                unique_collections[collection_name] = collection_info
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error: {collection_info}, Exception: {e}")

    # Return the values (the unique collection info lists)
    return list(unique_collections.values())
