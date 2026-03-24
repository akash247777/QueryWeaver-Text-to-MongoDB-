"""Graph loader module for loading data into graph databases."""

import json
import asyncio
import time
import tqdm

from api.config import Config
from api.extensions import db
from api.utils import generate_db_description, create_combined_description


async def load_to_graph(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
    graph_id: str,
    entities: dict,
    relationships: dict,
    batch_size: int = 100,
    db_name: str = "TBD",
    db_url: str = "",
) -> None:
    """
    Load the graph data into the database.
    It gets the Graph name as an argument and expects

    Input:
    - entities: A dictionary containing the entities and their attributes.
    - relationships: A dictionary containing the relationships between entities.
    - batch_size: The size of the batch for embedding.
    - db_name: The name of the database.
    """
    graph = db.select_graph(graph_id)
    embedding_model = Config.EMBEDDING_MODEL
    vec_len = embedding_model.get_vector_size()

    create_combined_description(entities)

    try:
        # Create vector indices
        await graph.query(
            """
            CREATE VECTOR INDEX FOR (t:Collection) ON (t.embedding)
            OPTIONS {dimension:$size, similarityFunction:'euclidean'}
        """,
            {"size": vec_len},
        )

        await graph.query(
            """
            CREATE VECTOR INDEX FOR (c:Column) ON (c.embedding)
            OPTIONS {dimension:$size, similarityFunction:'euclidean'}
        """,
            {"size": vec_len},
        )
        await graph.query("CREATE INDEX FOR (p:Collection) ON (p.name)")
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Error creating vector indices: {str(e)}")

    db_des = generate_db_description(db_name=db_name, table_names=list(entities.keys()))
    await graph.query(
        """
        CREATE (d:Database {
            name: $db_name,
            description: $description,
            url: $url
        })
        """,
        {"db_name": db_name, "description": db_des, "url": db_url},
    )

    # Optimization: Batch embedding for all tables at once
    table_names = list(entities.keys())
    table_descriptions = [entities[name]["description"] for name in table_names]
    
    # Gemini embedding API allows max 100 items per batch
    MAX_EMBED_BATCH = 100

    print(f"Creating embeddings for {len(table_names)} tables...")
    table_embeddings = []
    for i in range(0, len(table_descriptions), MAX_EMBED_BATCH):
        batch = table_descriptions[i : i + MAX_EMBED_BATCH]
        table_embeddings.extend(embedding_model.embed(batch))
        if i + MAX_EMBED_BATCH < len(table_descriptions):
            time.sleep(1)  # Rate-limit delay between batches

    # Create table nodes concurrently
    table_tasks = []
    for idx, table_name in enumerate(table_names):
        table_info = entities[table_name]
        fk = json.dumps(table_info.get("foreign_keys", []))
        table_tasks.append(graph.query(
            """
            CREATE (t:Collection {
                name: $table_name,
                description: $description,
                embedding: vecf32($embedding),
                foreign_keys: $foreign_keys
            })
            """,
            {
                "table_name": table_name,
                "description": table_info["description"],
                "embedding": table_embeddings[idx],
                "foreign_keys": fk,
            },
        ))
    
    # Insert table nodes in small chunks to avoid overwhelming FalkorDB
    GRAPH_CHUNK = 10
    for i in range(0, len(table_tasks), GRAPH_CHUNK):
        await asyncio.gather(*table_tasks[i : i + GRAPH_CHUNK])

    # Optimization: Collect all columns across all tables to batch embeddings
    all_columns = [] # List of (table_name, col_name, col_info)
    all_col_descriptions = []
    
    for table_name, table_info in entities.items():
        for col_name, col_info in table_info["columns"].items():
            all_columns.append((table_name, col_name, col_info))
            all_col_descriptions.append(col_info["description"])

    print(f"Creating embeddings for {len(all_col_descriptions)} columns...")
    # Embed columns in batches (max 100 per Gemini API limit)
    all_col_embeddings = []
    embed_batch = min(batch_size, MAX_EMBED_BATCH)
    total_batches = (len(all_col_descriptions) + embed_batch - 1) // embed_batch
    for batch_idx, i in enumerate(range(0, len(all_col_descriptions), embed_batch)):
        batch = all_col_descriptions[i : i + embed_batch]
        print(f"  Embedding batch {batch_idx + 1}/{total_batches} ({len(batch)} items)...")
        for attempt in range(3):  # Retry up to 3 times
            try:
                all_col_embeddings.extend(embedding_model.embed(batch))
                break
            except Exception as e:
                if attempt < 2:
                    wait_time = 2 ** (attempt + 1)  # 2s, 4s
                    print(f"  Embedding batch failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    raise
        # Rate-limit delay between batches
        if i + embed_batch < len(all_col_descriptions):
            time.sleep(1)

    # Create column nodes concurrently in chunks to avoid overwhelming the graph
    print(f"Inserting {len(all_columns)} column nodes into the graph...")
    chunk_size = 50
    for i in range(0, len(all_columns), chunk_size):
        chunk = all_columns[i : i + chunk_size]
        chunk_embeddings = all_col_embeddings[i : i + chunk_size]
        
        col_tasks = []
        for j, (table_name, col_name, col_info) in enumerate(chunk):
            final_description = col_info["description"]
            sample_values = col_info.get("sample_values", [])
            if sample_values:
                sample_values_str = f"(Sample values: {', '.join(f'({v})' for v in sample_values)})"
                final_description = f"{final_description} {sample_values_str}"

            col_tasks.append(graph.query(
                """
                MATCH (t:Collection {name: $table_name})
                CREATE (c:Column {
                    name: $col_name,
                    type: $type,
                    nullable: $nullable,
                    key_type: $key,
                    description: $description,
                    embedding: vecf32($embedding)
                })-[:BELONGS_TO]->(t)
                """,
                {
                    "table_name": table_name,
                    "col_name": col_name,
                    "type": col_info.get("type", "unknown"),
                    "nullable": col_info.get("null", "unknown"),
                    "key": col_info.get("key", "unknown"),
                    "description": final_description,
                    "embedding": chunk_embeddings[j],
                },
            ))
        # Insert column nodes in small chunks to avoid FalkorDB pending query limit
        for ci in range(0, len(col_tasks), GRAPH_CHUNK):
            await asyncio.gather(*col_tasks[ci : ci + GRAPH_CHUNK])

    # Create relationships (now tables and columns exist)
    print("Creating relationships...")
    for rel_name, table_info in relationships.items():
        for rel in table_info:
            source_table = rel["from"]
            source_field = rel["source_column"]
            target_table = rel["to"]
            target_field = rel["target_column"]
            note = rel.get("note", "")

            # Create relationship if both tables and columns exist
            try:
                await graph.query(
                    """
                    MATCH (src:Column {name: $source_col})
                        -[:BELONGS_TO]->(source:Collection {name: $source_table})
                    MATCH (tgt:Column {name: $target_col})
                        -[:BELONGS_TO]->(target:Collection {name: $target_table})
                    CREATE (src)-[:REFERENCES {
                        rel_name: $rel_name,
                        note: $note
                    }]->(tgt)
                    """,
                    {
                        "source_col": source_field,
                        "target_col": target_field,
                        "source_table": source_table,
                        "target_table": target_table,
                        "rel_name": rel_name,
                        "note": note,
                    },
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"Warning: Could not create relationship: {str(e)}")
                continue
