"""MongoDB loader for loading database schemas into FalkorDB graphs."""

import datetime
import json
import logging
import re
from typing import AsyncGenerator, Dict, Any, List, Tuple
from collections import Counter, defaultdict
from urllib.parse import urlparse

import tqdm

from api.loaders.base_loader import BaseLoader
from api.loaders.graph_loader import load_to_graph

try:
    from bson import ObjectId, Decimal128, Int64, json_util
except ImportError:
    ObjectId = None
    Decimal128 = None
    Int64 = None
    json_util = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class MongoDBQueryError(Exception):
    """Exception raised for MongoDB query execution errors."""


class MongoDBConnectionError(Exception):
    """Exception raised for MongoDB connection errors."""


# MongoDB type to descriptive type mapping
BSON_TYPE_MAP = {
    "str": "string",
    "int": "integer",
    "float": "double",
    "bool": "boolean",
    "list": "array",
    "dict": "object",
    "NoneType": "null",
    "datetime": "date",
    "ObjectId": "ObjectId",
    "Decimal128": "decimal",
    "Int64": "int64",
    "bytes": "binary",
}


class MongoDBLoader(BaseLoader):
    """
    Loader for MongoDB databases that connects and extracts schema information
    by sampling documents from each collection.
    """

    # Operations that modify the database schema/structure
    SCHEMA_MODIFYING_OPERATIONS = {
        'createCollection', 'drop', 'renameCollection',
        'createIndex', 'dropIndex', 'dropIndexes',
        'create', 'collMod'
    }

    @staticmethod
    def _execute_sample_query(
        cursor, table_name: str, col_name: str, sample_size: int = 3
    ) -> List[Any]:
        """
        Execute query to get random sample values for a field.
        MongoDB implementation using $sample aggregation stage.
        """
        # For MongoDB, cursor is actually a database object
        db = cursor
        collection = db[table_name]
        pipeline = [
            {"$match": {col_name: {"$ne": None, "$exists": True}}},
            {"$sample": {"size": sample_size}},
            {"$project": {col_name: 1, "_id": 0}}
        ]
        results = list(collection.aggregate(pipeline))
        return [
            MongoDBLoader._serialize_value(doc.get(col_name))
            for doc in results
            if doc.get(col_name) is not None
        ]

    @staticmethod
    def _serialize_value(value):
        """
        Convert non-JSON serializable MongoDB values to JSON serializable format.

        Args:
            value: The value to serialize

        Returns:
            JSON serializable version of the value
        """
        if value is None:
            return None
        if ObjectId and isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, datetime.datetime):
            return value.isoformat()
        if isinstance(value, datetime.date):
            return value.isoformat()
        if Decimal128 and isinstance(value, Decimal128):
            return float(str(value))
        if Int64 and isinstance(value, Int64):
            return int(value)
        if isinstance(value, bytes):
            return "<binary data>"
        if isinstance(value, dict):
            return {k: MongoDBLoader._serialize_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [MongoDBLoader._serialize_value(item) for item in value]
        return value

    @staticmethod
    def _parse_mongodb_url(connection_url: str) -> Dict[str, str]:
        """
        Parse MongoDB connection URL into components.

        Args:
            connection_url: MongoDB connection URL in format:
                          mongodb://username:password@host:port/database
                          mongodb+srv://username:password@host/database

        Returns:
            Dict with connection parameters
        """
        parsed = urlparse(connection_url)

        if parsed.scheme not in ('mongodb', 'mongodb+srv'):
            raise ValueError(
                "Invalid MongoDB URL format. Expected "
                "mongodb://... or mongodb+srv://..."
            )

        # Extract database name from path
        database = parsed.path.lstrip('/')
        if '?' in database:
            database = database.split('?')[0]

        if not database:
            raise ValueError("MongoDB URL must include database name")

        return {
            'url': connection_url,
            'database': database,
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 27017,
        }

    @staticmethod
    def _get_field_type(value) -> str:
        """Map a Python/BSON value to a descriptive type string."""
        if value is None:
            return "null"
        type_name = type(value).__name__
        return BSON_TYPE_MAP.get(type_name, type_name)

    @staticmethod
    async def load(prefix: str, connection_url: str) -> AsyncGenerator[tuple[bool, str], None]:
        """
        Load the schema from a MongoDB database into the graph database.

        Args:
            prefix: User prefix for graph namespacing
            connection_url: MongoDB connection URL

        Yields:
            Tuple[bool, str]: Success status and message
        """
        try:
            import pymongo  # pylint: disable=import-outside-toplevel

            # Parse connection URL
            conn_params = MongoDBLoader._parse_mongodb_url(connection_url)
            db_name = conn_params['database']

            # Connect to MongoDB with timeout
            logging.info(f"Connecting to MongoDB: {connection_url.split('@')[-1]}")
            yield True, f"Connecting to database '{db_name}'..."

            client = pymongo.MongoClient(connection_url, serverSelectionTimeoutMS=5000)
            db = client[db_name]

            # Test connection
            try:
                client.server_info()
                logging.info("MongoDB connection successful")
            except Exception as e:
                logging.error(f"MongoDB connection failed: {e}")
                yield False, f"Connection failed: {str(e)}"
                return

            # Get all collection information
            yield True, "Extracting collection information..."
            entities = MongoDBLoader.extract_collections_info(db)

            # Get relationship information
            yield True, "Extracting relationship information..."
            relationships = MongoDBLoader.extract_relationships(db, entities)

            # Close database connection
            client.close()

            # Load data into graph
            yield True, "Loading data into graph..."
            await load_to_graph(
                f"{prefix}_{db_name}", entities, relationships,
                db_name=db_name, db_url=connection_url
            )

            yield True, (
                f"MongoDB schema loaded successfully. "
                f"Found {len(entities)} collections."
            )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("Error loading MongoDB schema: %s", e)
            yield False, f"Failed to load MongoDB database schema: {str(e)}"

    @staticmethod
    def extract_collections_info(
        db, sample_size: int = 10
    ) -> Dict[str, Any]:
        """
        Extract collection and field information from MongoDB database
        by sampling documents from each collection.

        Args:
            db: pymongo Database object
            sample_size: Number of documents to sample per collection

        Returns:
            Dict containing collection information (mapped to table-like format)
        """
        entities = {}

        collection_names = [
            name for name in db.list_collection_names()
            if not name.startswith('system.')
        ]

        for collection_name in tqdm.tqdm(
            collection_names, desc="Extracting collection information"
        ):
            collection = db[collection_name]
            logging.info(f"Analyzing collection {collection_name}...")

            # Simple find with limit is much faster than aggregate $sample
            # Add a timeout to avoid hanging on specific collections
            try:
                sample_docs = list(collection.find().limit(sample_size).max_time_ms(2000))
            except Exception as e:
                logging.error(f"Failed to fetch sample from {collection_name}: {e}")
                sample_docs = []
                logging.error(f"Failed to fetch sample from {collection_name}: {e}")
                sample_docs = []

            if not sample_docs:
                # Empty collection - still register it
                entities[collection_name] = {
                    'description': f"Collection: {collection_name} (empty)",
                    'columns': {},
                    'foreign_keys': [],
                    'col_descriptions': []
                }
                continue

            # Analyze fields across all sampled documents
            columns_info = MongoDBLoader._analyze_fields(
                db, collection_name, sample_docs
            )

            # Generate collection description
            doc_count = collection.estimated_document_count()
            table_description = (
                f"Collection: {collection_name} "
                f"(~{doc_count} documents)"
            )

            # Get column descriptions for batch embedding
            col_descriptions = [
                col_info['description'] for col_info in columns_info.values()
            ]

            # Detect foreign key-like references
            foreign_keys = MongoDBLoader._detect_field_references(
                collection_name, columns_info
            )

            entities[collection_name] = {
                'description': table_description,
                'columns': columns_info,
                'foreign_keys': foreign_keys,
                'col_descriptions': col_descriptions
            }

        return entities

    @staticmethod
    def _analyze_fields(
        db, collection_name: str, sample_docs: List[Dict]
    ) -> Dict[str, Any]:
        """
        Analyze fields across sampled documents to build schema information.

        Args:
            db: Database object
            collection_name: Name of the collection
            sample_docs: List of sampled documents

        Returns:
            Dict of field information keyed by field name
        """
        # Track field types and occurrence counts
        field_types: Dict[str, Counter] = defaultdict(Counter)
        field_count: Counter = Counter()
        total_docs = len(sample_docs)

        for doc in sample_docs:
            MongoDBLoader._extract_field_types(doc, "", field_types, field_count)

        columns_info = {}
        for field_path, type_counter in field_types.items():
            # Determine the primary type (most common)
            most_common_type = type_counter.most_common(1)[0][0]
            occurrence_ratio = field_count[field_path] / total_docs

            # Determine nullability and key types
            is_nullable = 'YES' if occurrence_ratio < 1.0 else 'NO'
            key_type = 'NONE'
            if field_path == '_id':
                key_type = 'PRIMARY KEY'
                is_nullable = 'NO'
            elif (most_common_type == 'ObjectId' or field_path.endswith('Id')) and field_path.endswith(('_id', 'Id')):
                key_type = 'FOREIGN KEY'

            # Build description
            description_parts = [f"Field {field_path} of type {most_common_type}"]
            if len(type_counter) > 1:
                other_types = [f"{t}({c})" for t, c in type_counter.most_common() if t != most_common_type]
                description_parts.append(f"(also seen as: {', '.join(other_types)})")
            if key_type != 'NONE':
                description_parts.append(f"({key_type})")
            if is_nullable == 'NO':
                description_parts.append("(NOT NULL)")

            # Extract sample values from the already fetched docs instead of redundant DB hits
            sample_values = []
            for d in sample_docs:
                val = None
                if '.' in field_path:
                    # Handle nested dot notation
                    parts = field_path.split('.')
                    curr = d
                    for part in parts:
                        if isinstance(curr, dict):
                            curr = curr.get(part)
                        else:
                            curr = None
                            break
                    val = curr
                else:
                    val = d.get(field_path)
                
                if val is not None:
                    ser_val = MongoDBLoader._serialize_value(val)
                    if ser_val not in sample_values:
                        sample_values.append(ser_val)
                
                if len(sample_values) >= 3:
                    break

            columns_info[field_path] = {
                'type': most_common_type,
                'null': is_nullable,
                'key': key_type,
                'description': ' '.join(description_parts),
                'default': None,
                'sample_values': [str(v) for v in sample_values] if sample_values else []
            }

        return columns_info

    @staticmethod
    def _extract_field_types(
        doc: Dict, prefix: str,
        field_types: Dict[str, Counter],
        field_count: Counter,
        max_depth: int = 3
    ):
        """
        Recursively extract field types from a document.

        Args:
            doc: The document to analyze
            prefix: Current field path prefix
            field_types: Counter dict for tracking types per field
            field_count: Counter for tracking field occurrences
            max_depth: Maximum nesting depth to explore
        """
        if max_depth <= 0:
            return

        for key, value in doc.items():
            field_path = f"{prefix}.{key}" if prefix else key
            field_type = MongoDBLoader._get_field_type(value)

            field_types[field_path][field_type] += 1
            field_count[field_path] += 1

            # Recurse into nested documents (but not arrays)
            if isinstance(value, dict) and max_depth > 1:
                MongoDBLoader._extract_field_types(
                    value, field_path, field_types, field_count,
                    max_depth - 1
                )

    @staticmethod
    def _detect_field_references(
        collection_name: str, columns_info: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """
        Detect potential foreign key references based on field naming conventions.

        Args:
            collection_name: Name of the current collection
            columns_info: Column information for the collection

        Returns:
            List of foreign key-like reference dictionaries
        """
        foreign_keys = []

        for field_name, field_info in columns_info.items():
            field_type = field_info.get('type', '')

            # Detect ObjectId reference fields (e.g., user_id, userId, authorId)
            is_ref = False
            ref_collection = None

            if field_type == 'ObjectId' and field_name != '_id':
                is_ref = True
            elif field_name.endswith('_id') and field_name != '_id':
                is_ref = True
            elif field_name.endswith('Id') and field_name != '_id':
                is_ref = True

            if is_ref:
                # Try to infer the referenced collection name
                if field_name.endswith('_id'):
                    ref_collection = field_name[:-3]  # user_id -> user
                elif field_name.endswith('Id'):
                    ref_collection = field_name[:-2]  # userId -> user
                else:
                    ref_collection = field_name

                # Pluralize as a guess (MongoDB collections are often plural)
                ref_collection_plural = ref_collection + 's'

                foreign_keys.append({
                    'constraint_name': f"ref_{collection_name}_{field_name}",
                    'column': field_name,
                    'referenced_table': ref_collection_plural,
                    'referenced_column': '_id'
                })

        return foreign_keys

    @staticmethod
    def extract_relationships(
        db, entities: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, str]]]:
        """
        Extract relationship information based on detected references.

        Args:
            db: pymongo Database object
            entities: Extracted collection information

        Returns:
            Dict of relationships keyed by constraint name
        """
        collection_names = set(entities.keys())
        relationships = {}

        for collection_name, collection_info in entities.items():
            for fk in collection_info.get('foreign_keys', []):
                ref_table = fk.get('referenced_table', '')

                # Only create relationship if the referenced collection exists
                # Check both singular and plural forms
                if ref_table in collection_names:
                    constraint_name = fk['constraint_name']
                    if constraint_name not in relationships:
                        relationships[constraint_name] = []

                    relationships[constraint_name].append({
                        'from': collection_name,
                        'to': ref_table,
                        'source_column': fk['column'],
                        'target_column': '_id',
                        'note': f"MongoDB reference: {fk['column']} -> {ref_table}._id"
                    })

        return relationships

    @staticmethod
    def is_schema_modifying_query(query_str: str) -> Tuple[bool, str]:
        """
        Check if a MongoDB query (JSON) modifies the database schema.

        Args:
            query_str: The MongoDB query string to check (JSON format)

        Returns:
            Tuple of (is_schema_modifying, operation_type)
        """
        if not query_str:
            return False, ""
        
        if isinstance(query_str, str) and not query_str.strip():
            return False, ""

        try:
            query = json.loads(query_str) if isinstance(query_str, str) else query_str

            if isinstance(query, dict):
                operation = query.get('operation', '').lower()
                if operation in ('create_collection', 'drop', 'create_index',
                                 'drop_index', 'rename'):
                    return True, operation

            return False, ""
        except (json.JSONDecodeError, AttributeError):
            return False, ""

    @staticmethod
    async def refresh_graph_schema(
        graph_id: str, db_url: str
    ) -> Tuple[bool, str]:
        """
        Refresh the graph schema by clearing existing data and reloading
        from the MongoDB database.

        Args:
            graph_id: The graph ID to refresh
            db_url: MongoDB connection URL

        Returns:
            Tuple of (success, message)
        """
        try:
            logging.info("Schema modification detected. Refreshing graph schema.")

            from api.extensions import db as falkor_db  # pylint: disable=import-outside-toplevel

            # Clear existing graph data
            graph = falkor_db.select_graph(graph_id)
            await graph.delete()

            # Extract prefix from graph_id
            parts = graph_id.split('_')
            if len(parts) >= 2:
                prefix = '_'.join(parts[:-1])
            else:
                prefix = graph_id

            # Reuse the existing load method to reload the schema
            async for success, message in MongoDBLoader.load(prefix, db_url):
                if not success:
                    return False, message

            logging.info("Graph schema refreshed successfully.")
            return True, "Graph schema refreshed successfully"

        except Exception as e:  # pylint: disable=broad-exception-caught
            logging.error("Error refreshing graph schema: %s", str(e))
            return False, "Error refreshing graph schema"

    @staticmethod
    def execute_query(query_str: str, db_url: str) -> List[Dict[str, Any]]:
        """
        Execute a MongoDB query and return the results.
        Supports find, aggregate, insert, update, delete operations.

        The query is expected as a JSON string with the following format:
        {
            "collection": "collection_name",
            "operation": "find|aggregate|insert|update|delete|count",
            "filter": {...},           // for find/update/delete
            "projection": {...},       // for find
            "pipeline": [...],         // for aggregate
            "document": {...},         // for insert (single)
            "documents": [...],        // for insert (many)
            "update": {...},           // for update
            "sort": {...},             // for find
            "limit": N,               // for find
            "skip": N                  // for find
        }

        Args:
            query_str: MongoDB query as JSON string
            db_url: MongoDB connection URL

        Returns:
            List of dictionaries containing the query results
        """
        try:
            import pymongo  # pylint: disable=import-outside-toplevel

            conn_params = MongoDBLoader._parse_mongodb_url(db_url)
            client = pymongo.MongoClient(db_url)
            db = client[conn_params['database']]

            # Parse the query
            try:
                if isinstance(query_str, str):
                    if json_util:
                        query = json_util.loads(query_str)
                    else:
                        query = json.loads(query_str)
                else:
                    query = query_str
                    # If it's already a dict, re-process with json_util to handle $oid, etc.
                    if json_util and isinstance(query, dict):
                        # This canonicalizes Extended JSON markers in the dictionary
                        query = json_util.loads(json_util.dumps(query))
            except json.JSONDecodeError as je:
                raise MongoDBQueryError(f"Invalid JSON query: {str(je)}") from je

            collection_name = query.get('collection', '')
            operation = query.get('operation', 'find').lower()

            if not collection_name:
                raise MongoDBQueryError("Query must specify a 'collection' name")

            collection = db[collection_name]

            result_list = []

            if operation == 'find':
                filter_doc = query.get('filter', {})
                projection = query.get('projection', None)
                sort = query.get('sort', None)
                limit = query.get('limit', 0)
                skip = query.get('skip', 0)

                cursor = collection.find(filter_doc, projection)

                if sort:
                    sort_list = [(k, v) for k, v in sort.items()]
                    cursor = cursor.sort(sort_list)
                if limit:
                    cursor = cursor.limit(min(limit, 50))
                else:
                    cursor = cursor.limit(50)
                if skip:
                    cursor = cursor.skip(skip)

                for doc in cursor:
                    serialized = {
                        k: MongoDBLoader._serialize_value(v)
                        for k, v in doc.items()
                    }
                    result_list.append(serialized)

            elif operation == 'aggregate':
                pipeline = query.get('pipeline', [])
                if not isinstance(pipeline, list):
                    raise MongoDBQueryError("Aggregate pipeline must be a list")

                # Add a safety $limit to the pipeline if not already present
                has_limit = any('$limit' in stage for stage in pipeline)
                if not has_limit:
                    pipeline.append({'$limit': 50})

                for doc in collection.aggregate(pipeline):
                    serialized = {
                        k: MongoDBLoader._serialize_value(v)
                        for k, v in doc.items()
                    }
                    result_list.append(serialized)

            elif operation == 'count':
                filter_doc = query.get('filter', {})
                count = collection.count_documents(filter_doc)
                result_list = [{"count": count}]

            elif operation in ('insert', 'insert_one'):
                document = query.get('document', {})
                result = collection.insert_one(document)
                result_list = [{
                    "operation": "INSERT",
                    "inserted_id": str(result.inserted_id),
                    "status": "success"
                }]

            elif operation == 'insert_many':
                documents = query.get('documents', [])
                result = collection.insert_many(documents)
                result_list = [{
                    "operation": "INSERT_MANY",
                    "inserted_count": len(result.inserted_ids),
                    "status": "success"
                }]

            elif operation in ('update', 'update_one'):
                filter_doc = query.get('filter', {})
                update_doc = query.get('update', {})
                result = collection.update_one(filter_doc, update_doc)
                result_list = [{
                    "operation": "UPDATE",
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "status": "success"
                }]

            elif operation == 'update_many':
                filter_doc = query.get('filter', {})
                update_doc = query.get('update', {})
                result = collection.update_many(filter_doc, update_doc)
                result_list = [{
                    "operation": "UPDATE_MANY",
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "status": "success"
                }]

            elif operation in ('delete', 'delete_one'):
                filter_doc = query.get('filter', {})
                result = collection.delete_one(filter_doc)
                result_list = [{
                    "operation": "DELETE",
                    "deleted_count": result.deleted_count,
                    "status": "success"
                }]

            elif operation == 'delete_many':
                filter_doc = query.get('filter', {})
                result = collection.delete_many(filter_doc)
                result_list = [{
                    "operation": "DELETE_MANY",
                    "deleted_count": result.deleted_count,
                    "status": "success"
                }]

            elif operation == 'distinct':
                field = query.get('field', '')
                filter_doc = query.get('filter', {})
                values = collection.distinct(field, filter_doc)
                result_list = [{"distinct_values": [
                    MongoDBLoader._serialize_value(v) for v in values
                ]}]

            else:
                raise MongoDBQueryError(
                    f"Unsupported operation: {operation}. "
                    f"Use find, aggregate, count, insert, update, or delete."
                )

            client.close()
            return result_list

        except MongoDBQueryError:
            raise
        except Exception as e:
            if 'client' in locals():
                client.close()
            logging.error("MongoDB query execution error: %s", e)
            raise MongoDBQueryError(
                f"MongoDB query execution error: {str(e)}"
            ) from e
