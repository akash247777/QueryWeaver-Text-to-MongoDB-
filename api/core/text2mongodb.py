"""Graph-related routes for the text-to-MongoDB query API."""
# pylint: disable=line-too-long,trailing-whitespace

import asyncio
import json
import logging
import os
import time

from pydantic import BaseModel
from redis import ResponseError

from api.core.errors import GraphNotFoundError, InternalError, InvalidArgumentError
from api.core.schema_loader import load_database
from api.agents import AnalysisAgent, RelevancyAgent, ResponseFormatterAgent, FollowUpAgent
from api.agents.healer_agent import HealerAgent
from api.config import Config
from api.extensions import db
from api.graph import find, get_db_description, get_user_rules
from api.loaders.mongodb_loader import MongoDBLoader
from api.memory.graphiti_tool import MemoryTool


# Use the same delimiter as in the JavaScript
MESSAGE_DELIMITER = "|||QUERYWEAVER_MESSAGE_BOUNDARY|||"

GENERAL_PREFIX = os.getenv("GENERAL_PREFIX")

class GraphData(BaseModel):
    """Graph data model.

    Args:
        BaseModel (_type_): _description_
    """
    database: str


class ChatRequest(BaseModel):
    """Chat request model.

    Args:
        BaseModel (_type_): _description_
    """
    chat: list[str]
    result: list[str] | None = None
    instructions: str | None = None
    use_user_rules: bool = True  # If True, fetch rules from database; if False, don't use rules
    use_memory: bool = True


class ConfirmRequest(BaseModel):
    """Confirmation request model.

    Args:
        BaseModel (_type_): _description_
    """
    mongodb_query: str
    confirmation: str = ""
    chat: list = []
    use_user_rules: bool = True


def get_database_type_and_loader(db_url: str):
    """
    Determine the database type from URL and return appropriate loader class.

    Args:
        db_url: Database connection URL

    Returns:
        tuple: (database_type, loader_class)
    """
    if not db_url or db_url == "No URL available for this database.":
        return None, None

    db_url_lower = db_url.lower()

    if db_url_lower.startswith('mongodb://') or db_url_lower.startswith('mongodb+srv://'):
        return 'mongodb', MongoDBLoader

    return None, None

def sanitize_query(query: str) -> str:
    """Sanitize the query to prevent injection attacks."""
    return query.replace('\n', ' ').replace('\r', ' ')[:500]

def sanitize_log_input(value: str) -> str:
    """
    Sanitize input for safe logging—remove newlines, 
    carriage returns, tabs, and wrap in repr().
    """
    if not isinstance(value, str):
        value = str(value)

    return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

def _graph_name(user_id: str, graph_id:str) -> str:

    graph_id = graph_id.strip()[:200]
    if not graph_id:
        raise GraphNotFoundError("Invalid graph_id, must be less than 200 characters.")

    if GENERAL_PREFIX and graph_id.startswith(GENERAL_PREFIX):
        return graph_id

    return f"{user_id}_{graph_id}"

async def get_schema(user_id: str, graph_id: str):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Return all nodes and edges for the specified database schema (namespaced to the user).

    This endpoint returns a JSON object with two keys: `nodes` and `edges`.
    Nodes contain a minimal set of properties (id, name, labels, props).
    Edges contain source and target node names (or internal ids), type and props.
    
        args:
            graph_id (str): The ID of the graph to query (the database name).
    """
    namespaced = _graph_name(user_id, graph_id)
    try:
        graph = db.select_graph(namespaced)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error("Failed to select graph %s: %s", sanitize_log_input(namespaced), e)
        raise GraphNotFoundError("Graph not found or database error") from e

    # Build collection nodes with columns and collection-to-collection links (foreign keys)
    tables_query = """
    MATCH (t:Collection)
    OPTIONAL MATCH (c:Column)-[:BELONGS_TO]->(t)
    RETURN t.name AS table, collect(DISTINCT {name: c.name, type: c.type}) AS columns
    """

    links_query = """
    MATCH (src_col:Column)-[:BELONGS_TO]->(src_table:Collection),
          (tgt_col:Column)-[:BELONGS_TO]->(tgt_table:Collection),
          (src_col)-[:REFERENCES]->(tgt_col)
    RETURN DISTINCT src_table.name AS source, tgt_table.name AS target
    """

    try:
        tables_res = (await graph.query(tables_query)).result_set
        links_res = (await graph.query(links_query)).result_set
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.error("Error querying graph data for %s: %s", sanitize_log_input(namespaced), e)
        raise InternalError("Failed to read graph data") from e

    nodes = []
    for row in tables_res:
        try:
            table_name, columns = row
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        # Normalize columns: ensure a list of dicts with name/type
        if not isinstance(columns, list):
            columns = [] if columns is None else [columns]

        normalized = []
        for col in columns:
            try:
                # col may be a mapping-like object or a simple value
                if not col:
                    continue
                # Some drivers may return a tuple or list for the collected map
                if isinstance(col, (list, tuple)) and len(col) >= 2:
                    # try to interpret as (name, type)
                    name = col[0]
                    ctype = col[1] if len(col) > 1 else None
                elif isinstance(col, dict):
                    name = col.get('name') or col.get('columnName')
                    ctype = col.get('type') or col.get('dataType')
                else:
                    name = str(col)
                    ctype = None

                if not name:
                    continue

                normalized.append({"name": name, "type": ctype})
            except Exception:  # pylint: disable=broad-exception-caught
                continue

        nodes.append({
            "id": table_name,
            "name": table_name,
            "columns": normalized,
        })

    links = []
    seen = set()
    for row in links_res:
        try:
            source, target = row
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        key = (source, target)
        if key in seen:
            continue
        seen.add(key)
        links.append({"source": source, "target": target})

    return {"nodes": nodes, "links": links}

async def query_database(user_id: str, graph_id: str, chat_data: ChatRequest):  # pylint: disable=too-many-statements
    """
    Query the Database with the given graph_id and chat_data.
    
        Args:
            graph_id (str): The ID of the graph to query.
            chat_data (ChatRequest): The chat data containing user queries and context.
    """
    graph_id = _graph_name(user_id, graph_id)

    queries_history = chat_data.chat if hasattr(chat_data, 'chat') else None
    result_history = chat_data.result if hasattr(chat_data, 'result') else None
    instructions = chat_data.instructions if hasattr(chat_data, 'instructions') else None
    use_user_rules = chat_data.use_user_rules if hasattr(chat_data, 'use_user_rules') else True

    if not queries_history or not isinstance(queries_history, list):
        raise InvalidArgumentError("Invalid or missing chat history")

    if len(queries_history) == 0:
        raise InvalidArgumentError("Empty chat history")

    # Truncate history to keep only the last N questions maximum (configured in Config)
    if len(queries_history) > Config.SHORT_MEMORY_LENGTH:
        queries_history = queries_history[-Config.SHORT_MEMORY_LENGTH:]
        # Keep corresponding results (one less than queries since current query has no result yet)
        if result_history and len(result_history) > 0:
            max_results = Config.SHORT_MEMORY_LENGTH - 1
            if max_results > 0:
                result_history = result_history[-max_results:]
            else:
                result_history = []

    logging.info("User Query: %s", sanitize_query(queries_history[-1]))

    if chat_data.use_memory:
        memory_tool_task = asyncio.create_task(MemoryTool.create(user_id, graph_id))
    else:
        memory_tool_task = None

    # Create a generator function for streaming
    async def generate():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        # Start overall timing
        overall_start = time.perf_counter()
        logging.info("Starting query processing pipeline for query: %s",
                     sanitize_query(queries_history[-1]))  # nosemgrep

        agent_rel = RelevancyAgent(queries_history, result_history)
        agent_an = AnalysisAgent(queries_history, result_history)
        follow_up_agent = FollowUpAgent(queries_history, result_history)

        step = {"type": "reasoning_step",
                "final_response": False,
                "message": "Step 1: Analyzing user query and generating MongoDB query..."}
        yield json.dumps(step) + MESSAGE_DELIMITER
        # Ensure the database description is loaded
        db_description, db_url = await get_db_description(graph_id)
        # Fetch user rules from database only if toggle is enabled
        user_rules_spec = await get_user_rules(graph_id) if use_user_rules else None

        # Determine database type and get appropriate loader
        db_type, loader_class = get_database_type_and_loader(db_url)

        if not loader_class:
            overall_elapsed = time.perf_counter() - overall_start
            logging.info("Query processing failed (no loader) - Total time: %.2f seconds",
                         overall_elapsed)
            yield json.dumps({
                "type": "error",
                "final_response": True,
                "message": "Unable to determine database type"
            }) + MESSAGE_DELIMITER
            return

        # Start both tasks concurrently
        find_task = asyncio.create_task(find(graph_id, queries_history, db_description))

        relevancy_task = asyncio.create_task(agent_rel.get_answer(
            queries_history[-1], db_description
        ))

        logging.info("Starting relevancy check and graph analysis concurrently")

        # Wait for relevancy check first
        answer_rel = await relevancy_task

        if answer_rel["status"] != "On-topic": # pylint: disable=too-many-nested-blocks
            # Cancel the find task since query is off-topic
            find_task.cancel()
            try:
                await find_task
            except asyncio.CancelledError:
                logging.info("Find task cancelled due to off-topic query")

            step = {
                "type": "followup_questions",
                "final_response": True,
                "message": "Off topic question: " + answer_rel["reason"],
            }
            logging.info("Query fail reason: %s", answer_rel["reason"])  # nosemgrep
            yield json.dumps(step) + MESSAGE_DELIMITER
            # Total time for off-topic query
            overall_elapsed = time.perf_counter() - overall_start
            logging.info("Query processing completed (off-topic) - Total time: %.2f seconds",
                         overall_elapsed)
        else:
            # Query is on-topic, wait for find results
            result = await find_task

            logging.info("Calling to analysis agent with query: %s",
                         sanitize_query(queries_history[-1]))  # nosemgrep
            
            memory_context = None
            if memory_tool_task:
                memory_tool = await memory_tool_task
                memory_context = await memory_tool.search_memories(
                    query=queries_history[-1]
                )

            # Step 2: Analyze user question (is it on-topic? what collection?)
            answer_an = agent_an.get_analysis(
                user_query=queries_history[-1],
                combined_collections=result,
                db_description=db_description,
                instructions=instructions,
                memory_context=memory_context,
                database_type=db_type,
                user_rules_spec=user_rules_spec
            )

            is_translatable = answer_an.get("is_query_translatable", False)
            mongodb_query = answer_an.get("mongodb_query", "")
            follow_up_result = ""
            execution_error = False

            logging.info("Generated MongoDB query: %s", answer_an.get('mongodb_query', ''))
            yield json.dumps({
                "type": "mongodb_query",
                "content": answer_an.get("mongodb_query") or "",
                "mongodb_query": answer_an.get("mongodb_query") or "",
                "conf": answer_an.get("confidence"),
                "miss": answer_an.get("missing_information"),
                "query_analysis": answer_an.get("query_analysis", "Summary of the business intent and how it maps to MongoDB."),
                "explanation": answer_an.get("explanation", "A natural language answer/explanation of what the query will retrieve."),
                "is_query_translatable": answer_an.get("is_query_translatable", True),
                "final_response": False,
            }) + MESSAGE_DELIMITER

            # Initialize variables for memory tool
            user_readable_response = None
            follow_up_result = None
            execution_error = None

            # If the query is valid, execute it using the configured database and db_url
            if is_translatable:
                # MongoDB queries are ready to be used
                pass

                # Check if this is a destructive operation that requires confirmation
                query_to_check = answer_an["mongodb_query"]
                op_type = 'FIND'
                if db_type == 'mongodb':
                        try:
                            import json as _json
                            query_to_process = query_to_check
                            if isinstance(query_to_process, str):
                                mongo_query = _json.loads(query_to_process)
                            else:
                                mongo_query = query_to_process
                                query_to_check = _json.dumps(query_to_process) # Normalize to string
                            
                            mongo_op = mongo_query.get('operation', 'find').lower()
                            destructive_mongo_ops = {'insert', 'insert_one', 'insert_many',
                                                     'update', 'update_one', 'update_many',
                                                     'delete', 'delete_one', 'delete_many',
                                                     'drop'}
                            op_type = mongo_op.upper() if mongo_op in destructive_mongo_ops else 'FIND'
                        except Exception:
                            op_type = 'FIND'
                            if not isinstance(query_to_check, str):
                                import json as _json
                                query_to_check = _json.dumps(query_to_check)

                destructive_ops = ['INSERT', 'UPDATE', 'DELETE', 'DROP',
                                  'INSERT_MANY', 'UPDATE_MANY', 'DELETE_MANY',
                                  'INSERT_ONE', 'UPDATE_ONE', 'DELETE_ONE']
                is_destructive = op_type in destructive_ops
                general_graph = graph_id.startswith(GENERAL_PREFIX) if GENERAL_PREFIX else False
                if is_destructive and not general_graph:
                    # This is a destructive operation - ask for user confirmation
                    query_label = "MongoDB Query"
                    confirmation_message = f"""⚠️ DESTRUCTIVE OPERATION DETECTED ⚠️

The generated query will perform a **{op_type}** operation:

{query_label}:
{query_to_check}

What this will do:
"""
                    if op_type in ('INSERT', 'INSERT_ONE', 'INSERT_MANY'):
                        confirmation_message += "• Add new data to the database"
                    elif op_type in ('UPDATE', 'UPDATE_ONE', 'UPDATE_MANY'):
                        confirmation_message += ("• Modify existing data in the "
                                                "database")
                    elif op_type in ('DELETE', 'DELETE_ONE', 'DELETE_MANY'):
                        confirmation_message += ("• **PERMANENTLY DELETE** data "
                                                "from the database")
                    elif op_type == 'DROP':
                        confirmation_message += ("• **PERMANENTLY DELETE** entire "
                                                "tables or database objects")
                    
                    confirmation_message += """

⚠️ WARNING: This operation will make changes to your database and may be irreversible.
"""

                    yield json.dumps(
                        {
                            "type": "destructive_confirmation",
                            "message": confirmation_message,
                            "mongodb_query": query_to_check,
                            "operation_type": op_type,
                            "final_response": False,
                        }
                    ) + MESSAGE_DELIMITER
                    # Log end-to-end time for destructive operation that requires confirmation
                    overall_elapsed = time.perf_counter() - overall_start
                    logging.info(
                        "Query processing halted for confirmation - Total time: %.2f seconds",
                        overall_elapsed
                    )
                    return  # Stop here and wait for user confirmation

                try:
                    if is_destructive and general_graph:
                        yield json.dumps(
                            {
                                "type": "error", 
                                "final_response": True, 
                                "message": "Destructive operation not allowed on demo graphs"
                            }) + MESSAGE_DELIMITER
                    else:
                        step = {"type": "reasoning_step",
                                "final_response": False,
                                "message": "Step 2: Executing MongoDB query"}
                        yield json.dumps(step) + MESSAGE_DELIMITER

                        # Check if this query modifies the database schema
                        # using the appropriate loader
                        is_schema_modifying, operation_type = (
                            loader_class.is_schema_modifying_query(answer_an["mongodb_query"])
                        )

                        # Try executing the query
                        try:
                            if db_type == 'mongodb':
                                query_results = loader_class.execute_query(
                                    answer_an["mongodb_query"],
                                    db_url
                                )
                            else:
                                raise InternalError("Only MongoDB is supported")
                        except Exception as exec_error:  # pylint: disable=broad-exception-caught
                            # Initial execution failed - start iterative healing process
                            step = {
                                "type": "reasoning_step",
                                "final_response": False,
                                "message": "Step 2a: Query execution failed, attempting to heal query..."
                            }
                            yield json.dumps(step) + MESSAGE_DELIMITER

                            # Create healer agent and attempt iterative healing
                            healer_agent = HealerAgent(max_healing_attempts=3)
                            
                            # Create a wrapper function for query execution
                            if db_type == 'mongodb':
                                def execute_query(q: str):
                                    return loader_class.execute_query(q, db_url)
                            
                            healing_result = healer_agent.heal_and_execute(
                                initial_mongodb=answer_an["mongodb_query"],
                                initial_error=str(exec_error),
                                execute_query_func=execute_query,
                                db_description=db_description,
                                question=queries_history[-1],
                                database_type=db_type
                            )
                            
                            if not healing_result.get("success"):
                                # Healing failed after all attempts
                                yield json.dumps({
                                    "type": "healing_failed",
                                    "final_response": False,
                                    "message": f"❌ Failed to heal query after {healing_result['attempts']} attempt(s)",
                                    "final_error": healing_result.get("final_error", str(exec_error)),
                                    "healing_log": healing_result.get("healing_log", [])
                                }) + MESSAGE_DELIMITER
                                raise exec_error
                            
                            # Healing succeeded!
                            healing_log = healing_result.get("healing_log", [])
                            
                            # Show healing progress
                            for log_entry in healing_log:
                                if log_entry.get("status") == "healed":
                                    changes_msg = ", ".join(log_entry.get("changes_made", []))
                                    yield json.dumps({
                                        "type": "healing_attempt",
                                        "final_response": False,
                                        "message": f"Attempt {log_entry['attempt']}: {changes_msg}",
                                        "attempt": log_entry["attempt"],
                                        "changes": log_entry.get("changes_made", []),
                                        "confidence": log_entry.get("confidence", 0)
                                    }) + MESSAGE_DELIMITER
                            
                            # Update the query to the healed version
                            answer_an["mongodb_query"] = healing_result["mongodb_query"]
                            query_results = healing_result["query_results"]
                            
                            yield json.dumps({
                                "type": "healing_success",
                                "final_response": False,
                                "message": f"✅ Query healed and executed successfully after {healing_result['attempts'] + 1} attempt(s)",
                                "healed_mongodb": healing_result["mongodb_query"],
                                "attempts": healing_result["attempts"] + 1
                            }) + MESSAGE_DELIMITER

                        if len(query_results) != 0:
                            yield json.dumps(
                                {
                                    "type": "query_result",
                                    "data": query_results,
                                    "final_response": False
                                }
                            ) + MESSAGE_DELIMITER

                        # If schema was modified, refresh the graph using the appropriate loader
                        if is_schema_modifying:
                            step = {"type": "reasoning_step",
                                    "final_response": False,
                                    "message": ("Step 3: Schema change detected - "
                                                "refreshing graph...")}
                            yield json.dumps(step) + MESSAGE_DELIMITER

                            refresh_result = await loader_class.refresh_graph_schema(
                                graph_id, db_url)
                            refresh_success, refresh_message = refresh_result

                            if refresh_success:
                                refresh_msg = (f"✅ Schema change detected "
                                            f"({operation_type} operation)\n\n"
                                            f"🔄 Graph schema has been automatically "
                                            f"refreshed with the latest database "
                                            f"structure.")
                                yield json.dumps(
                                    {
                                        "type": "schema_refresh",
                                        "final_response": False,
                                        "message": refresh_msg,
                                        "refresh_status": "success"
                                    }
                                ) + MESSAGE_DELIMITER
                            else:
                                failure_msg = (f"⚠️ Schema was modified but graph "
                                            f"refresh failed: {refresh_message}")
                                yield json.dumps(
                                    {
                                        "type": "schema_refresh",
                                        "final_response": False,
                                        "message": failure_msg,
                                        "refresh_status": "failed"
                                    }
                                ) + MESSAGE_DELIMITER

                        # Generate user-readable response using AI
                        step_num = "4" if is_schema_modifying else "3"
                        step = {"type": "reasoning_step",
                                "final_response": False,
                            "message": f"Step {step_num}: Generating user-friendly response"}
                        yield json.dumps(step) + MESSAGE_DELIMITER

                        response_agent = ResponseFormatterAgent()
                        user_readable_response = response_agent.format_response(
                            user_query=queries_history[-1],
                            mongodb_query=answer_an["mongodb_query"],
                            query_results=query_results,
                            db_description=db_description
                        )

                        yield json.dumps(
                            {
                                "type": "ai_response",
                                "final_response": True,
                                "message": user_readable_response,
                            }
                        ) + MESSAGE_DELIMITER

                        # Log overall completion time
                        overall_elapsed = time.perf_counter() - overall_start
                        logging.info(
                            "Query processing completed successfully - Total time: %.2f seconds",
                            overall_elapsed
                        )

                except Exception as e:  # pylint: disable=broad-exception-caught
                    execution_error = str(e)
                    overall_elapsed = time.perf_counter() - overall_start
                    logging.error("Database query execution error: %s", execution_error)
                    yield json.dumps({
                        "type": "error", 
                        "final_response": True, 
                        "message": f"Error executing MongoDB query: {execution_error}"
                    }) + MESSAGE_DELIMITER
            else:
                execution_error = "Missing information"
                # Query is not valid/translatable - generate follow-up questions
                follow_up_result = follow_up_agent.generate_follow_up_question(
                    user_question=queries_history[-1],
                    analysis_result=answer_an
                )

                # Send follow-up questions to help the user
                yield json.dumps({
                    "type": "followup_questions",
                    "final_response": True,
                    "message": follow_up_result,
                    "missing_information": answer_an.get("missing_information", ""),
                    "ambiguities": answer_an.get("ambiguities", "")
                }) + MESSAGE_DELIMITER

                overall_elapsed = time.perf_counter() - overall_start
                logging.info(
                    "Query processing completed (non-translatable query) - Total time: %.2f seconds",
                    overall_elapsed
                )

            # Save conversation to memory (only for on-topic queries)
            # Only save to memory if use_memory is enabled
            if memory_tool_task:
                # Determine the final answer based on which path was taken
                final_answer = user_readable_response if user_readable_response else follow_up_result

                # Build comprehensive response for memory
                full_response = {
                    "question": queries_history[-1],
                    "generated_query": answer_an.get('mongodb_query', ""),
                    "answer": final_answer
                }

                # Add error information if query execution failed
                if execution_error:
                    full_response["error"] = execution_error
                    full_response["success"] = False
                else:
                    full_response["success"] = True


                # Save query to memory
                save_query_task = asyncio.create_task(
                    memory_tool.save_query_memory(
                        query=queries_history[-1],
                        mongodb_query=answer_an.get("mongodb_query", ""),
                        success=full_response["success"],
                        error=execution_error
                    )
                )
                save_query_task.add_done_callback(
                    lambda t: logging.error("Query memory save failed: %s", t.exception())  # nosemgrep
                    if t.exception() else logging.info("Query memory saved successfully")
                )

                # Save conversation with memory tool (run in background)
                save_task = asyncio.create_task(
                    memory_tool.add_new_memory(full_response,
                                                [queries_history, result_history])
                )
                # Add error handling callback to prevent silent failures
                save_task.add_done_callback(
                    lambda t: logging.error("Memory save failed: %s", t.exception())  # nosemgrep
                    if t.exception() else logging.info("Conversation saved to memory tool")
                )
                logging.info("Conversation save task started in background")

                # Clean old memory in background (once per week cleanup)
                clean_memory_task = asyncio.create_task(memory_tool.clean_memory())
                clean_memory_task.add_done_callback(
                    lambda t: logging.error("Memory cleanup failed: %s", t.exception())  # nosemgrep
                    if t.exception() else logging.info("Memory cleanup completed successfully")
                )

        # Log timing summary at the end of processing
        overall_elapsed = time.perf_counter() - overall_start
        logging.info("Query processing pipeline completed - Total time: %.2f seconds",
                     overall_elapsed)

    return generate()


async def execute_destructive_operation(  # pylint: disable=too-many-statements
    user_id: str,
    graph_id: str,
    confirm_data: ConfirmRequest,
):
    """
    Handle user confirmation for destructive database operations
    """

    graph_id = _graph_name(user_id, graph_id)

    if hasattr(confirm_data, 'confirmation'):
        confirmation = confirm_data.confirmation.strip().upper()
    else:
        confirmation = ""

    query_to_execute = confirm_data.mongodb_query if hasattr(confirm_data, 'mongodb_query') else ""
    queries_history = confirm_data.chat if hasattr(confirm_data, 'chat') else []

    if not query_to_execute:
        raise InvalidArgumentError("No query provided")

    # Create a generator function for streaming the confirmation response
    async def generate_confirmation():  # pylint: disable=too-many-locals,too-many-statements
        # Create memory tool for saving query results
        memory_tool = await MemoryTool.create(user_id, graph_id)

        if confirmation == "CONFIRM":
            try:
                db_description, db_url = await get_db_description(graph_id)

                # Determine database type and get appropriate loader
                _, loader_class = get_database_type_and_loader(db_url)

                if not loader_class:
                    yield json.dumps({
                        "type": "error",
                        "message": "Unable to determine database type"
                    }) + MESSAGE_DELIMITER
                    return

                step = {"type": "reasoning_step",
                       "message": "Step 2: Executing confirmed query"}
                yield json.dumps(step) + MESSAGE_DELIMITER

                # Auto-quote table names for confirmed destructive operations
                if query_to_execute:
                    # Get schema to extract known tables
                    graph = db.select_graph(graph_id)
                    tables_query = "MATCH (t:Table) RETURN t.name"
                    try:
                        tables_res = (await graph.query(tables_query)).result_set
                        known_tables = (
                            {row[0] for row in tables_res}
                            if tables_res else set()
                        )
                    except Exception:  # pylint: disable=broad-exception-caught
                        known_tables = set()

                    # Determine database type and get appropriate quote character
                    db_type, _ = get_database_type_and_loader(db_url)

                    # MongoDB doesn't need specific sanitization like SQL
                    pass

                # Check if this query modifies the database schema using appropriate loader
                is_schema_modifying, operation_type = (
                    loader_class.is_schema_modifying_query(query_to_execute)
                )
                if db_type == 'mongodb':
                    query_results = loader_class.execute_query(query_to_execute, db_url)
                else:
                    raise InternalError(f"Unsupported database type: {db_type}")
                yield json.dumps(
                    {
                        "type": "query_result",
                        "data": query_results,
                    }
                ) + MESSAGE_DELIMITER

                # If schema was modified, refresh the graph
                if is_schema_modifying:
                    step = {"type": "reasoning_step",
                           "message": "Step 3: Schema change detected - refreshing graph..."}
                    yield json.dumps(step) + MESSAGE_DELIMITER

                    refresh_success, refresh_message = (
                        await loader_class.refresh_graph_schema(graph_id, db_url)
                    )

                    if refresh_success:
                        yield json.dumps(
                            {
                                "type": "schema_refresh",
                                "message": (f"✅ Schema change detected ({operation_type} "
                                          "operation)\n\n🔄 Graph schema has been automatically "
                                          "refreshed with the latest database structure."),
                                "refresh_status": "success"
                            }
                        ) + MESSAGE_DELIMITER
                    else:
                        yield json.dumps(
                            {
                                "type": "schema_refresh",
                                "message": (f"⚠️ Schema was modified but graph refresh failed: "
                                          f"{refresh_message}"),
                                "refresh_status": "failed"
                            }
                        ) + MESSAGE_DELIMITER

                # Generate user-readable response using AI
                step_num = "4" if is_schema_modifying else "3"
                step = {"type": "reasoning_step",
                       "message": f"Step {step_num}: Generating user-friendly response"}
                yield json.dumps(step) + MESSAGE_DELIMITER

                response_agent = ResponseFormatterAgent()
                user_readable_response = response_agent.format_response(
                    user_query=queries_history[-1] if queries_history else "Destructive operation",
                    mongodb_query=query_to_execute,
                    query_results=query_results,
                    db_description=db_description
                )

                yield json.dumps(
                    {
                        "type": "ai_response",
                        "message": user_readable_response,
                    }
                ) + MESSAGE_DELIMITER

                # Save successful confirmed query to memory
                save_query_task = asyncio.create_task(
                    memory_tool.save_query_memory(
                        query=(queries_history[-1] if queries_history
                               else "Destructive operation confirmation"),
                        mongodb_query=query_to_execute,
                        success=True,
                        error=""
                    )
                )
                save_query_task.add_done_callback(
                    lambda t: logging.error("Confirmed query memory save failed: %s",
                                            t.exception())  # nosemgrep
                    if t.exception() else logging.info("Confirmed query memory saved successfully")
                )

            except Exception as e:  # pylint: disable=broad-exception-caught
                logging.error("Error executing confirmed query: %s", str(e))  # nosemgrep
                error_message = str(e) if str(e) else "Error executing query"

                # Save failed confirmed query to memory
                save_query_task = asyncio.create_task(
                    memory_tool.save_query_memory(
                        query=(queries_history[-1] if queries_history
                               else "Destructive operation confirmation"),
                        mongodb_query=query_to_execute,
                        success=False,
                        error=str(e)
                    )
                )
                save_query_task.add_done_callback(
                    lambda t: logging.error(  # nosemgrep
                        "Failed confirmed query memory save failed: %s", t.exception()
                    ) if t.exception() else logging.info(
                        "Failed confirmed query memory saved successfully"
                    )
                )

                yield json.dumps(
                    {"type": "error", "message": error_message}
                ) + MESSAGE_DELIMITER
        else:
            # User cancelled or provided invalid confirmation
            yield json.dumps(
                {
                    "type": "operation_cancelled",
                    "message": "Operation cancelled. The destructive query was not executed."
                }
            ) + MESSAGE_DELIMITER

    return generate_confirmation()

async def refresh_database_schema(user_id: str, graph_id: str):
    """
    Manually refresh the graph schema from the database.
    This endpoint allows users to manually trigger a schema refresh
    if they suspect the graph is out of sync with the database.
    """
    graph_id = _graph_name(user_id, graph_id)

    # Prevent refresh of demo databases
    if GENERAL_PREFIX and graph_id.startswith(GENERAL_PREFIX):
        raise InvalidArgumentError("Demo graphs cannot be refreshed")

    try:
        # Get database description and URL
        _, db_url = await get_db_description(graph_id)

        if not db_url or db_url == "No URL available for this database.":
            raise InternalError("No database URL found for this graph")

        # Call load_database to refresh the schema by reconnecting
        return await load_database(db_url, user_id)
    except InternalError:
        raise
    except Exception as e:
        logging.error("Error in refresh_graph_schema: %s", str(e))
        raise InternalError("Internal server error while refreshing schema") from e

async def delete_database(user_id: str, graph_id: str):
    """Delete the specified graph (namespaced to the user).

    This will attempt to delete the FalkorDB graph belonging to the
    authenticated user. The graph id used by the client is stripped of
    namespace and will be namespaced using the user's id from the request
    state.
    """
    namespaced = _graph_name(user_id, graph_id)
    if GENERAL_PREFIX and graph_id.startswith(GENERAL_PREFIX):
        raise InvalidArgumentError("Demo graphs cannot be deleted")

    try:
        # Select and delete the graph using the FalkorDB client API
        graph = db.select_graph(namespaced)
        await graph.delete()
        return {"success": True, "graph": graph_id}
    except ResponseError as re:
        raise GraphNotFoundError("Failed to delete graph, Graph not found") from re
    except Exception as e:  # pylint: disable=broad-exception-caught
        logging.exception("Failed to delete graph %s: %s", sanitize_log_input(namespaced), e)
        raise InternalError("Failed to delete graph") from e
