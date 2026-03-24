"""Response formatter agent for generating user-readable responses from MongoDB query results."""

from typing import List, Dict
from litellm import completion
from api.config import Config


RESPONSE_FORMATTER_PROMPT = """
You are an AI assistant that helps users understand database query results. Your task is to analyze the MongoDB query results and provide a clear, concise, and user-friendly explanation in natural language.

**Context:**
Database Description: {DB_DESCRIPTION}

**User's Original Question:**
{USER_QUERY}

**MongoDB Query Executed:**
{MONGODB_QUERY}

**Operation Type:** {OPERATION_TYPE}

**Query Results:**
{FORMATTED_RESULTS}

**Instructions:**
1. Provide a direct, natural language answer to the user's question based ONLY on the query results.
2. For retrieval queries (find/aggregate): State the answer clearly (e.g., "The driver name is Sameer Ansari" instead of just listing fields).
3. If multiple items are found, summarize them clearly or use a list.
4. For data modifications (insert/update/delete): Confirm the action (e.g., "Successfully updated the driver record").
5. Do NOT mention technical details like MongoDB syntax, ObjectIds, or collection names unless the user asked for them.
6. Be conversational, professional, and concise.
7. If the results are empty, politely state that no matching record was found.

**Response Format:**
A direct natural language answer to the user's question.
"""


class ResponseFormatterAgent:
    # pylint: disable=too-few-public-methods
    """Agent for generating user-readable responses from MongoDB query results."""

    def __init__(self):
        """Initialize the response formatter agent."""

    def format_response(self, user_query: str, mongodb_query: str,
                       query_results: List[Dict], db_description: str = "") -> str:
        """
        Generate a user-readable response based on the MongoDB query results.

        Args:
            user_query: The original user question
            mongodb_query: The MongoDB query that was executed
            query_results: The results from the MongoDB query execution
            db_description: Description of the database context

        Returns:
            A formatted, user-readable response string
        """
        prompt = self._build_response_prompt(user_query, mongodb_query, query_results, db_description)

        messages = [{"role": "user", "content": prompt}]

        completion_result = completion(
            model=Config.COMPLETION_MODEL,
            messages=messages,
            temperature=0.3,  # Slightly higher temperature for more natural responses
            top_p=1,
        )

        response = completion_result.choices[0].message.content
        return response.strip()

    def _build_response_prompt(self, user_query: str, mongodb_query: str,
                              query_results: List[Dict], db_description: str) -> str:
        """Build the prompt for generating user-readable responses."""

        # Format the query results for better readability
        formatted_results = self._format_query_results(query_results)

        # Determine the type of query operation
        query_str = mongodb_query if isinstance(mongodb_query, str) else ""
        if not query_str and mongodb_query:
            try:
                import json as _json
                if isinstance(mongodb_query, dict):
                    query_str = _json.dumps(mongodb_query)
                else:
                    query_str = str(mongodb_query)
            except Exception:
                query_str = ""

        if query_str and query_str.strip().startswith('{'):
            # MongoDB query - try to extract operation type
            try:
                import json as _json
                mongo_query = _json.loads(query_str)
                operation_type = f"MongoDB {mongo_query.get('operation', 'find').upper()}"
            except (Exception):
                operation_type = "MongoDB QUERY"
        else:
            operation_type = query_str.strip().split()[0].upper() if query_str else "UNKNOWN"

        prompt = RESPONSE_FORMATTER_PROMPT.format(
            DB_DESCRIPTION=db_description if db_description else "Not provided",
            USER_QUERY=user_query,
            MONGODB_QUERY=mongodb_query,
            OPERATION_TYPE=operation_type,
            FORMATTED_RESULTS=formatted_results
        )

        return prompt

    def _format_query_results(self, query_results: List[Dict]) -> str:
        """Format query results for inclusion in the prompt."""
        if not query_results:
            return "No results found."

        if len(query_results) == 0:
            return "No results found."

        # Check if this is an operation result (INSERT/UPDATE/DELETE)
        if len(query_results) == 1 and "operation" in query_results[0]:
            result = query_results[0]
            operation = result.get("operation", "UNKNOWN")
            modified_count = result.get("modified_count")
            status = result.get("status", "unknown")

            parts = [f"Operation: {operation}", f"Status: {status}"]

            if "inserted_count" in result:
                parts.append(f"Inserted: {result['inserted_count']}")
            if "inserted_id" in result:
                parts.append(f"Inserted ID: {result['inserted_id']}")

            return ", ".join(parts)

        # Handle regular query results
        # Limit the number of results shown in the prompt to avoid token limits
        max_results_to_show = 50
        results_to_show = query_results[:max_results_to_show]

        formatted = []
        for i, result in enumerate(results_to_show, 1):
            if isinstance(result, dict):
                result_str = ", ".join([f"{k}: {v}" for k, v in result.items()])
                formatted.append(f"{i}. {result_str}")
            else:
                formatted.append(f"{i}. {result}")

        result_text = "\n".join(formatted)

        if len(query_results) > max_results_to_show:
            result_text += f"\n... and {len(query_results) - max_results_to_show} more results"

        return result_text
