"""
HealerAgent - Specialized agent for fixing MongoDB query errors.

This agent focuses solely on correcting queries that failed execution,
without requiring full graph context. It uses the error message and the
failed query to generate a corrected version.
"""
# pylint: disable=trailing-whitespace,line-too-long,too-many-arguments
# pylint: disable=too-many-positional-arguments,broad-exception-caught

import re
import json
from typing import Dict, Callable, Any
from litellm import completion
from api.config import Config
from .utils import parse_response


class HealerAgent:
    """Agent specialized in fixing MongoDB query errors."""
    
    def __init__(self, max_healing_attempts: int = 3):
        """Initialize the healer agent.
        
        Args:
            max_healing_attempts: Maximum number of healing attempts before giving up
        """
        self.max_healing_attempts = max_healing_attempts
        self.messages = []
    
    @staticmethod
    def validate_query_syntax(query_str: str) -> dict:
        """
        Validate MongoDB query for basic syntax errors.
        
        Args:
            query_str: The MongoDB query string (JSON) to validate
            
        Returns:
            dict with 'is_valid', 'errors', and 'warnings' keys
        """
        errors = []
        warnings = []
        
        if not query_str:
            errors.append("Query is empty")
            return {"is_valid": False, "errors": errors, "warnings": warnings}
        
        # Check if it's valid JSON (if string) or already a dict
        if isinstance(query_str, str):
            query = query_str.strip()
            if not query:
                errors.append("Query is empty")
                return {"is_valid": False, "errors": errors, "warnings": warnings}
            try:
                parsed = json.loads(query)
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON: {str(e)}")
                return {"is_valid": False, "errors": errors, "warnings": warnings}
        else:
            parsed = query_str
        
        # Check for required fields
        if not isinstance(parsed, dict):
            errors.append("Query must be a JSON object")
            return {"is_valid": False, "errors": errors, "warnings": warnings}
        
        if "collection" not in parsed:
            errors.append("Missing required field: 'collection'")
        
        if "operation" not in parsed:
            errors.append("Missing required field: 'operation'")
        else:
            valid_ops = {'find', 'aggregate', 'count', 'distinct', 
                        'insert', 'insert_one', 'insert_many',
                        'update', 'update_one', 'update_many',
                        'delete', 'delete_one', 'delete_many'}
            if parsed.get('operation', '').lower() not in valid_ops:
                errors.append(f"Invalid operation: '{parsed.get('operation')}'. Must be one of: {', '.join(sorted(valid_ops))}")
        
        # Check for dangerous operations
        op = parsed.get('operation', '').lower()
        dangerous_ops = {'delete', 'delete_one', 'delete_many', 'drop'}
        if op in dangerous_ops:
            warnings.append(f"Query contains potentially dangerous operation: {op}")
        
        return {
            "is_valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings
        }
    
    def _build_healing_prompt(
        self,
        failed_query: str,
        error_message: str,
        db_description: str,
        question: str,
        database_type: str
    ) -> str:
        """Build a focused prompt for MongoDB query healing."""
        
        # Analyze error to provide targeted hints
        error_hints = self._analyze_error(error_message, database_type)
        
        prompt = f"""You are a MongoDB query debugging expert. Your task is to fix a MongoDB query that failed execution.

DATABASE TYPE: MONGODB

FAILED QUERY:
```json
{failed_query}
```

EXECUTION ERROR:
{error_message}

{f"ORIGINAL QUESTION: {question}" if question else ""}

{f"DATABASE INFO: {db_description}"}

COMMON ERROR PATTERNS:
{error_hints}

YOUR TASK:
1. Identify the exact cause of the error
2. Fix ONLY what's broken - don't rewrite the entire query
3. Ensure the fix produces valid MongoDB query JSON
4. Maintain the original query logic and intent

CRITICAL RULES FOR MONGODB:
- MongoDB queries are JSON objects, ensure valid JSON structure
- Check aggregation pipeline stages are in correct order
- Ensure field names match the schema exactly (case-sensitive)
- $match, $group, $sort, $project, $lookup must be in correct format
- Use $lookup for joining collections
- The "operation" field must be one of: find, aggregate, count, distinct, insert, update, delete
- Filter objects must use MongoDB operators ($eq, $gt, $gte, $lt, $lte, $in, $ne, $and, $or, etc.)
- Aggregation expressions use $ prefix for field references (e.g., "$fieldname")
- The query must be a valid JSON object with "collection" and "operation" fields

RESPONSE FORMAT (valid JSON only):
{{
  "mongodb_query": "-- your fixed MongoDB query as JSON string here",
  "confidence": 85,
  "explanation": "Brief explanation of what was fixed",
  "changes_made": ["Fixed operator syntax", "Corrected field name"]
}}

IMPORTANT:
- Return ONLY the JSON object, no other text
- Fix ONLY the specific error, preserve the rest
- The mongodb_query field must contain a valid MongoDB query as a JSON string
- Test your fix mentally before responding
"""
        
        return prompt
    
    def heal_and_execute(  # pylint: disable=too-many-locals
        self,
        initial_mongodb: str,
        initial_error: str,
        execute_query_func: Callable[[str], Any],
        db_description: str = "",
        question: str = "",
        database_type: str = "mongodb"
    ) -> Dict[str, Any]:
        """Iteratively heal and execute MongoDB query until success or max attempts.
        
        This method creates a conversation loop between the healer and the database:
        1. Build initial prompt once with the failed query and error (including syntax validation)
        2. Loop: Call LLM → Parse healed query → Execute → Check if successful
        3. If successful, return results
        4. If failed and not last attempt, add error feedback and repeat
        5. If failed on last attempt, return failure
        
        Args:
            initial_mongodb: The initial query that failed
            initial_error: The error message from the initial execution failure
            execute_query_func: Function that executes the query and returns results or raises exception
            db_description: Optional database description
            question: Optional original question
            database_type: Type of database (mongodb)
            
        Returns:
            Dict containing:
                - success: Whether healing succeeded
                - mongodb_query: Final query (healed or original)
                - query_results: Results from successful execution (if success=True)
                - attempts: Number of healing attempts made
                - final_error: Final error message (if success=False)
        """
        self.messages = []
        
        # Ensure query is a string for prompt formatting
        if isinstance(initial_mongodb, dict):
            try:
                # Try to use json_util if available, otherwise fallback to json
                try:
                    from bson import json_util
                    initial_mongodb_str = json_util.dumps(initial_mongodb, indent=2)
                except ImportError:
                    initial_mongodb_str = json.dumps(initial_mongodb, indent=2)
            except Exception:
                initial_mongodb_str = str(initial_mongodb)
        else:
            initial_mongodb_str = initial_mongodb
        
        # Validate query for additional error context
        additional_context = ""
        validation_result = self.validate_query_syntax(initial_mongodb_str)
        if validation_result["errors"]:
            additional_context += f"\nSyntax errors: {', '.join(validation_result['errors'])}"
        if validation_result["warnings"]:
            additional_context += f"\nWarnings: {', '.join(validation_result['warnings'])}"
        
        # Enhance error message with validation context
        enhanced_error = initial_error + additional_context
        
        # Build initial prompt once before the loop
        prompt = self._build_healing_prompt(
            failed_query=initial_mongodb_str,
            error_message=enhanced_error,
            db_description=db_description,
            question=question,
            database_type=database_type
        )
        self.messages.append({"role": "user", "content": prompt})
        
        for attempt in range(self.max_healing_attempts):
            # Call LLM
            response = completion(
                model=Config.COMPLETION_MODEL,
                messages=self.messages,
                temperature=0.1,
                max_tokens=2000
            )
            
            content = response.choices[0].message.content
            self.messages.append({"role": "assistant", "content": content})
            
            # Parse response
            result = parse_response(content)
            healed_mongodb = result.get("mongodb_query", "")
            
            # Execute against database
            error = None
            try:
                query_results = execute_query_func(healed_mongodb)
            except Exception as e:
                error = str(e)
            
            # Check if it worked
            if error is None:
                # Success!
                return {
                    "success": True,
                    "mongodb_query": healed_mongodb,
                    "query_results": query_results,
                    "attempts": attempt + 1,
                    "final_error": None
                }
            
            # Failed - check if last attempt
            if attempt >= self.max_healing_attempts - 1:
                return {
                    "success": False,
                    "mongodb_query": healed_mongodb,
                    "query_results": None,
                    "attempts": attempt + 1,
                    "final_error": error
                }
            
            # Not last attempt - add feedback and continue
            feedback = f"""The healed query failed with error:

```json
{healed_mongodb}
```

ERROR:
{error}

Please fix this error."""
            self.messages.append({"role": "user", "content": feedback})
        
        # Fallback return
        return {
            "success": False,
            "mongodb_query": initial_mongodb,
            "query_results": None,
            "attempts": self.max_healing_attempts,
            "final_error": initial_error
        }
        
    
    def _analyze_error(self, error_message: str, database_type: str) -> str:
        """Analyze error message and provide targeted hints."""
        
        error_lower = error_message.lower()
        hints = []
        
        # MongoDB-specific errors
        if "json" in error_lower or "parse" in error_lower:
            hints.append("⚠️  Invalid JSON structure - check brackets, quotes, and commas")
            hints.append("   Ensure the query is a valid JSON object")
        
        if "collection" in error_lower and "not found" in error_lower:
            hints.append("⚠️  Collection name doesn't exist - check spelling (case-sensitive)")
        
        if "field" in error_lower or "key" in error_lower:
            hints.append("⚠️  Field name issue - MongoDB field names are case-sensitive")
            hints.append("   Check for typos and exact field names from the schema")
        
        if "$" in error_lower and ("operator" in error_lower or "unknown" in error_lower):
            hints.append("⚠️  Invalid MongoDB operator - check operator spelling")
            hints.append("   Common: $match, $group, $sort, $project, $lookup, $unwind")
        
        if "pipeline" in error_lower:
            hints.append("⚠️  Aggregation pipeline error - check stage ordering and syntax")
            hints.append("   Each stage must be a JSON object with exactly one operator key")
        
        if "type" in error_lower or "bson" in error_lower:
            hints.append("⚠️  Data type mismatch - check that filter values match the field's data type")
        
        # Generic hints if no specific patterns matched
        if not hints:
            hints.append("⚠️  Check MongoDB query JSON structure and field names")
            hints.append("⚠️  Verify collection names and operators are correct")
        
        return "\n".join(hints)

