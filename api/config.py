
"""
This module contains the configuration for the text-to-MongoDB query module.
"""

import os
import logging
import dataclasses
from typing import Union

from dotenv import load_dotenv

# Load .env FIRST — before any os.getenv() calls.
# config.py is imported early via the module chain (index → app_factory → routes → config)
# so load_dotenv() in app_factory.py runs too late.
load_dotenv()

import litellm
from litellm import embedding

# Ensure GEMINI_API_KEY is set if GOOGLE_API_KEY is available
# LiteLLM routes gemini/ models through Google AI Studio when GEMINI_API_KEY is set
_google_api_key = os.getenv("GOOGLE_API_KEY", "")
if _google_api_key and not os.getenv("GEMINI_API_KEY"):
    os.environ["GEMINI_API_KEY"] = _google_api_key

# Configure litellm logging to prevent sensitive data leakage
def configure_litellm_logging():
    """Configure litellm to suppress completion logs."""

    # Disable LiteLLM logger that outputs
    litellm_logger = logging.getLogger("LiteLLM")
    litellm_logger.setLevel(logging.ERROR)
    litellm_logger.disabled = True


# Initialize litellm configuration
configure_litellm_logging()


class EmbeddingsModel:
    """Embeddings model wrapper for text embedding operations."""

    def __init__(self, model_name: str, api_key: str = None, config: dict = None):
        self.model_name = model_name
        self.api_key = api_key
        self.config = config

    def embed(self, text: Union[str, list]) -> list:
        """
        Get the embeddings of the text

        Args:
            text (str|list): The text(s) to embed

        Returns:
            list: The embeddings of the text

        """
        kwargs = {"model": self.model_name, "input": text, "timeout": 120}
        if self.api_key:
            kwargs["api_key"] = self.api_key
        embeddings = embedding(**kwargs)
        embeddings = [embedding["embedding"] for embedding in embeddings.data]
        return embeddings

    def get_vector_size(self) -> int:
        """
        Get the size of the vector

        Returns:
            int: The size of the vector

        """
        kwargs = {"input": ["Hello World"], "model": self.model_name}
        if self.api_key:
            kwargs["api_key"] = self.api_key
        response = embedding(**kwargs)
        size = len(response.data[0]["embedding"])
        return size


@dataclasses.dataclass
class Config:
    """
    Configuration class for the text-to-MongoDB query module.
    """
    AZURE_FLAG = True
    GOOGLE_FLAG = False

    # Priority: GOOGLE_API_KEY > OPENAI_API_KEY > Azure (default)
    if os.getenv("GOOGLE_API_KEY"):
        AZURE_FLAG = False
        GOOGLE_FLAG = True
        _API_KEY = os.getenv("GOOGLE_API_KEY", "")
        EMBEDDING_MODEL_NAME = "gemini/gemini-embedding-001"
        COMPLETION_MODEL = os.getenv("COMPLETION_MODEL", "gemini/gemini-3.1-flash-lite-preview")
    elif os.getenv("OPENAI_API_KEY"):
        AZURE_FLAG = False
        _API_KEY = os.getenv("OPENAI_API_KEY", "")
        EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "openai/text-embedding-ada-002")
        COMPLETION_MODEL = os.getenv("COMPLETION_MODEL", "openai/gpt-4.1")
    else:
        _API_KEY = None
        EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "azure/text-embedding-ada-002")
        COMPLETION_MODEL = os.getenv("COMPLETION_MODEL", "azure/gpt-4.1")

    DB_MAX_DISTINCT: int = 100  # pylint: disable=invalid-name
    DB_UNIQUENESS_THRESHOLD: float = 0.5  # pylint: disable=invalid-name
    SHORT_MEMORY_LENGTH = 1  # Maximum number of questions to keep in short-term memory

    EMBEDDING_MODEL = EmbeddingsModel(model_name=EMBEDDING_MODEL_NAME, api_key=_API_KEY)

    FIND_SYSTEM_PROMPT = """
    You are an expert in analyzing natural language queries against MongoDB databases.
    Please analyze the user's query and generate a set of collection and field descriptions that might be relevant to the user's query.
    These descriptions should describe the collections and fields that are relevant to the user's query.
    If the user's query is more relevant to specific fields, please provide a description of those fields.
    - Try to generate description for any part of the user query.
    - Create generic collection or field descriptions, do not use specific codes, values or any specific condition.
    - Try to be accurate and precise in your descriptions.
    - In any case do not generate more than five descriptions (each).
    - List the collections and fields in the order of their relevance to the user's query.

    Keep in mind that the database that you work with has the following DB description: {db_description}.

    **Input:**
    * **MongoDB Database:**
    You will be provided with the database name and the description of the database domain.

    * **Previous User Queries:**
    You will be provided with a list of previous queries the user has asked in this session. Each query will be prefixed with "Query N:" where N is the query number. Use this context to better understand the user's intent and provide more relevant collection and field suggestions.

    * **User Query (Natural Language):**
    You will be given a user's current question or request in natural language.

    **Output:**
    * **Collection Descriptions:**
    You should provide a set of collection descriptions that are relevant to the user's query.

    * **Field Descriptions:**
    If the user's query is more relevant to specific fields, you should provide a set of field descriptions that are relevant to the user's query.
    """

    Text_To_MongoDB_PROMPT = """
    You are a Text-to-MongoDB model. Your task is to generate MongoDB queries based on natural language questions and a provided database schema.

    **Instructions:**
    1. **Understand the Database Schema:** Carefully analyze the provided database schema to understand the collections, fields, data types, and relationships.
    2. **Consider Previous Queries:** Review the user's previous queries to understand the context of their current question and maintain consistency in your approach.
    3. **Interpret the User's Question:** Understand the user's question and identify the relevant entities, attributes, and relationships.
    4. **Generate the MongoDB Query:** Construct a valid MongoDB query (using pymongo syntax) that accurately reflects the user's question and uses the provided database schema.
    5. **Use MongoDB Query Syntax:** Use proper MongoDB query operators ($match, $group, $sort, $project, $lookup, $unwind, $limit, $skip, $count, etc.).
    6. **Return Only the Query:** Do not include any explanations, justifications, or additional text. Only return the generated MongoDB query as a JSON object.
    7. **Handle Ambiguity:** If the user's question is ambiguous, make reasonable assumptions based on the schema and previous queries to generate the most likely MongoDB query.
    8. **Handle Unknown Information:** If the user's question refers to information not present in the schema, return an appropriate error message or a query that retrieves as much relevant information as possible.
    9. **Prioritize Accuracy:** Accuracy is paramount. Ensure the generated MongoDB query returns the correct results.
    10. **Use MongoDB aggregation pipelines for complex queries** that involve grouping, sorting, joining (via $lookup), or computed fields.
    11. **Do not add any comments to the generated query.**
    12. **When you use $match, please use the exact value as the user provided, and do not make up values.**
    13. **If you dont have the value for a $match filter, use "TBD" for string and 1111 for number.**
    14. **Only use $lookup between collections based on the foreign key references described in the schema.**
    15. **Do not create $lookup between collections that are not explicitly connected by references in the input schema.**
    16. **Use $project to limit returned fields when appropriate.**

    The query format should be a JSON object with:
    - "collection": the collection name to query
    - "operation": one of "find", "aggregate", "count", "distinct"
    - "query": the filter/match document (for find operations)
    - "pipeline": the aggregation pipeline array (for aggregate operations)
    - "projection": fields to include/exclude (for find operations)
    - "sort": sort specification (for find operations)
    - "limit": maximum number of documents to return

    Keep in mind that the database that you work with has the following description: {db_description}.

    Before you start to answer, analyze the user_query step by step and try to understand the user's intent and the relevant collections and fields.

    **Input:**
    * **Database Schema:**
    You will be provided with part of the database schema that might be relevant to the user's question.
    With the following structure:
    {{"schema": [["collection_name", description, references[list], [{{"field_name": "field_description", "data_type": "data_type",...}},...]],...]}}

    * **Previous Queries:**
    You will be provided with a list of the user's previous queries in this session. Each query will be prefixed with "Query N:" where N is the query number, followed by both the natural language question and the MongoDB query that was generated. Use these to maintain consistency and understand the user's evolving information needs.

    * **User Query (Natural Language):**
    You will be given a user's current question or request in natural language.
    """
