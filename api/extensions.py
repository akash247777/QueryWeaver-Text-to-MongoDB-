"""Extensions for the text-to-MongoDB query library"""

import os

from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

# Connect to FalkorDB
url = os.getenv("FALKORDB_URL", None)
if url is None:
    try:
        db = FalkorDB(host="localhost", port=6379)
    except Exception as e:
        raise ConnectionError(f"Failed to connect to FalkorDB: {e}") from e
else:
    # Ensure the URL is properly encoded as string and handle potential encoding issues
    try:
        # The redis driver expects redis://, rediss:// or unix://
        # Replace falkordb:// with redis:// for compatibility
        if url.startswith("falkordb://"):
            url = url.replace("falkordb://", "redis://", 1)
            
        # Create connection pool with explicit encoding settings
        pool = BlockingConnectionPool.from_url(
            url,
            decode_responses=True
        )
        db = FalkorDB(connection_pool=pool)
    except Exception as e:
        raise ConnectionError(f"Failed to connect to FalkorDB with URL: {e}") from e
