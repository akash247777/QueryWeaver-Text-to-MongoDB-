import asyncio
import logging
import sys
import os

# Set PYTHONPATH to include the current directory
sys.path.append(os.getcwd())

from api.core.schema_loader import load_database

logging.basicConfig(level=logging.INFO)

async def test_load_db():
    uri = "mongodb://admin:yourStrongPassword@34.16.116.26:27017/test?authSource=admin"
    user_id = "guest_local"
    
    print(f"Testing load_database with URI: {uri} for user: {user_id}")
    
    try:
        # load_database returns a generator (or an awaitable that returns one)
        generator = await load_database(uri, user_id)
        
        async for chunk in generator:
            print(f"Progress: {chunk}")
            
    except Exception as e:
        print(f"Error during load_database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_load_db())
