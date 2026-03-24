import pymongo
import sys
import logging

logging.basicConfig(level=logging.INFO)
uri = "mongodb://admin:yourStrongPassword@34.16.116.26:27017/test?authSource=admin"

def test_connection():
    try:
        print(f"Connecting to {uri}...")
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        # The ismaster command is cheap and does not require special privileges.
        client.admin.command('ismaster')
        print("Successfully connected to MongoDB!")
        
        db = client.test
        collections = db.list_collection_names()
        print(f"Collections in 'test' database: {collections}")
        
        # Try to count docs in the first collection if any
        if collections:
            count = db[collections[0]].count_documents({})
            print(f"Document count in '{collections[0]}': {count}")
        
        client.close()
        return True
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
