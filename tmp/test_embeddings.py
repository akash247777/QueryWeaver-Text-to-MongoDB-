import os
from litellm import embedding
from dotenv import load_dotenv

load_dotenv()

def test_embedding():
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("GOOGLE_API_KEY not found")
        return

    models = ["gemini/embedding-001", "gemini/text-embedding-004"]
    
    for model in models:
        print(f"Testing model: {model}")
        try:
            response = embedding(
                model=model,
                input=["Hello world"],
                api_key=api_key
            )
            print(f"Success for {model}!")
        except Exception as e:
            print(f"Failed for {model}: {e}")

if __name__ == "__main__":
    test_embedding()
