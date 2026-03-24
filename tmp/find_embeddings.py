import os
import requests
from dotenv import load_dotenv

load_dotenv()

def list_models():
    api_key = os.getenv("GOOGLE_API_KEY")
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    
    response = requests.get(url)
    if response.status_code == 200:
        models = response.json().get('models', [])
        print("Available Embedding Models:")
        found = False
        for m in models:
            name = m.get('name')
            methods = m.get('supportedGenerationMethods', [])
            if 'embedContent' in methods:
                print(f"- {name}")
                found = True
        if not found:
            print("No embedding models found!")
    else:
        print(f"Error: {response.status_code}")

if __name__ == "__main__":
    list_models()
