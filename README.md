<div align="center">
  <img src="https://github.com/user-attachments/assets/34663279-0273-4c21-88a8-d20700020a07" alt="QueryWeaver Logo" width="600px" />
  <h1>✨ QueryWeaver: Text-to-MongoDB ✨</h1>
  <p><b>Transform natural language into powerful MongoDB queries with graph-powered schema intelligence.</b></p>

  <div>
    <a href="https://discord.gg/b32KEzMzce">
      <img src="https://img.shields.io/badge/Discord-%235865F2.svg?&logo=discord&logoColor=white" alt="Discord" />
    </a>
    <a href="https://app.falkordb.cloud">
      <img src="https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900" alt="Try Free" />
    </a>
    <a href="https://hub.docker.com/r/falkordb/queryweaver/">
      <img src="https://img.shields.io/docker/pulls/falkordb/queryweaver?label=Docker" alt="Dockerhub" />
    </a>
    <a href="https://app.queryweaver.ai/docs">
      <img src="https://img.shields.io/badge/API-Swagger-11B48A?logo=swagger&logoColor=white" alt="Swagger UI" />
    </a>
  </div>
</div>

---

## 🚀 Overview

QueryWeaver is an **open-source Text-to-MongoDB** tool that bridges the gap between natural language and database queries. By leveraging **graph-powered schema understanding**, it converts plain-English questions into valid MongoDB aggregation pipelines and find queries.

### Key Features
- 🧠 **Graph-Powered AI**: Uses FalkorDB to maintain high-fidelity schema graphs for better query accuracy.
- 🔌 **REST API & MCP**: Exposes clean REST endpoints and Model Context Protocol (MCP) for seamless integration.
- 💬 **Interactive Chat**: Streaming responses with reasoning steps and confirmation for destructive operations.
- 🔐 **Secure & Scalable**: Support for OAuth (Google/GitHub), API tokens, and production-ready deployments.

---

## 🏗️ How It Works

QueryWeaver uses a sophisticated multi-agent system to process your requests:

1. **Schema Extraction**: Connects to your MongoDB/PostgreSQL/MySQL database and extracts metadata (collections, fields, types, and references).
2. **Graph Modeling**: Stores this metadata in **FalkorDB** as a graph, mapping relationships and semantic descriptions.
3. **Natural Language Processing**: When you ask a question, the `AnalysisAgent` retrieves relevant schema context from the graph.
4. **Query Generation**: The LLM (OpenAI, Azure, or Gemini) generates a structured JSON containing the MongoDB query.
5. **Execution & Feedback**: The query is executed, and the results are returned alongside a natural language explanation.

```mermaid
graph LR
    User([User Query]) --> API[FastAPI Backend]
    API --> Agent[Analysis Agent]
    Agent --> Graph[(FalkorDB Graph)]
    Agent --> LLM{AI Model}
    LLM --> Query[MongoDB Query]
    Query --> DB[(MongoDB Database)]
    DB --> Results[Query Results]
    Results --> UI[Visual Dashboard]
```

---

## 📥 Getting Started

### 🐳 Run with Docker (Recommended)
The fastest way to evaluate QueryWeaver is via Docker:

```bash
docker run -p 5000:5000 -it falkordb/queryweaver
```

Access the dashboard at: [http://localhost:5000](http://localhost:5000)

### 🛠️ Local Installation (Development)

#### 1. Prerequisites
- **Python 3.12+** and `pipenv`
- **Node.js 18+** and `npm`
- **FalkorDB** instance (local or via [FalkorDB Cloud](https://app.falkordb.cloud))

#### 2. Backend Setup
```bash
# Clone the repository
git clone https://github.com/akash247777/QueryWeaver-Text-to-MongoDB-.git
cd QueryWeaver-main

# Install dependencies
pipenv sync --dev

# Set up environment variables
cp .env.example .env
# Edit .env and add your AI keys (OPENAI_API_KEY, etc.)
```

#### 3. Frontend Setup
```bash
cd app
npm install
```

#### 4. Run the Application

Start the backend (from the root):
```powershell
python -m api.index
```

Start the frontend (from `/app`):
```powershell
npm run dev
```

---

## ⚙️ Configuration (.env)

| Variable | Description | Required |
|----------|-------------|----------|
| `FASTAPI_SECRET_KEY` | Secret for session management | Yes |
| `FALKORDB_URL` | Connection URL (e.g., `redis://localhost:6379/0`) | Yes |
| `OPENAI_API_KEY` | API Key for OpenAI models | Optional |
| `GOOGLE_API_KEY` | API Key for Gemini models | Optional |
| `AZURE_API_KEY` | API Key for Azure OpenAI | Optional |
| `MONGODB_URL` | Your target MongoDB database connection | Yes (for queries) |

> [!TIP]
> Use `.env.example` as a template for all available configuration options.

---

## 🧪 Testing

We use **Pytest** for backend tests and **Playwright** for End-to-End browser testing.

```bash
# Setup test environment
make setup-dev

# Run all tests
make test
```

---

## 📜 License

This project is licensed under the **GNU Affero General Public License (AGPL)**. See the [LICENSE](LICENSE) file for details.

© 2025 FalkorDB Ltd.
