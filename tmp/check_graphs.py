import asyncio
import os
from api.extensions import db

async def check_graphs():
    try:
        graphs = await db.get_graphs()
        print(f"Available graphs: {graphs}")
        for graph_name in graphs:
            graph = db.select_graph(graph_name)
            result = await graph.query("MATCH (n:Table) RETURN count(n)")
            print(f"Graph '{graph_name}' has {result.result_set[0][0]} table nodes.")
    except Exception as e:
        print(f"Error checking graphs: {e}")

if __name__ == "__main__":
    asyncio.run(check_graphs())
