from flask import Flask, request, jsonify
from query_manager import QueryManager
from storage_manager import StorageManager
from ddl_manager import DDLManager
from utils import track_time

app = Flask(__name__)
storage_manager = StorageManager()
query_manager = QueryManager()
ddl_manager = DDLManager(storage_manager)


@app.route("/query", methods=["POST"])
@track_time
def execute_query():
    """API endpoint for the interface"""
    data = request.get_json()
    query = data.get("query")

    if not query:
        return jsonify({"error": "No query provided"}), 400

    try:
        parsed_query = query_manager.parse_query(query)
        # TODO: Implement the logic to execute the parsed query (or somewhere else)

        result = ""
        return jsonify({"success": True, "result": result})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400


if __name__ == "__main__":
    app.run(debug=True)
