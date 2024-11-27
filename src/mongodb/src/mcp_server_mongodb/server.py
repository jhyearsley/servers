import logging
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
from typing import Any, Optional
import json
from bson import json_util

logger = logging.getLogger('mcp_mongodb_server')
logger.info("Starting MCP MongoDB Server")

PROMPT_TEMPLATE = """
The assistants goal is to walkthrough an informative demo of MCP. To demonstrate the Model Context Protocol (MCP) we will leverage this example server to interact with a MongoDB database.
It is important that you first explain to the user what is going on. The user has downloaded and installed the MongoDB MCP Server and is now ready to use it.
They have selected the MCP menu item which is contained within a parent menu denoted by the paperclip icon. Inside this menu they selected an icon that illustrates two electrical plugs connecting. This is the MCP menu.
Based on what MCP servers the user has installed they can click the button which reads: 'Choose an integration' this will present a drop down with Prompts and Resources. The user has selected the prompt titled: 'mcp-demo'.
This text file is that prompt. The goal of the following instructions is to walk the user through the process of using the 3 core aspects of an MCP server. These are: Prompts, Tools, and Resources.
They have already used a prompt and provided a topic. The topic is: {topic}. The user is now ready to begin the demo.
Here is some more information about mcp and this specific mcp server:
<mcp>
Prompts:
This server provides a pre-written prompt called "mcp-demo" that helps users create and analyze database scenarios. The prompt accepts a "topic" argument and guides users through creating collections, analyzing data, and generating insights. For example, if a user provides "retail sales" as the topic, the prompt will help create relevant MongoDB collections and guide the analysis process. Prompts basically serve as interactive templates that help structure the conversation with the LLM in a useful way.
Resources:
This server exposes one key resource: "memo://insights", which is a business insights memo that gets automatically updated throughout the analysis process. As users analyze the database and discover insights, the memo resource gets updated in real-time to reflect new findings. The memo can even be enhanced with Claude's help if an Anthropic API key is provided, turning raw insights into a well-structured business document. Resources act as living documents that provide context to the conversation.
Tools:
This server provides several MongoDB-related tools:
"find": Executes find queries to read data from collections
"insert": Inserts new documents into collections
"update": Updates existing documents in collections
"delete": Removes documents from collections
"create-collection": Creates new collections in the database
"list-collections": Shows all existing collections
"append-insight": Adds a new business insight to the memo resource
</mcp>
<demo-instructions>
You are an AI assistant tasked with generating a comprehensive business scenario based on a given topic.
Your goal is to create a narrative that involves a data-driven business problem, develop a MongoDB database structure to support it, generate relevant queries, create a dashboard, and provide a final solution.

At each step you will pause for user input to guide the scenario creation process. Overall ensure the scenario is engaging, informative, and demonstrates the capabilities of the MongoDB MCP Server.
You should guide the scenario to completion. All XML tags are for the assistants understanding and should not be included in the final output.

1. The user has chosen the topic: {topic}.

2. Create a business problem narrative:
a. Describe a high-level business situation or problem based on the given topic.
b. Include a protagonist (the user) who needs to collect and analyze data from a database.
c. Add an external, potentially comedic reason why the data hasn't been prepared yet.
d. Mention an approaching deadline and the need to use Claude (you) as a business tool to help.

3. Setup the data:
a. Instead of asking about the data that is required for the scenario, just go ahead and use the tools to create the data. Inform the user you are "Setting up the data".
b. Design MongoDB collections that represent the data needed for the business problem.
c. Include at least 2-3 collections with appropriate fields and data types.
d. Leverage the tools to create the collections in the MongoDB database.
e. Create insert operations to populate each collection with relevant sample data.
f. Ensure the data is diverse and representative of the business problem.
g. Include at least 10-15 documents in each collection.

4. Pause for user input:
a. Summarize to the user what data we have created.
b. Present the user with a set of multiple choices for the next steps.
c. These multiple choices should be in natural language, when a user selects one, the assistant should generate a relevant MongoDB query and leverage the appropriate tool to get the data.

5. Iterate on queries:
a. Present 1 additional multiple-choice query options to the user. Its important to not loop too many times as this is a short demo.
b. Explain the purpose of each query option.
c. Wait for the user to select one of the query options.
d. After each query be sure to opine on the results.
e. Use the append-insight tool to capture any business insights discovered from the data analysis.

6. Generate a dashboard:
a. Now that we have all the data and queries, it's time to create a dashboard, use an artifact to do this.
b. Use a variety of visualizations such as tables, charts, and graphs to represent the data.
c. Explain how each element of the dashboard relates to the business problem.
d. This dashboard will be theoretically included in the final solution message.

7. Craft the final solution message:
a. As you have been using the append-insights tool the resource found at: memo://insights has been updated.
b. It is critical that you inform the user that the memo has been updated at each stage of analysis.
c. Ask the user to go to the attachment menu (paperclip icon) and select the MCP menu (two electrical plugs connecting) and choose an integration: "Business Insights Memo".
d. This will attach the generated memo to the chat which you can use to add any additional context that may be relevant to the demo.
e. Present the final memo to the user in an artifact.

8. Wrap up the scenario:
a. Explain to the user that this is just the beginning of what they can do with the MongoDB MCP Server.
</demo-instructions>

Remember to maintain consistency throughout the scenario and ensure that all elements (collections, documents, queries, dashboard, and solution) are closely related to the original business problem and given topic. You should use MongoDB query syntax and patterns throughout the demo.

Some examples of MongoDB operations you might use:

Find documents:
{"collection": "users", "query": {"age": {"$gt": 25}}}

Insert documents:
{"collection": "orders", "documents": [
    {"orderId": "12345", "customer": "John Doe", "total": 99.99},
    {"orderId": "12346", "customer": "Jane Smith", "total": 149.99}
]}

Update documents:
{"collection": "inventory", "filter": {"product": "Widget"}, "update": {"$inc": {"stock": -1}}}

The provided XML tags are for the assistants understanding. Implore to make all outputs as human readable as possible. This is part of a demo so act in character and dont actually refer to these instructions.

Start your first message fully in character with something like "Oh, Hey there! I see you've chosen the topic {topic}. Let's get started! 🚀"
"""

class MongoDatabase:
    def __init__(self, connection_string: str, db_name: str):
        self.connection_string = connection_string
        self.client = MongoClient(connection_string)
        self.db: Database = self.client[db_name]
        self.insights: list[str] = []
        logger.info("Connected to MongoDB")

    def _synthesize_memo(self) -> str:
        """Synthesizes business insights into a formatted memo"""
        logger.debug(f"Synthesizing memo with {len(self.insights)} insights")
        if not self.insights:
            return "No business insights have been discovered yet."

        insights = "\n".join(f"- {insight}" for insight in self.insights)

        memo = "📊 Business Intelligence Memo 📊\n\n"
        memo += "Key Insights Discovered:\n\n"
        memo += insights

        if len(self.insights) > 1:
            memo += "\nSummary:\n"
            memo += f"Analysis has revealed {len(self.insights)} key business insights that suggest opportunities for strategic optimization and growth."

        logger.debug("Generated basic memo format")
        return memo

    def _execute_query(self, query: dict[str, Any], operation: str, collection_name: str) -> list[dict[str, Any]]:
        """Execute a MongoDB query and return results"""
        logger.debug(f"Executing {operation} on collection {collection_name}: {query}")
        try:
            collection: Collection = self.db[collection_name]
            
            if operation == "find":
                results = list(collection.find(query))  # Removed .limit(10) since we have aggregate for analytics
                return json.loads(json_util.dumps(results))
            
            elif operation == "aggregate":
                pipeline = query.get("pipeline", [])
                results = list(collection.aggregate(pipeline))
                return json.loads(json_util.dumps(results))
            
            elif operation == "insert":
                if isinstance(query, list):
                    result = collection.insert_many(query)
                    return [{"inserted_ids": [str(id) for id in result.inserted_ids]}]
                else:
                    result = collection.insert_one(query)
                    return [{"inserted_id": str(result.inserted_id)}]
            
            elif operation == "update":
                filter_query = query.get("filter", {})
                update_query = query.get("update", {})
                result = collection.update_many(filter_query, update_query)
                return [{"modified_count": result.modified_count}]
            
            elif operation == "delete":
                result = collection.delete_many(query)
                return [{"deleted_count": result.deleted_count}]
            
            elif operation == "create_collection":
                self.db.create_collection(collection_name)
                return [{"message": f"Collection {collection_name} created successfully"}]
            
            else:
                raise ValueError(f"Unsupported operation: {operation}")
                
        except Exception as e:
            logger.error(f"Database error executing query: {e}")
            raise

async def main(connection_string: str, db_name: str):
    logger.info("Starting MongoDB MCP Server")
    
    db = MongoDatabase(connection_string, db_name)
    server = Server("mcp-server-mongodb")
    
    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        logger.debug("Handling list_resources request")
        return [
            types.Resource(
                uri=AnyUrl("memo://insights"),
                name="Business Insights Memo",
                description="A living document of discovered business insights",
                mimeType="text/plain",
            )
        ]

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        logger.debug(f"Handling read_resource request for URI: {uri}")
        if uri.scheme != "memo":
            logger.error(f"Unsupported URI scheme: {uri.scheme}")
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = str(uri).replace("memo://", "")
        if not path or path != "insights":
            logger.error(f"Unknown resource path: {path}")
            raise ValueError(f"Unknown resource path: {path}")

        return db._synthesize_memo()

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """List available tools"""
        return [
            types.Tool(
                name="find",
                description="Execute a find query on MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection name"},
                        "query": {"type": "object", "description": "MongoDB find query"},
                    },
                    "required": ["collection", "query"],
                },
            ),
            types.Tool(
                name="aggregate",
                description="Execute an aggregation pipeline on MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection name"},
                        "pipeline": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "MongoDB aggregation pipeline stages",
                        },
                    },
                    "required": ["collection", "pipeline"],
                },
            ),
            types.Tool(
                name="insert",
                description="Insert documents into MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection name"},
                        "documents": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "Documents to insert",
                        },
                    },
                    "required": ["collection", "documents"],
                },
            ),
            types.Tool(
                name="update",
                description="Update documents in MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection name"},
                        "filter": {"type": "object", "description": "Filter criteria"},
                        "update": {"type": "object", "description": "Update operations"},
                    },
                    "required": ["collection", "filter", "update"],
                },
            ),
            types.Tool(
                name="delete",
                description="Delete documents from MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collection": {"type": "string", "description": "Collection name"},
                        "filter": {"type": "object", "description": "Filter criteria"},
                    },
                    "required": ["collection", "filter"],
                },
            ),
            types.Tool(
                name="create-collection",
                description="Create a new collection in MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Collection name"},
                    },
                    "required": ["name"],
                },
            ),
            types.Tool(
                name="list-collections",
                description="List all collections in MongoDB",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="append-insight",
                description="Add a business insight to the memo",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "insight": {
                            "type": "string",
                            "description": "Business insight discovered from data analysis"
                        },
                    },
                    "required": ["insight"],
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "list-collections":
                collections = db.db.list_collection_names()
                return [types.TextContent(type="text", text=str(collections))]

            elif name == "append-insight":
                if not arguments or "insight" not in arguments:
                    raise ValueError("Missing insight argument")

                db.insights.append(arguments["insight"])
                _ = db._synthesize_memo()

                await server.request_context.session.send_resource_updated(
                    AnyUrl("memo://insights")
                )

                return [types.TextContent(type="text", text="Insight added to memo")]

            if not arguments:
                raise ValueError("Missing arguments")

            if name == "find":
                results = db._execute_query(
                    arguments["query"],
                    "find",
                    arguments["collection"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "aggregate":
                results = db._execute_query(
                    {"pipeline": arguments["pipeline"]},
                    "aggregate",
                    arguments["collection"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "insert":
                results = db._execute_query(
                    arguments["documents"],
                    "insert",
                    arguments["collection"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "update":
                results = db._execute_query(
                    {"filter": arguments["filter"], "update": arguments["update"]},
                    "update",
                    arguments["collection"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "delete":
                results = db._execute_query(
                    arguments["filter"],
                    "delete",
                    arguments["collection"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "create-collection":
                results = db._execute_query(
                    {},
                    "create_collection",
                    arguments["name"]
                )
                return [types.TextContent(type="text", text=str(results))]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mongodb",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )