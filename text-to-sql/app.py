"""
NYC Taxi Data Analytics - Text-to-SQL Interface (Vanna 2.0 + FastAPI)

FastAPI application for natural language queries on NYC Taxi data.
Uses Vanna AI 2.0 with AWS Athena and Glue Data Catalog integration.
"""
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from config import Config
from athena_tool import AthenaRunner
from glue_training import GlueTrainingService

# Validate configuration
try:
    Config.validate()
except ValueError as e:
    print(f"Configuration Error: {e}")
    print("Please set required environment variables or configure SSM Parameter Store.")
    raise
        



# Initialize Vanna 2.0 components with LegacyVannaAdapter
# This allows us to use legacy training methods (train(ddl=...)) while getting 2.0 features
try:
    from vanna import Agent, AgentConfig
    from vanna.tools import RunSqlTool
    from chart_tool import AthenaChartTool
    from vanna.legacy.adapter import LegacyVannaAdapter
    from vanna.core.user import UserResolver, User, RequestContext
    
    # Import ChromaAgentMemory for persistent storage
    try:
        from vanna.integrations.chromadb import ChromaAgentMemory
    except ImportError:
        ChromaAgentMemory = None
        print("⚠️  ChromaAgentMemory not available, will use default memory")
    
    # Import legacy Vanna for training
    # We'll use VannaDefault which handles storage automatically
    # Imports for LLM classes will be done conditionally in lifespan
    
    # Import OpenAI LLM service (OpenAI only - no Anthropic support)
    from vanna.integrations.openai import OpenAILlmService
    llm_service = OpenAILlmService(
        api_key=Config.LLM_API_KEY,
        model=Config.LLM_MODEL or "gpt-5-mini"  # GPT-5 Mini model
    )
    
    from vanna.servers.fastapi.routes import register_chat_routes
    from vanna.servers.base import ChatHandler
except ImportError as e:
    print(f"Error importing Vanna 2.0 modules: {e}")
    print("Make sure Vanna 2.0 is installed: pip install 'vanna[fastapi]'")
    raise
        

# Simple user resolver (can be enhanced later)
class SimpleUserResolver(UserResolver):
    """Simple user resolver for authentication"""
    
    async def resolve_user(self, request_context: RequestContext) -> User:
        """Resolve user from request context"""
        # For now, return a default user
        # Can be enhanced to extract user from headers, cookies, etc.
        user_id = request_context.get_header('X-User-ID') or 'anonymous'
        return User(
            id=user_id,
            email=f"{user_id}@example.com",
            group_memberships=['users']
        )


# Custom system prompt builder with schema context
class SchemaAwareSystemPromptBuilder:
    """Custom system prompt builder that includes database schema"""
    
    def __init__(self):
        """Initialize with Glue training service to get DDLs"""
        self._ddls_cache = None
        self._load_ddls()
    
    def _load_ddls(self):
        """Load DDLs from Glue for all tables"""
        try:
            import boto3
            from glue_training import GlueTrainingService
            
            # Create a dummy agent just to use the DDL generation
            class DummyAgent:
                pass
            
            glue_service = GlueTrainingService(DummyAgent())
            tables = glue_service.get_glue_tables()
            
            self._ddls_cache = {}
            for table in tables:
                table_name = table['Name']
                ddl = glue_service.generate_ddl_from_glue(table_name)
                if ddl:
                    # Add database qualification to DDL
                    db_quoted = f'"{Config.ATHENA_DATABASE}"'
                    # Replace unqualified table name with qualified one
                    ddl_qualified = ddl.replace(f"CREATE EXTERNAL TABLE {table_name}", 
                                              f"CREATE EXTERNAL TABLE {db_quoted}.{table_name}")
                    self._ddls_cache[table_name] = ddl_qualified
        except Exception as e:
            print(f"Warning: Could not load DDLs for system prompt: {e}")
            self._ddls_cache = {}
    
    async def build_system_prompt(self, user, tool_schemas) -> str:
        """Build system prompt with schema information"""
        
        db_name = Config.ATHENA_DATABASE
        db_quoted = f'"{db_name}"'
        
        base_prompt = f"""You are a helpful SQL assistant for NYC Taxi data analytics.
You have access to an AWS Athena database with NYC Yellow Taxi trip data.

**COMMUNICATION STYLE:**
- Be concise and direct in your responses
- Execute queries immediately without asking for confirmation
- Do NOT ask "Would you like me to run this query?" or "Should I execute this?"
- Just run the query and show the results
- Only ask for clarification or correction if a query fails or returns an error
- If a query fails, explain the error briefly and ask what the user intended

**CRITICAL DATABASE INFORMATION:**

Database Name: {db_name}
Database (quoted for SQL): {db_quoted}

**IMPORTANT: Always use fully qualified table names with database name in double quotes:**
- Format: {db_quoted}.table_name
- Example: {db_quoted}.trips_cleaned
- The database name contains hyphens, so it MUST be wrapped in double quotes
- NEVER use unqualified table names - always prefix with {db_quoted}.

**MAIN DATA TABLE:**
- {db_quoted}.trips_cleaned - Partitioned NYC taxi trips (ALWAYS use this for trip queries)
  Partition columns (REQUIRED in WHERE clause for performance):
  - pickup_year (integer)
  - pickup_month (integer)
  
  Data columns:
  - tpep_pickup_datetime (timestamp) - Trip pickup time
  - tpep_dropoff_datetime (timestamp) - Trip dropoff time
  - passenger_count (integer)
  - trip_distance (double) - Miles
  - fare_amount (double)
  - extra (double)
  - mta_tax (double)
  - tip_amount (double)
  - tolls_amount (double)
  - total_amount (double)
  - payment_type (integer) - 1=Credit, 2=Cash, etc.
  - rate_code_id (integer)
  - pickup_location_id (integer)
  - dropoff_location_id (integer)
  - vendor_id (integer)

**LOOKUP TABLES:**
- {db_quoted}.payment_type_lookup - Maps payment_type_id to payment_type_name
  Columns: payment_type_id (int), payment_type_name (string)
  Use: JOIN {db_quoted}.payment_type_lookup p ON table.payment_type = p.payment_type_id
  Then select: p.payment_type_name (NOT payment_type_desc, description, or payment_type)
- {db_quoted}.taxi_zone_lookup - Maps locationid to zone, borough, service_zone
  Columns: locationid (int), borough (string), zone (string), service_zone (string)
  Use: JOIN {db_quoted}.taxi_zone_lookup z ON table.pickup_location_id = z.locationid
  Then select: z.zone, z.borough (column names are lowercase)
- {db_quoted}.vendor_lookup - Maps vendor_id to vendor_name
  Columns: vendor_id (int), vendor_name (string)
  Use: JOIN {db_quoted}.vendor_lookup v ON table.vendorid = v.vendor_id
  Then select: v.vendor_name

**AGGREGATED VIEWS:**
- {db_quoted}.monthly_trends, {db_quoted}.trip_volume_by_hour, {db_quoted}.popular_pickup_zones, {db_quoted}.popular_dropoff_zones, {db_quoted}.revenue_by_payment_type, {db_quoted}.tip_analysis

**QUERY GUIDELINES:**
1. ALWAYS use fully qualified table names: {db_quoted}.table_name
2. ALWAYS query {db_quoted}.trips_cleaned with partition filters: WHERE pickup_year = YYYY AND pickup_month = M
3. For date ranges, use: tpep_pickup_datetime >= '2025-01-01' AND tpep_pickup_datetime < '2025-02-01'
4. Join with {db_quoted}.taxi_zone_lookup for location names
5. DO NOT use SHOW TABLES or information_schema - query tables directly
6. NEVER use unqualified table names - always prefix with {db_quoted}

**EXAMPLE QUERIES:**

Count January 2025 trips:
```sql
SELECT COUNT(*) as trip_count
FROM {db_quoted}.trips_cleaned
WHERE pickup_year = 2025 AND pickup_month = 1
```

Average fare by payment type in January 2025:
```sql
SELECT p.payment_type_name, AVG(t.fare_amount) as avg_fare
FROM {db_quoted}.trips_cleaned t
JOIN {db_quoted}.payment_type_lookup p ON t.payment_type = p.payment_type_id
WHERE t.pickup_year = 2025 AND t.pickup_month = 1
GROUP BY p.payment_type_name
```

Top pickup zones in January 2025:
```sql
SELECT z.zone as zone_name, z.borough, COUNT(*) as trip_count
FROM {db_quoted}.trips_cleaned t
JOIN {db_quoted}.taxi_zone_lookup z ON t.pickup_location_id = z.locationid
WHERE t.pickup_year = 2025 AND t.pickup_month = 1
GROUP BY z.zone, z.borough
ORDER BY trip_count DESC
LIMIT 10
```

**TABLE SCHEMAS (DDLs):**

Below are the CREATE TABLE statements for all tables in the database. Use these to understand column names, data types, and table structures:

"""
        
        # Add DDLs for all tables, prioritizing lookup tables first
        lookup_tables = ['payment_type_lookup', 'taxi_zone_lookup', 'vendor_lookup']
        other_tables = [name for name in (self._ddls_cache.keys() if self._ddls_cache else []) 
                       if name not in lookup_tables]
        
        # Add lookup tables first (most important)
        if self._ddls_cache:
            base_prompt += "\n**LOOKUP TABLES DDLs:**\n"
            for table_name in lookup_tables:
                if table_name in self._ddls_cache:
                    base_prompt += f"\n{self._ddls_cache[table_name]}\n"
            
            # Add other tables
            if other_tables:
                base_prompt += "\n**OTHER TABLES DDLs:**\n"
                for table_name in sorted(other_tables):
                    base_prompt += f"\n{self._ddls_cache[table_name]}\n"
        else:
            base_prompt += "\n(DDLs not available - using table descriptions above)\n"
        
        base_prompt += """
**EXECUTION BEHAVIOR:**
- When the user asks a question, immediately generate and execute the appropriate SQL query
- Do not ask for permission or confirmation before executing
- Show results directly after execution
- If visualization is requested, create a chart automatically
- Only stop and ask questions if:
  * The query fails with an error
  * The results are clearly wrong or unexpected

Now answer the user's question using the database schema above. Execute queries immediately without asking for confirmation."""
        return base_prompt


# Global agent instance (will be initialized in startup)
agent = None
glue_training_service = None
athena_runner = None


# This function is no longer needed - we use legacy training methods instead


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global agent, glue_training_service, athena_runner
    
    # Startup: Initialize agent and run training
    print("🚀 Initializing Vanna 2.0 Agent...")
    
    # Create Athena runner
    athena_runner = AthenaRunner(
        database=Config.ATHENA_DATABASE,
        workgroup=Config.ATHENA_WORKGROUP,
        output_location=Config.ATHENA_S3_STAGING,
        region_name=Config.AWS_REGION
    )
    
    # Create legacy Vanna instance with ChromaDB for training
    # ChromaDB will use EFS (Elastic File System) for persistent storage
    # EFS provides POSIX-compliant NFS, perfect for ChromaDB's file operations
    
    # Determine ChromaDB storage paths
    # EFS_MOUNT_POINT can be set for both local development and EC2 production
    # If EFS_MOUNT_POINT is set, we REQUIRE EFS to be available (no fallback)
    # If not set, use local storage
    efs_mount_point = os.getenv('EFS_MOUNT_POINT')
    
    if efs_mount_point:
        # EFS is REQUIRED when EFS_MOUNT_POINT is set (works for both local and EC2)
        efs_path = Path(efs_mount_point)
        if not efs_path.exists():
            raise RuntimeError(
                f"❌ EFS mount point does not exist: {efs_mount_point}\n"
                f"   EFS_MOUNT_POINT is set, but the directory is not mounted.\n"
                f"   Please ensure EFS is properly mounted before starting the application.\n"
                f"   To mount EFS locally, use: mount -t efs -o tls <efs-id>:/ {efs_mount_point}"
            )
        if not os.access(efs_path, os.W_OK):
            raise RuntimeError(
                f"❌ EFS mount point is not writable: {efs_mount_point}\n"
                f"   Please check EFS mount permissions and IAM roles."
            )
        
        # Use EFS: separate directories for legacy Vanna and agent memory
        chroma_db_path = efs_path / "legacy_vanna"  # For legacy Vanna DDLs/training
        agent_memory_path = efs_path / "agent_memory"  # For ChromaAgentMemory
        
        # Ensure directories exist
        chroma_db_path.mkdir(parents=True, exist_ok=True)
        agent_memory_path.mkdir(parents=True, exist_ok=True)
        
        print(f"✅ Using EFS for ChromaDB storage:")
        print(f"   EFS Mount Point: {efs_mount_point}")
        print(f"   Legacy Vanna: {chroma_db_path}")
        print(f"   Agent Memory: {agent_memory_path}")
    else:
        # Local development: use local storage (EFS_MOUNT_POINT not set)
        local_fallback_base = Path(__file__).parent / ".chroma_db"
        chroma_db_path = local_fallback_base / "legacy_vanna"
        agent_memory_path = local_fallback_base / "agent_memory"
        chroma_db_path.mkdir(parents=True, exist_ok=True)
        agent_memory_path.mkdir(parents=True, exist_ok=True)
        print(f"⚠️  Using local storage (EFS_MOUNT_POINT not set):")
        print(f"   Legacy Vanna: {chroma_db_path}")
        print(f"   Agent Memory: {agent_memory_path}")
        print(f"   To use EFS locally, set EFS_MOUNT_POINT environment variable and mount EFS.")
    
    print("🔄 Creating legacy Vanna instance with ChromaDB (EFS-backed)...")
    from vanna.legacy.chromadb import ChromaDB_VectorStore
    from vanna.legacy.openai import OpenAI_Chat
    
    # Create a custom Vanna class that uses ChromaDB for storage
    # ChromaDB will use EFS (or local fallback) for persistent storage
    class LocalChromaVanna(ChromaDB_VectorStore, OpenAI_Chat):
        """Custom Vanna class with ChromaDB storage (EFS-backed) and OpenAI LLM"""
        def __init__(self, config=None):
            ChromaDB_VectorStore.__init__(self, config=config)
            OpenAI_Chat.__init__(self, config=config)
        
        # Implement abstract methods required by ChromaDB_VectorStore
        def assistant_message(self, message: str) -> str:
            return message
        
        def user_message(self, message: str) -> str:
            return message
        
        def system_message(self, message: str) -> str:
            return message
        
        def submit_prompt(self, prompt: str, **kwargs) -> str:
            return OpenAI_Chat.submit_prompt(self, prompt, **kwargs)
    
    # Create legacy Vanna instance with ChromaDB storage on EFS
    vanna_config = {
        'api_key': Config.LLM_API_KEY,
        'model': Config.LLM_MODEL or 'gpt-5-mini',  # GPT-5 Mini model
        'path': str(chroma_db_path)  # ChromaDB storage path (EFS or local fallback)
    }
    
    legacy_vanna = LocalChromaVanna(config=vanna_config)
    
    print(f"✅ Legacy Vanna instance created with ChromaDB")
    print(f"  ChromaDB path: {chroma_db_path}")
    print(f"  Agent Memory path: {agent_memory_path}")
    print(f"  Storage type: {'EFS' if efs_mount_point else 'Local'}")
    print(f"  Model: {vanna_config['model']}")
    if efs_mount_point:
        print(f"  ✅ All data stored persistently on EFS")
    else:
        print(f"  ⚠️  Using local storage (set EFS_MOUNT_POINT to use EFS)")
    
    # Train on Glue Data Catalog using legacy training methods
    print("🔄 Training on Glue Data Catalog (using legacy train() methods)...")
    from glue_training import GlueTrainingService
    glue_training_service = GlueTrainingService(legacy_vanna)  # Pass legacy instance
    training_result = glue_training_service.train_from_glue_catalog(force_refresh=False)
    print(f"✅ Training complete: {training_result}")
    
    # Wrap legacy Vanna with adapter for 2.0 features
    # The adapter bridges legacy training methods with 2.0 agent features
    print("🔄 Wrapping with LegacyVannaAdapter for Vanna 2.0 features...")
    legacy_adapter = LegacyVannaAdapter(legacy_vanna)
    
    # Create SQL tool for direct SQL execution
    sql_tool = RunSqlTool(sql_runner=athena_runner)
    
    # Create charting tool for data visualization
    print("🔄 Setting up charting capabilities...")
    try:
        chart_tool = AthenaChartTool(sql_runner=athena_runner)
        print("  ✅ AthenaChartTool created")
    except Exception as e:
        print(f"  ⚠️  Could not create chart tool: {e}")
        import traceback
        traceback.print_exc()
        chart_tool = None
    
    # Register SQL tool with adapter
    # The adapter implements ToolRegistry interface and should support tool registration
    tool_registry_to_use = legacy_adapter
    try:
        if hasattr(legacy_adapter, 'register_local_tool'):
            legacy_adapter.register_local_tool(sql_tool, access_groups=['users'])
            print("  ✅ SQL tool registered with adapter")
            # Register chart tool if available
            if chart_tool:
                try:
                    legacy_adapter.register_local_tool(chart_tool, access_groups=['users'])
                    print("  ✅ Chart tool registered with adapter")
                except Exception as e:
                    print(f"  ⚠️  Could not register chart tool: {e}")
        else:
            # If adapter doesn't support registration, create a composite registry
            # We'll combine adapter (for training data) with a tool registry (for SQL tool)
            from vanna.core.registry import ToolRegistry
            tool_registry = ToolRegistry()
            tool_registry.register_local_tool(sql_tool, access_groups=['users'])
            # Register chart tool if available
            if chart_tool:
                try:
                    tool_registry.register_local_tool(chart_tool, access_groups=['users'])
                    print("  ✅ Chart tool registered")
                except Exception as e:
                    print(f"  ⚠️  Could not register chart tool: {e}")
            # Use tool registry as primary (has SQL tool), adapter provides training data access
            tool_registry_to_use = tool_registry
            print("  ⚠️  Adapter doesn't support tool registration - using separate tool registry")
            print("  Note: Training data is in adapter, SQL tool is in separate registry")
    except Exception as e:
        print(f"  ⚠️  Could not register SQL tool: {e}")
        # Create separate tool registry as fallback
        from vanna.core.registry import ToolRegistry
        tool_registry = ToolRegistry()
        tool_registry.register_local_tool(sql_tool, access_groups=['users'])
        # Register chart tool if available
        if chart_tool:
            try:
                tool_registry.register_local_tool(chart_tool, access_groups=['users'])
                print("  ✅ Chart tool registered")
            except Exception as e:
                print(f"  ⚠️  Could not register chart tool: {e}")
        tool_registry_to_use = tool_registry
        print("  Using separate tool registry for SQL tool")
    
    # Create agent with custom system prompt
    print("🔄 Creating user resolver and agent config...")
    user_resolver = SimpleUserResolver()
    
    # Create agent config with custom system prompt builder
    agent_config = AgentConfig()
    system_prompt_builder = SchemaAwareSystemPromptBuilder()
    
    # Create agent memory (ChromaDB for persistence on EFS)
    print("🔄 Creating agent memory...")
    if ChromaAgentMemory:
        try:
            agent_memory = ChromaAgentMemory(
                persist_directory=str(agent_memory_path),
                collection_name="vanna_tool_memory"
            )
            print(f"✅ Created ChromaAgentMemory with path: {agent_memory_path}")
        except Exception as e:
            print(f"⚠️  Failed to create ChromaAgentMemory: {e}")
            # Fallback: Create a simple AgentMemory instance
            from vanna.capabilities.agent_memory.base import AgentMemory
            agent_memory = AgentMemory()
            print("⚠️  Using base AgentMemory (not persistent)")
    else:
        # Fallback: Create a simple AgentMemory instance
        from vanna.capabilities.agent_memory.base import AgentMemory
        agent_memory = AgentMemory()
        print("⚠️  Using base AgentMemory (not persistent)")
    
    # Create agent using tool registry (adapter or composite)
    # The adapter provides access to the trained knowledge base (DDLs in ChromaDB)
    print("🔄 Creating Agent instance...")
    try:
        agent = Agent(
            llm_service=llm_service,
            agent_memory=agent_memory,
            tool_registry=tool_registry_to_use,  # Adapter (training data) or composite (training + SQL tool)
            user_resolver=user_resolver,
            config=agent_config,
            system_prompt_builder=system_prompt_builder
        )
        print("✅ Agent created successfully")
    except Exception as e:
        print(f"❌ Failed to create Agent: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    # Store reference to legacy_vanna for potential access to training data
    agent._legacy_vanna = legacy_vanna
    agent._legacy_adapter = legacy_adapter
    
    print("✅ Agent created with LegacyVannaAdapter (DDLs stored in ChromaDB)")
    
    # IMPORTANT: Wrap agent in ChatHandler BEFORE registering routes
    chat_handler = ChatHandler(agent)
    
    # Register Vanna routes with ChatHandler (not Agent directly)
    register_chat_routes(app, chat_handler)
    print("✅ Vanna routes registered successfully")
    
    yield
    
    # Shutdown: Clean up resources
    print("🛑 Shutting down...")
    if athena_runner:
        athena_runner.close()


# Create FastAPI app
app = FastAPI(
    title="NYC Taxi Data Analytics - Text-to-SQL",
    description="Natural language query interface for NYC Taxi data using Vanna AI 2.0",
    version="2.0.0",
    lifespan=lifespan
)


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "text-to-sql-api",
        "version": "2.0.0",
        "database": Config.ATHENA_DATABASE
    }


# Debug endpoint to check training data
@app.get("/debug/training")
async def debug_training():
    """Debug endpoint to see what training data the agent has"""
    global agent, glue_training_service
    
    if agent is None:
        return {"status": "Agent not initialized"}
    
    if glue_training_service is None:
        return {"status": "Training service not initialized"}
    
    try:
        # Try to inspect agent memory/knowledge base
        info = {
            "status": "ok",
            "agent_memory_type": type(agent.agent_memory).__name__,
            "has_tool_registry": hasattr(agent, 'tool_registry'),
        }
        
        # Try to get tool count
        if hasattr(agent, 'tool_registry'):
            if hasattr(agent.tool_registry, '_tools'):
                info["tool_count"] = len(agent.tool_registry._tools)
            elif hasattr(agent.tool_registry, 'tools'):
                info["tool_count"] = len(agent.tool_registry.tools)
        
        # Try to check if agent has knowledge base methods
        info["has_add_ddl"] = hasattr(agent, 'add_ddl')
        info["has_add_sql"] = hasattr(agent, 'add_sql')
        info["has_add_documentation"] = hasattr(agent, 'add_documentation')
        info["has_knowledge_base"] = hasattr(agent, 'knowledge_base')
        
        return info
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


# Debug endpoint to show schema information
@app.get("/debug/schema")
async def debug_schema():
    """Show the schema information the agent is using"""
    builder = SchemaAwareSystemPromptBuilder()
    prompt = await builder.build_system_prompt(None, None)
    return {"system_prompt": prompt}


# Serve static files (for HTML frontend)
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# Serve index.html at root
@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML page"""
    html_file = Path(__file__).parent / "index.html"
    if html_file.exists():
        return FileResponse(html_file)
    else:
        return HTMLResponse("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>NYC Taxi Data Analytics</title>
            <script type="module" src="https://img.vanna.ai/vanna-components.js"></script>
        </head>
        <body>
            <h1>NYC Taxi Data Analytics - Text-to-SQL</h1>
            <vanna-chat sse-endpoint="/api/vanna/v2/chat_sse"></vanna-chat>
        </body>
        </html>
        """)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
