# Text-to-SQL Interface

AI-powered natural language query interface for NYC Taxi Analytics using Vanna AI 2.0, OpenAI, and AWS services.

## Architecture

**Stack:**
- **Vanna AI 2.0** (Agent API) - Text-to-SQL with RAG
- **OpenAI GPT-5-mini** - LLM for SQL generation
- **ChromaDB** - Vector storage for schema and context
- **AWS EFS** - Persistent storage for ChromaDB data
- **FastAPI** - REST API and UI server
- **AWS Athena** - Query execution engine
- **AWS Glue** - Data catalog and schema source
- **Plotly** - Chart generation

**Data Flow:**
1. User asks question in natural language (via UI or API)
2. Vanna Agent retrieves relevant context from ChromaDB (DDLs, examples)
3. OpenAI generates SQL query based on context
4. Query executes on Athena, results returned
5. Optional: Generate Plotly charts for visualization

**Storage Architecture:**
- **EFS mount**: `/mnt/efs/vanna/chroma_db`
  - `legacy_vanna/` - ChromaDB for table schemas (DDLs)
  - `agent_memory/` - ChromaDB for agent tool memory
- **No local storage** - All data persists on EFS across restarts
- **No retraining** - Cached schemas loaded from ChromaDB on startup

## Features

✅ **Auto Glue Integration** - Loads schemas from AWS Glue Data Catalog  
✅ **Natural Language Queries** - Ask questions in plain English  
✅ **SQL Generation & Execution** - Generates and runs Athena queries  
✅ **Chart Generation** - Auto-generates Plotly charts for results  
✅ **FastAPI Backend** - RESTful API with OpenAPI docs  
✅ **Web UI** - Interactive chat interface (Vanna components)  
✅ **Persistent Storage** - ChromaDB on EFS for zero retraining  
✅ **Lookup Tables** - Smart JOINs for payment types, vendors, zones  

## Prerequisites

1. **AWS Resources** (provisioned via Terraform):
   - Glue database with tables (crawlers run)
   - Athena workgroup and S3 results bucket
   - EFS file system for ChromaDB storage
   - EC2 instance with IAM role (EFS, Athena, Glue, SSM access)

2. **OpenAI API Key**:
   - Stored in SSM Parameter Store: `/nyc-taxi-analytics/dev/llm-api-key`
   - Or set `OPENAI_API_KEY` environment variable (local only)

3. **Python 3.11+**

## Local Development

### Quick Start (Automated)

Use the convenience script to automatically set up and run:

```bash
cd text-to-sql
./run_local.sh
```

This script will:
- Generate `.env` from Terraform outputs (if missing)
- Create virtual environment (if needed)
- Install/update dependencies
- Validate configuration
- Start the server with auto-reload

### Manual Setup

1. **Install dependencies**:
   ```bash
   cd text-to-sql
   python3.11 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment** (`.env` file):
   ```bash
   OPENAI_API_KEY=your-key-here
   AWS_REGION=us-east-1
   ATHENA_DATABASE=nyc-taxi-analytics_dev_db
   ATHENA_WORKGROUP=nyc-taxi-analytics-dev-workgroup
   ATHENA_S3_STAGING=s3://your-bucket-athena-results/
   GLUE_DATABASE=nyc-taxi-analytics_dev_db
   LLM_PROVIDER=openai
   LLM_MODEL=gpt-5-mini
   # Optional: simulate EFS locally
   # EFS_MOUNT_POINT=/path/to/local/efs
   ```

   Or generate from Terraform:
   ```bash
   python3 setup_local_env.py
   ```

3. **Run the server**:
   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000 --reload
   ```

4. **Access**:
   - UI: http://localhost:8000
   - API Docs: http://localhost:8000/docs
   - Health: http://localhost:8000/health

## EC2 Deployment (Production)

### Prerequisites
1. **Provision Infrastructure** (from terraform root):
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```
   This creates: EFS, EC2 instance, security groups, IAM roles

2. **Outputs to Note**:
   - `vanna_ai_public_ip` - EC2 public IP
   - `efs_id` - EFS file system ID
   - `vanna_ai_url` - Application URL

### Deployment Process

The EC2 instance is **auto-configured** via `user_data.sh`:
1. Installs dependencies (Python 3.11, amazon-efs-utils)
2. Mounts EFS at `/mnt/efs/vanna/chroma_db` using NFS4
3. Downloads app files from S3
4. Creates Python virtual environment
5. Retrieves OpenAI API key from SSM Parameter Store
6. Creates systemd service `vanna-ai.service`
7. Starts the application

### Verify Deployment

1. **SSH to EC2**:
   ```bash
   ssh -i terraform/key-pairs/nyc-taxi-analytics-dev.pem ec2-user@<public_ip>
   ```

2. **Check EFS Mount**:
   ```bash
   df -h | grep efs
   ls -la /mnt/efs/vanna/chroma_db/
   ```

3. **Check Application Logs**:
   ```bash
   sudo journalctl -u vanna-ai -f
   ```

4. **Verify Health**:
   ```bash
   curl http://<public_ip>:8000/health
   ```

5. **Access UI**:
   Open `http://<public_ip>:8000` in browser

### EFS Storage Verification

Ensure **all data is on EFS, not local**:
```bash
# On EC2:
ls -lh /mnt/efs/vanna/chroma_db/legacy_vanna/chroma.sqlite3  # ChromaDB data
du -sh /mnt/efs/vanna/chroma_db/agent_memory/                # Agent memory
[ ! -d /opt/vanna-ai/.chroma_db ] && echo "✅ No local storage"
```

### Service Management

```bash
# Restart service
sudo systemctl restart vanna-ai

# View logs
sudo journalctl -u vanna-ai -n 50

# Check status
sudo systemctl status vanna-ai
```

### Important: No Retraining on Restart

After first deployment:
- Schemas are trained once and stored in ChromaDB on EFS
- Subsequent restarts load cached data from EFS
- Verify logs show: `✅ Using cached training data - no new tables detected`

## Example Questions

- "What payment methods are used most frequently?"
- "Show me trip volume by hour"
- "Which pickup zones are most popular?"
- "What's the average fare by payment type?"
- "Compare revenue between vendors"
- "Show me congestion fee analysis"

## Data Loading & Training

### Initial Training (First Start Only)

On first start, the application:
1. Fetches all table DDLs from AWS Glue Data Catalog
2. Trains Vanna on 25+ tables (trips, aggregations, lookup tables)
3. Stores schemas in ChromaDB on EFS (`/mnt/efs/vanna/chroma_db/legacy_vanna/`)
4. Takes ~30-60 seconds

### Subsequent Starts (Cached)

On restart:
1. Loads cached schemas from ChromaDB (EFS)
2. Skips retraining if no new tables detected
3. Starts in ~3-5 seconds

### Tables Trained

- **Raw data**: `trips_cleaned` (28M+ rows)
- **Aggregations**: trip_volume, revenue, tip_analysis, etc. (25 tables)
- **Lookup tables**: payment_type, vendor, taxi_zone (for JOINs)

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI |
| `/health` | GET | Health check |
| `/api/vanna/v2/chat` | POST | Chat (streaming SSE) |
| `/api/vanna/v2/chat_poll` | POST | Chat (polling) |
| `/docs` | GET | OpenAPI docs |

## Troubleshooting

### EFS Not Mounted
```bash
# Check mount
df -h | grep efs

# Manually mount (if needed)
sudo mount -t nfs4 -o nfsvers=4.1 <mount_target_ip>:/ /mnt/efs/vanna/chroma_db
```

### Application Not Starting
```bash
# Check logs
sudo journalctl -u vanna-ai -n 100

# Common issues:
# - EFS not writable: sudo chown -R ec2-user:ec2-user /mnt/efs/vanna/chroma_db
# - Missing API key: Check SSM parameter /nyc-taxi-analytics/dev/llm-api-key
# - Dependencies: cd /opt/vanna-ai && source venv/bin/activate && pip list
```

### Query Failures
- Check Athena permissions in IAM role
- Verify Glue database has tables (run crawlers)
- Review generated SQL in API response

### Retraining on Every Start
- Verify `force_refresh=False` in `app.py` line 386
- Check ChromaDB data exists on EFS
- Review logs for "Using cached training data"

## Testing

**Test script** (works for both local and EC2):
```bash
cd text-to-sql

# Test localhost (default)
python test_query.py

# Test EC2 instance
python test_query.py http://<ec2-ip>:8000

# Test with custom question
python test_query.py http://localhost:8000 "Show me trip volume by hour"

# Or use environment variable
export VANNA_API_URL=http://100.52.54.194:8000
python test_query.py
```

## Files

- `app.py` - FastAPI application (Vanna 2.0 Agent)
- `config.py` - Configuration loader (env, SSM)
- `athena_tool.py` - Custom Athena SQL runner tool
- `chart_tool.py` - Plotly chart generation tool
- `glue_training.py` - Glue schema training service
- `requirements.txt` - Python dependencies
- `index.html` - Web UI (Vanna chat component)

## Terraform

- `terraform/modules/vanna_ai/` - EC2, IAM, S3 uploads
- `terraform/modules/efs/` - EFS file system, mount targets
- `terraform/modules/vanna_ai/user_data.sh` - EC2 bootstrap script

## License

MIT License

Copyright (c) 2026

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
