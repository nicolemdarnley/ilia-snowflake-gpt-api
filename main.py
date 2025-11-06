from fastapi import FastAPI
import snowflake.connector
from fastapi.responses import JSONResponse
import os

app = FastAPI()

# Read Snowflake credentials from environment variables
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_USER = os.getenv("SF_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE = os.getenv("SF_DATABASE", "ANALYTICS")
SF_SCHEMA = os.getenv("SF_SCHEMA", "GPTS")

@app.get("/data")
def get_gpt_data():
    try:
        ctx = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
        )
        cs = ctx.cursor()
        cs.execute("SELECT * FROM gpt_innovation_analyst")
        columns = [col[0] for col in cs.description]
        results = [dict(zip(columns, row)) for row in cs.fetchall()]
        cs.close()
        ctx.close()
        return JSONResponse(content=results)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
