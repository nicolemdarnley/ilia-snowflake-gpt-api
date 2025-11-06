import os
import snowflake.connector
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/data")
def get_gpt_data():
    try:
        with open("rsa_key.pem", "rb") as key_file:
            private_key = key_file.read()

        ctx = snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            account=os.getenv("SF_ACCOUNT"),
            private_key=private_key,
            warehouse=os.getenv("SF_WAREHOUSE"),
            database=os.getenv("SF_DATABASE", "ANALYTICS"),
            schema=os.getenv("SF_SCHEMA", "GPTS"),
            role=os.getenv("SF_ROLE")
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
