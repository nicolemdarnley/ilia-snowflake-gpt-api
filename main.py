import os
import snowflake.connector
import base64
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/data")
def get_gpt_data():
    try:
        private_key_b64 = os.getenv("SF_PRIVATE_KEY_B64")
        if not private_key_b64:
            return JSONResponse(content={"error": "Private key not found"}, status_code=500)

        # Decode the base64 DER key (just ONCE!)
        private_key_der = base64.b64decode(private_key_b64)

        # Connect to Snowflake
        ctx = snowflake.connector.connect(
            user=os.getenv("SF_USER"),
            account=os.getenv("SF_ACCOUNT"),
            private_key=private_key_der,
            warehouse=os.getenv("SF_WAREHOUSE"),
            database=os.getenv("SF_DATABASE"),
            schema=os.getenv("SF_SCHEMA"),
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
