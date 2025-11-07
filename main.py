import os
import snowflake.connector
import base64
from datetime import date, datetime
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/data")
def get_gpt_data(
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0, ge=0)
):
    try:
        private_key_b64 = os.getenv("SF_PRIVATE_KEY_B64")
        if not private_key_b64:
            return JSONResponse(content={"error": "Private key not found"}, status_code=500)

        private_key_der = base64.b64decode(private_key_b64)

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
        #query= "SELECT * FROM gpt_innovation_forecast_analyst"
        query= "SELECT * FROM gpt_innovation_forecast_analyst LIMIT %s OFFSET %s"
        cs.execute(query, (limit, offset))
        columns = [col[0] for col in cs.description]
        rows = cs.fetchall()
        cs.close()
        ctx.close()

        # Serialize rows into JSON-safe objects
        def serialize_value(v):
            if isinstance(v, (date, datetime)):
                return v.isoformat()
            return v

        results = [dict((col, serialize_value(val)) for col, val in zip(columns, row)) for row in rows]

        return JSONResponse(content=results)

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
