from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import date, datetime
import snowflake.connector
import os
import base64

app = FastAPI()

@app.get("/data")
def get_gpt_data(
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0, ge=0),
    region: str = Query(default=None),
    collection: str = Query(default=None),
    sku: str = Query(default=None),
    product_category: str = Query(default=None)
):
    try:
        private_key_b64 = os.getenv("SF_PRIVATE_KEY_B64")
        if not private_key_b64:
            return JSONResponse(content={"error": "Missing private key"}, status_code=500)

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

        # Build SQL dynamically with bind parameters
        query = "SELECT * FROM gpt_innovation_forecast_analyst"
        conditions = []
        params = []

        if region:
            conditions.append("region = %s")
            params.append(region)
        if collection:
            conditions.append("collection = %s")
            params.append(collection)
        if sku:
            conditions.append("sku = %s")
            params.append(sku)
        if product_category:
            conditions.append("product_category = %s")
            params.append(product_category)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        cs.execute(query, params)
        columns = [col[0] for col in cs.description]
        rows = cs.fetchall()
        cs.close()
        ctx.close()

        def serialize_value(v):
            if isinstance(v, (date, datetime)):
                return v.isoformat()
            return v

        results = [
            {col: serialize_value(val) for col, val in zip(columns, row)}
            for row in rows
        ]

        return JSONResponse(content={
            "data": results,
            "nextOffset": offset + limit if len(rows) == limit else None
        })

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
