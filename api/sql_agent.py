import os
from databricks import sql
from groq import Groq
import dotenv

dotenv.load_dotenv()


SCHEMA_CONTEXT = """
Table: bts_flight_data.gold.fct_flights
Columns: flight_year, flight_month, fl_date, flight_number, tail_number,
carrier_sk, origin_airport_sk, dest_airport_sk, dep_delay, arr_delay,
air_time, distance, arr_delay_15plus

Table: bts_flight_data.gold.dim_airports
Columns: airport_sk, airport_code, airport_name, city, country, is_current

Table: bts_flight_data.gold.dim_carriers
Columns: carrier_sk, carrier_code, carrier_name, country, active, is_current

Table: bts_flight_data.gold.agg_carrier_monthly_performance
Columns: carrier_code, carrier_name, flight_year, flight_month,
total_flights, avg_dep_delay, avg_arr_delay, pct_delayed

Table: bts_flight_data.gold.agg_route_delay_trends
Columns: origin, dest, flight_year, flight_month, total_flights,
avg_arr_delay, avg_congestion_score
"""

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def generate_sql(user_question: str) -> str:
    prompt = f"""You are a SQL assistant for a Databricks warehouse.
Given the schema below, write ONE valid SELECT statement (Databricks SQL syntax)
that answers the user's question. Return ONLY the SQL, no explanation, no markdown fences.

Schema:
{SCHEMA_CONTEXT}

Question: {user_question}
"""
    response = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()


def validate_sql(query: str) -> bool:
    q = query.strip().lower()
    if not q.startswith("select"):
        return False
    forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "merge", "create"]
    return not any(word in q for word in forbidden)


def run_query(query: str, max_rows: int = 100):
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchmany(max_rows)
            columns = [desc[0] for desc in cursor.description]
            return columns, rows


def summarize_results(user_question: str, columns: list, rows: list) -> str:
    preview = str(rows[:10])
    prompt = f"""The user asked: "{user_question}"
Query returned these columns: {columns}
Sample rows: {preview}

Write a brief, natural-language answer (2-3 sentences) summarizing the result for the user."""
    response = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )
    return response.choices[0].message.content


def ask(user_question: str) -> dict:
    sql_query = generate_sql(user_question)

    if not validate_sql(sql_query):
        return {"error": "This question would require a non-SELECT operation, which isn't permitted.", "sql": sql_query}

    columns, rows = run_query(sql_query)
    summary = summarize_results(user_question, columns, rows)

    return {"sql": sql_query, "columns": columns, "rows": rows, "summary": summary}