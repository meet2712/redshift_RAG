from dotenv import load_dotenv
load_dotenv(override=True)
import os
from tabulate import tabulate
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql.base import PGDialect
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_experimental.sql import SQLDatabaseChain
from langchain.memory import ConversationBufferMemory
from langchain.agents import Tool, initialize_agent

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

# ─── Prevent SQLAlchemy from reflecting Redshift collations ───────────────
PGDialect.supports_collation = False

# ─── Manual schema definition ─────────────────────────────────────────────
transaction_table_info = '''
Table: transaction
Columns:
  hash (VARCHAR)
  timestamp (TIMESTAMP)
  from (VARCHAR)
  tx_type (VARCHAR)
  __hevo__marked_deleted (BOOLEAN)
'''

# ─── Patched SQLDatabase ──────────────────────────────────────────────────
class PatchedSQLDatabase(SQLDatabase):
    def get_table_info(self, table_names=None):
        return transaction_table_info

# ─── Redshift connection ──────────────────────────────────────────────────
redshift_uri = URL.create(
    drivername="redshift+psycopg2",
    username=os.getenv("redshift_user"),
    password=os.getenv("redshift_pass"),
    host=os.getenv("redshift_host"),
    port=5439,
    database="rskexplorer",
)
engine = create_engine(redshift_uri, echo=False)

# ─── Initialize patched DB ────────────────────────────────────────────────
db = PatchedSQLDatabase(
    engine=engine,
    schema="mainnet",
    include_tables=["transaction"],
    sample_rows_in_table_info=0,
    lazy_table_reflection=True,
)
with engine.begin() as conn:
    conn.execute(text("SET search_path TO mainnet;"))

# ─── Prelude definitions ──────────────────────────────────────────────────
preludes_dict = {
    "transaction": '''
        You are an SQL generator for Amazon Redshift.
        - Table: transaction
        - Filter: __hevo__marked_deleted = FALSE
        - When counting transactions, use COUNT(DISTINCT hash) as transactions
        - Yesterday: timestamp >= date_trunc('day', current_date - INTERVAL '1 day') AND timestamp < date_trunc('day', current_date)
        - Today: timestamp >= date_trunc('day', current_date)
        - Return raw SQL only, no joins
    ''',
    "active addresses": '''
        You are an SQL generator for Amazon Redshift.
        - Table: transaction
        - Count active addresses: COUNT(DISTINCT "from") as active_address
        - Return raw SQL only, no joins
    ''',
    "who is transacting right now": '''
        You are an SQL generator for Amazon Redshift.
        - Table: who_is_transacting
        - Fetch: date, transactions, address
        - Return raw SQL only
    ''',
    "transaction per partner": '''
        You are an SQL generator for Amazon Redshift.
        - Schema: mainnet
        - Table: transaction_per_partner
        - Fetch: transaction_per_partner, partner
        - Yesterday: date >= date_trunc('day', current_date - INTERVAL '1 day') AND date < date_trunc('day', current_date)
        - Return raw SQL only, no joins
    '''
}
default_prelude = 'You are an analyst writing SQL for Redshift. Use PostgreSQL syntax and no joins.'

# ─── Topic detection ──────────────────────────────────────────────────────
def detect_topics(user_input: str):
    low = user_input.lower()
    topics = []
    if "transaction per partner" in low or "transaction made by partner" in low or "transactions made by partner" in low:
        topics.append("transaction per partner")
    if "active address" in low or "active addresses" in low:
        topics.append("active addresses")
    if "who is transacting" in low or "transacting right now" in low or "Who is transacting right now" in low:
        topics.append("who is transacting right now")
    if ("transaction" in low or "transactions" in low or "how many transaction" in low) and not topics :
        topics.append("transaction")
    if topics:
        return topics
    # fallback LLM detection
    llm = ChatOpenAI(temperature=0, model_name="gpt-4o", openai_api_key=os.getenv("OPENAI_API_KEY"))
    prompt = (
        "Identify relevant topics: transaction, active addresses, who is transacting right now, transaction per partner."
        f" Question: {user_input}"
    )
    resp = llm.invoke(prompt).content.strip().lower()
    return [t.strip() for t in resp.split(',') if t.strip() in preludes_dict]

# ─── Dynamic prelude builder ─────────────────────────────────────────────
def get_dynamic_prelude(user_input: str) -> str:
    topics = detect_topics(user_input)
    return "\n".join(preludes_dict.get(t, default_prelude) for t in topics) or default_prelude

# ─── Execute SQL & fetch raw rows ─────────────────────────────────────────
def run_query_raw(user_input: str):
    prelude = get_dynamic_prelude(user_input) + "\nReturn raw SQL without markdown."
    template = PromptTemplate(
        input_variables=["table_info","input"],
        template=prelude + "\nSchema:\n{table_info}\nGenerate SQL for: {input}",
    )
    llm = ChatOpenAI(temperature=0, model_name="gpt-4o", openai_api_key=os.getenv("OPENAI_API_KEY"))
    chain = SQLDatabaseChain.from_llm(llm=llm, db=db, prompt=template, verbose=False, return_direct=True)
    sql_prompt = chain.llm_chain.prompt.format(table_info=db.get_table_info(), input=user_input)
    generated_sql = llm.invoke(sql_prompt).content.strip()
    with engine.connect() as conn:
        result = conn.execute(text(generated_sql))
        rows = result.fetchall()
        headers = list(result.keys())
    return rows, headers, generated_sql

# ─── Pretty table runner ─────────────────────────────────────────────────
def run_query_table(user_input: str) -> str:
    rows, headers, _ = run_query_raw(user_input)
    rows = rows[:100]
    return tabulate(rows, headers, tablefmt="psql")

# ─── Simple summary detector ──────────────────────────────────────────────
def summary_mode(user_input: str) -> bool:
    low = user_input.lower()
    return any(kw in low for kw in ["summary","how many","count","total","number of"])

# ─── Conversational RAG Agent (optional use) ──────────────────────────────
sql_tool = Tool(name="Redshift SQL", func=run_query_table,
                description="Executes SQL on Redshift, returns a psql-formatted table.")
memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
agent = initialize_agent(
    tools=[sql_tool],
    llm=ChatOpenAI(temperature=0, model_name="gpt-4o", openai_api_key=os.getenv("OPENAI_API_KEY")),
    agent="conversational-react-description",
    memory=memory,
    verbose=False,
    return_direct=True
)

# ─── Async DB Logger Setup ────────────────────────────────────────────────
executor = ThreadPoolExecutor(max_workers=2)
def log_query_to_db_async(question: str,
                           sql: str,
                           status: str,
                           row_count: int = 0,
                           error_message: str = None,
                           response: str = None):
    def _log():
        timestamp = datetime.utcnow()
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO etl_tables.rag_query_logs (timestamp, question, generated_sql, status, row_count, error_message, response)
                        VALUES (:timestamp, :question, :generated_sql, :status, :row_count, :error_message, :response)
                    """),
                    {
                        "timestamp": timestamp,
                        "question": question,
                        "generated_sql": sql,
                        "status": status,
                        "row_count": row_count,
                        "error_message": error_message,
                        "response": response
                    }
                )
        except Exception as e:
            print(f"[LogError] Failed to log query: {e}")
    executor.submit(_log)

# ─── Main REPL with dynamic output ─────────────────────────────────────────
if __name__ == '__main__':
    print("🤖 Redshift REPL started. Ask SQL or exit to quit.")
    while True:
        query = input("You: ").strip()
        if not query:
            continue
        if query.lower() in ("exit", "quit"):
            print("👋 Goodbye!")
            break
        try:
            rows, headers, generated_sql = run_query_raw(query)
            # determine output and response
            if summary_mode(query) and rows and len(rows[0]) == 1:
                count = rows[0][0]
                label = headers[0]
                answer = f"There were {count} {label}."
            else:
                table = tabulate(rows[:100], headers, tablefmt="psql")
                answer = table
            print(answer)
            log_query_to_db_async(query,
                                   generated_sql,
                                   status="success",
                                   row_count=len(rows),
                                   error_message=None,
                                   response=answer)
        except Exception as e:
            answer = f"Error: {e}"
            print(answer)
            log_query_to_db_async(query,
                                   generated_sql or "",
                                   status="error",
                                   error_message=str(e),
                                   response=answer)
