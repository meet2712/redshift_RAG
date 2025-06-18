# Redshift RAG

A lightweight, single-file Retrieval-Augmented Generation (RAG) CLI for Amazon Redshift.  
Ask natural-language questions, and behind the scenes it:

1. **Retrieves** the right prompt “preludes” (schema info, filters, date logic)  
2. **Generates** SQL via an LLM (GPT-4o)  
3. **Executes** that SQL on your Redshift cluster  
4. **Displays** results in a psql-style table  

---

## Prerequisites

- **Python 3.13** on your `PATH`  
- **Amazon Redshift** cluster endpoint & credentials  
- **OpenAI** API key  

---

## Files

- `rag.py`  
- `requirements.txt`  
- `.env-example`  

---

## Topics Supported (v1)

> Currently limited to 3–4 core topics; we’ll expand once the roadmap locks down:

- **transaction**  
- **active addresses**  
- **who is transacting right now**  
- **transaction per partner**

---

## Setup & Run

1. **Ensure** your directory has:
   ```bash
   rag.py
   requirements.txt
   .env-example
   ```

2. **Create & activate** a Python 3.13 venv:
   ```bash
   python3.13 -m venv .venv
   source .venv/bin/activate      # macOS/Linux
   .\.venv\Scripts\Activate    # Windows PowerShell
   ```

3. **Install** dependencies:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Configure** credentials:
   ```bash
   cp .env-example .env
   ```
   Edit `.env` and set:
   ```env
   REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
   REDSHIFT_PORT=5439
   REDSHIFT_DB=rskexplorer
   REDSHIFT_USER=your_username
   REDSHIFT_PASS=your_password
   OPENAI_API_KEY=your_openai_api_key
   ```

5. **Launch** the RAG shell:
   ```bash
   python rag.py
   ```

6. **Ask** natural-language questions:
   ```text
   Which partner transacted yesterday?
   How many transactions happened yesterday?
   How many active addresses were there last month?
   How many transactions did each partner make last week?
   Who is transacting right now?
   ```
   Type `exit` or `quit` to leave.

---

## Examples

```bash
$ python rag.py
🤖 Redshift RAG started. Ask SQL or \`exit\` to quit.
You: How many transactions happened yesterday?
+---------------+
| transactions  |
|---------------|
|          1,234|
+---------------+

You: exit
👋 Goodbye!
```

---

## Troubleshooting

- **“Module not found”** → ensure you installed all packages with `pip install -r requirements.txt`.  
- **Env vars not picked up** → check that `.env` is in the same folder as `rag.py`.  
- **Connection errors** → verify `REDSHIFT_HOST`, `REDSHIFT_PORT`, `REDSHIFT_DB` etc.  

---

## Contributing

1. Fork the repo  
2. Make changes in `rag.py`  
3. Submit a PR with your feature or fix  

---

## License

MIT © RootstockLabs
