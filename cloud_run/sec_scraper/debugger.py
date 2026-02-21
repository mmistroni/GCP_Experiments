from google.cloud import bigquery
client = bigquery.Client()

# This is what your script uses:
PROJECT = client.project 
print(f"🕵️ Your script is currently targeting Project: {PROJECT}")

# Let's see if the table actually exists in the eyes of the API
table_id = f"{PROJECT}.gcp_shareloader.all_holdings_master"
try:
    table = client.get_table(table_id)
    print(f"📊 Table '{table_id}' exists and has {table.num_rows} rows.")
    print(f"📍 Region: {table.location}")
except Exception as e:
    print(f"❌ Table NOT FOUND: {e}")