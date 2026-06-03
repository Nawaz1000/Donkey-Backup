import sys
import json
import urllib.request
import urllib.parse
import ssl
import time

def get_unique_key(solr_base_url, headers, context, collection):
    url = f"{solr_base_url}/solr/{collection}/schema/uniquekey?wt=json"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=context, timeout=10) as response:
            data = json.loads(response.read().decode())
            return data.get("uniqueKey", "id")
    except Exception:
        return "id"

def dump_solr(solr_base_url, collection, auth_header):
    headers = {"Authorization": auth_header} if auth_header else {}
    context = ssl._create_unverified_context()
    
    unique_key = get_unique_key(solr_base_url, headers, context, collection)
    cursor_mark = "*"
    
    total_dumped = 0
    
    while True:
        query_url = f"{solr_base_url}/solr/{collection}/select?q=*:*&rows=1000&wt=json&cursorMark={urllib.parse.quote(cursor_mark)}&sort={unique_key}+asc"
        req = urllib.request.Request(query_url, headers=headers)
        try:
            with urllib.request.urlopen(req, context=context, timeout=60) as response:
                data = json.loads(response.read().decode())
                docs = data.get("response", {}).get("docs", [])
                
                for doc in docs:
                    if "_version_" in doc:
                        del doc["_version_"]
                    # Write JSON lines to stdout
                    sys.stdout.buffer.write(json.dumps(doc).encode('utf-8') + b"\n")
                
                total_dumped += len(docs)
                if len(docs) > 0:
                    sys.stderr.write(f"Dumped {total_dumped} documents from '{collection}'...\n")
                    sys.stderr.flush()
                
                next_cursor = data.get("nextCursorMark")
                if not next_cursor or next_cursor == cursor_mark or not docs:
                    break
                cursor_mark = next_cursor
        except Exception as e:
            sys.stderr.write(f"Solr dump error: {str(e)}\n")
            sys.exit(1)

def restore_solr(solr_base_url, collection, auth_header):
    headers = {"Authorization": auth_header, "Content-Type": "application/json"} if auth_header else {"Content-Type": "application/json"}
    context = ssl._create_unverified_context()
    
    batch = []
    total_restored = 0
    
    def post_batch():
        nonlocal total_restored
        if not batch:
            return
        url = f"{solr_base_url}/solr/{collection}/update?commitWithin=10000"
        payload = json.dumps(batch).encode('utf-8')
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, context=context, timeout=120) as response:
                total_restored += len(batch)
                sys.stderr.write(f"Restored {total_restored} documents to '{collection}'...\n")
                sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(f"Solr restore batch error: {str(e)}\n")
            sys.exit(1)
        batch.clear()

    # Read JSON lines from stdin
    for line in sys.stdin.buffer:
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
            batch.append(doc)
            if len(batch) >= 500:
                post_batch()
        except Exception as e:
            sys.stderr.write(f"JSON parse error during restore: {str(e)}\n")
            
    post_batch()
    
    # Final Commit
    sys.stderr.write(f"Issuing final commit for '{collection}'...\n")
    sys.stderr.flush()
    commit_url = f"{solr_base_url}/solr/{collection}/update?commit=true"
    req = urllib.request.Request(commit_url, data=b"{}", headers=headers, method="POST")
    try:
        urllib.request.urlopen(req, context=context, timeout=60)
        sys.stderr.write(f"Final commit successful.\n")
        sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"Solr final commit error: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.exit("Usage: python solr_helper.py [dump|restore] <solr_base_url> <collection> [Auth Header]")
        
    action = sys.argv[1]
    solr_base_url = sys.argv[2].rstrip("/")
    collection = sys.argv[3]
    auth_header = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] != "None" else None

    if action == "dump":
        dump_solr(solr_base_url, collection, auth_header)
    elif action == "restore":
        restore_solr(solr_base_url, collection, auth_header)
    else:
        sys.exit(f"Unknown action: {action}")
