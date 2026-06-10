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

def apply_schema(solr_base_url, collection, headers, context, backup_schema):
    sys.stderr.write("Applying source schema to target collection...\n")
    sys.stderr.flush()
    try:
        url = f"{solr_base_url}/solr/{collection}/schema?wt=json"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, context=context, timeout=30) as resp:
            target_data = json.loads(resp.read().decode())
            target_schema = target_data.get("schema", {})
        
        target_field_types = {ft["name"] for ft in target_schema.get("fieldTypes", [])}
        target_fields = {f["name"] for f in target_schema.get("fields", [])}
        target_dynamic = {f["name"] for f in target_schema.get("dynamicFields", [])}
        
        payload = {}
        
        add_field_types = [ft for ft in backup_schema.get("fieldTypes", []) if ft["name"] not in target_field_types]
        if add_field_types:
            payload["add-field-type"] = add_field_types
            
        add_fields = [f for f in backup_schema.get("fields", []) if f["name"] not in target_fields]
        if add_fields:
            payload["add-field"] = add_fields
            
        add_dynamic = [f for f in backup_schema.get("dynamicFields", []) if f["name"] not in target_dynamic]
        if add_dynamic:
            payload["add-dynamic-field"] = add_dynamic
            
        # Copy fields can just be added, errors will be suppressed if they exist
        add_copy = backup_schema.get("copyFields", [])
        if add_copy:
            payload["add-copy-field"] = add_copy

        if not payload:
            sys.stderr.write("Schema is already up-to-date.\n")
            return

        schema_update_url = f"{solr_base_url}/solr/{collection}/schema?wt=json"
        post_data = json.dumps(payload).encode('utf-8')
        req_update = urllib.request.Request(schema_update_url, data=post_data, headers=headers, method="POST")
        with urllib.request.urlopen(req_update, context=context, timeout=60) as resp_update:
            sys.stderr.write("Successfully applied source schema.\n")
            sys.stderr.flush()
            
    except Exception as e:
        sys.stderr.write(f"WARN: Failed to fully apply schema (some fields may already exist): {e}\n")
        sys.stderr.flush()

def dump_solr(solr_base_url, collection, auth_header):
    headers = {"Authorization": auth_header} if auth_header else {}
    context = ssl._create_unverified_context()
    
    unique_key = get_unique_key(solr_base_url, headers, context, collection)
    
    # 1. Back up Schema
    schema_url = f"{solr_base_url}/solr/{collection}/schema?wt=json"
    try:
        req = urllib.request.Request(schema_url, headers=headers)
        with urllib.request.urlopen(req, context=context, timeout=30) as response:
            schema_data = json.loads(response.read().decode())
            schema_payload = {"__backupvault_schema__": schema_data.get("schema", {})}
            sys.stdout.buffer.write(json.dumps(schema_payload).encode('utf-8') + b"\n")
            sys.stdout.buffer.flush()
    except Exception as e:
        sys.stderr.write(f"WARN: Failed to fetch Solr schema: {e}\n")
    
    cursor_mark = "*"
    total_dumped = 0
    
    while True:
        query_url = f"{solr_base_url}/solr/{collection}/select?q=*:*&rows=20000&wt=json&cursorMark={urllib.parse.quote(cursor_mark)}&sort={unique_key}+asc"
        req = urllib.request.Request(query_url, headers=headers)
        try:
            with urllib.request.urlopen(req, context=context, timeout=120) as response:
                data = json.loads(response.read().decode('utf-8'))
                docs = data.get("response", {}).get("docs", [])
                
                buffer_bytes = bytearray()
                for doc in docs:
                    doc.pop("_version_", None)
                    buffer_bytes.extend(json.dumps(doc).encode('utf-8'))
                    buffer_bytes.extend(b"\n")
                
                sys.stdout.buffer.write(buffer_bytes)
                
                total_dumped += len(docs)
                num_found = data.get("response", {}).get("numFound", 0)
                if len(docs) > 0:
                    pct = int((total_dumped / num_found) * 100) if num_found > 0 else 0
                    pct = min(100, max(0, pct))
                    sys.stderr.write(f"Dumped {total_dumped}/{num_found} documents from '{collection}'... ({pct}%)\n")
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
            if "__backupvault_schema__" in doc:
                apply_schema(solr_base_url, collection, headers, context, doc["__backupvault_schema__"])
                continue
                
            batch.append(doc)
            if len(batch) >= 5000:
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
