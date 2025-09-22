# Synchronize OpenFoodFacts delta files to MongoDB (weekly): download, decompress, upsert.
# What this code does: sync France products, normalize 'code' as _id, ensure unique code index.

import os, gzip, json, requests, logging, subprocess, sys
from datetime import datetime
from pymongo import MongoClient


# Load env
# On GitHub Actions the secrets are injected as environment variables.

MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ["DB_NAME"]
PRODUCTS_COLLECTION = os.environ["PRODUCTS_COLLECTION"]
STATE_COLLECTION = os.environ.get("STATE_COLLECTION", "off_state")

# Mongo
db = MongoClient(MONGO_URI)[DB_NAME]
products, state = db[PRODUCTS_COLLECTION], db[STATE_COLLECTION]

# Logging
LOG_PATH = os.path.join(os.path.dirname(__file__), "sync_openfoodfacts.log")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_PATH)])
log = logging.getLogger("sync_openfoodfacts")

INDEX_URL = "https://static.openfoodfacts.org/data/delta/index.txt"

def get_index():
    r = requests.get(INDEX_URL, timeout=30); r.raise_for_status()
    return [l.strip() for l in r.text.splitlines() if l.strip()]

def last_processed():
    d = state.find_one({"_id": "last_processed"})
    return set(d.get("files", [])) if d else set()

def mark_processed(f):
    state.update_one({"_id": "last_processed"},
                     {"$push": {"files": f}, "$set": {"ts": datetime.utcnow()}},
                     upsert=True)

def is_france_product(d):
    tags = [str(c).lower() for c in (d.get("countries_tags") or [])]
    if "france" in tags:
        return True
    countries_str = str(d.get("countries") or "").lower()
    if "france" in [c.strip() for c in countries_str.split(",")]:
        return True
    return False

def ensure_unique_code_index():
    """Ensure there's a unique index on 'code'. Do not create if duplicates exist."""
    try:
        info = products.index_information()
        # find index name on 'code' if any
        code_idx_name = None
        code_is_unique = False
        for name, meta in info.items():
            keys = meta.get("key") or []
            if any(k[0] == "code" for k in keys):
                code_idx_name = name
                code_is_unique = meta.get("unique", False)
                break
        if code_is_unique:
            return
        # check duplicates quickly
        dup = products.aggregate([
            {"$group": {"_id": "$code", "c": {"$sum": 1}}},
            {"$match": {"c": {"$gt": 1}}},
            {"$limit": 1}
        ])
        if any(dup):
            log.error("duplicates found in 'code'. Resolve them before creating unique index.")
            return
        if code_idx_name:
            products.drop_index(code_idx_name)
        products.create_index("code", unique=True, sparse=True)
        log.info("created unique index on 'code'")
    except Exception as e:
        log.warning("ensure_unique_code_index failed: %s", e)

def process_doc(d):
    # normalize code, never modify _id or code in $set to avoid conflicts
    raw = d.get("code") or d.get("id") or d.get("_id")
    if not raw: return 0
    code = str(raw).strip()
    if not code: return 0
    d["_synced_at"] = datetime.utcnow()
    # prepare payload: do not set _id or code in $set
    payload = d.copy()
    payload.pop("_id", None)
    payload.pop("code", None)
    try:
        products.update_one(
            {"code": code},
            {"$set": payload, "$setOnInsert": {"_id": code, "code": code}},
            upsert=True
        )
        return 1
    except Exception:
        log.exception("upsert failed %s", code)
        return 0
    
def process_file(f):
    url = f"https://static.openfoodfacts.org/data/delta/{f}"
    log.info("processing %s", f)
    r = requests.get(url, timeout=120); r.raise_for_status()
    raw = gzip.decompress(r.content).decode("utf-8")
    try:
        docs = json.loads(raw) if raw.lstrip().startswith("[") else [json.loads(l) for l in raw.splitlines() if l.strip()]
    except Exception:
        log.exception("json parse failed %s", f)
        return 0
    count = 0
    for d in docs:
        if not is_france_product(d):
            continue
        count += process_doc(d)
    log.info("finished %s: %d docs", f, count)
    return count

def main():
    log.info("sync start")
    ensure_unique_code_index()
    try:
        files = sorted(get_index())
    except Exception:
        log.exception("fetch index failed")
        return
    processed = last_processed()
    new = [f for f in files if f not in processed]
    if not new:
        log.info("no new files")
        return
    total = 0
    for f in new:
        try:
            n = process_file(f)
            total += n
            if n: mark_processed(f)
        except Exception:
            log.exception("error processing %s", f)
    log.info("sync finished, total %d", total)

if __name__ == "__main__":
    main()
    subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "search_blob.py")], check=True)
