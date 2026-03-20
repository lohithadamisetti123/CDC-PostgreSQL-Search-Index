import os
import struct
import time
import json
from datetime import datetime, timezone

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor, LogicalReplicationConnection
from psycopg2 import sql, errors

import requests
from faker import Faker
import redis

# Env
PG_HOST = os.getenv("CDC_PG_HOST", "postgres")
PG_PORT = int(os.getenv("CDC_PG_PORT", "5432"))
PG_USER = os.getenv("CDC_PG_USER", "cdc_user")
PG_PASSWORD = os.getenv("CDC_PG_PASSWORD", "cdc_password")
PG_DB = os.getenv("CDC_PG_DB", "cdc_db")
PUBLICATION_NAME = os.getenv("CDC_PUBLICATION_NAME", "my_publication")
REPLICATION_SLOT = os.getenv("CDC_REPLICATION_SLOT", "my_replication_slot")
LSN_CHECKPOINT_FILE = os.getenv("CDC_LSN_CHECKPOINT_FILE", "/data/lsn_checkpoint.txt")

MEILI_HOST = os.getenv("MEILI_HOST", "http://meilisearch:7700")
MEILI_INDEX_NAME = os.getenv("MEILI_INDEX_NAME", "products")
MEILI_MASTER_KEY = os.getenv("MEILI_MASTER_KEY", "master_key_placeholder")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "cdc_events")

fake = Faker()


def connect_pg():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def meili_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MEILI_MASTER_KEY}",
    }


def init_meili_index():
    idx_url = f"{MEILI_HOST}/indexes/{MEILI_INDEX_NAME}"
    r = requests.get(idx_url, headers=meili_headers())
    if r.status_code == 404:
        payload = {"uid": MEILI_INDEX_NAME, "primaryKey": "id"}
        requests.post(f"{MEILI_HOST}/indexes", headers=meili_headers(), data=json.dumps(payload))

    base = f"{MEILI_HOST}/indexes/{MEILI_INDEX_NAME}"
    requests.post(
        f"{base}/settings/searchable-attributes",
        headers=meili_headers(),
        data=json.dumps(["name", "description", "category"]),
    )
    requests.post(
        f"{base}/settings/filterable-attributes",
        headers=meili_headers(),
        data=json.dumps(["category", "in_stock"]),
    )
    requests.post(
        f"{base}/settings/sortable-attributes",
        headers=meili_headers(),
        data=json.dumps(["price"]),
    )


def ensure_replication_slot(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            sql.SQL("SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput');"),
            (REPLICATION_SLOT,),
        )
    except errors.DuplicateObject:
        pass
    finally:
        cur.close()


def seed_database_if_empty(conn):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT COUNT(*) FROM products;")
        count = cur.fetchone()[0]
        if count >= 5000:
            print(f"DB already seeded with {count} products")
            return

        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
        category_ids = []
        for name in categories:
            cur.execute(
                "INSERT INTO categories (name) VALUES (%s) RETURNING category_id;",
                (name,),
            )
            category_ids.append(cur.fetchone()[0])

        total = 10000
        for _ in range(total):
            name = fake.unique.sentence(nb_words=3)[:250]
            description = fake.text(max_nb_chars=200)
            price = round(
                fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=5, max_value=500),
                2,
            )
            category_id = fake.random_element(elements=category_ids)
            cur.execute(
                """
                INSERT INTO products (name, description, price, category_id)
                VALUES (%s, %s, %s, %s)
                RETURNING product_id;
                """,
                (name, description, price, category_id),
            )
            product_id = cur.fetchone()[0]
            quantity = fake.random_int(min=0, max=100)
            cur.execute(
                """
                INSERT INTO inventory (product_id, quantity)
                VALUES (%s, %s);
                """,
                (product_id, quantity),
            )
        conn.commit()
        print("Seeded database with test data")


def build_product_document(conn, product_id):
    with conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT p.product_id, p.name, p.description, p.price,
                   c.name AS category,
                   COALESCE(i.quantity, 0) AS quantity
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.category_id
            LEFT JOIN inventory i ON p.product_id = i.product_id
            WHERE p.product_id = %s;
            """,
            (product_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row["product_id"],
            "name": row["name"],
            "description": row["description"],
            "price": float(row["price"]),
            "category": row["category"],
            "quantity": row["quantity"],
            "in_stock": row["quantity"] > 0,
        }


def index_document(doc):
    url = f"{MEILI_HOST}/indexes/{MEILI_INDEX_NAME}/documents"
    requests.post(url, headers=meili_headers(), data=json.dumps([doc]))


def index_document_batch(docs):
    if not docs:
        return
    url = f"{MEILI_HOST}/indexes/{MEILI_INDEX_NAME}/documents"
    requests.post(url, headers=meili_headers(), data=json.dumps(docs))


def delete_document(product_id):
    url = f"{MEILI_HOST}/indexes/{MEILI_INDEX_NAME}/documents/{product_id}"
    requests.delete(url, headers=meili_headers())


def bulk_index_all_products(conn):
    print("Starting bulk indexing of all products to Meilisearch...")
    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM products;")
        ids = [row[0] for row in cur.fetchall()]

    batch = []
    for pid in ids:
        doc = build_product_document(conn, pid)
        if doc:
            batch.append(doc)
        if len(batch) >= 1000:
            index_document_batch(batch)
            batch = []
    if batch:
        index_document_batch(batch)
    print(f"Bulk indexed {len(ids)} products into Meilisearch")


def load_lsn_checkpoint():
    if not os.path.exists(LSN_CHECKPOINT_FILE):
        return None
    with open(LSN_CHECKPOINT_FILE, "r") as f:
        value = f.read().strip()
        return value or None


def save_lsn_checkpoint(lsn_text):
    with open(LSN_CHECKPOINT_FILE, "w") as f:
        f.write(lsn_text)


def int_to_lsn(xlogpos):
    high = xlogpos >> 32
    low = xlogpos & 0xFFFFFFFF
    return f"{high:X}/{low:X}"


def parse_pgoutput_message(payload, relation_map):
    if not payload:
        return None, None

    buf = memoryview(payload)
    msg_type = chr(buf[0])
    idx = 1

    if msg_type == "R":
        relid = struct.unpack_from("!I", buf, idx)[0]
        idx += 4

        schema_end = payload.find(b"\x00", idx)
        nspname = buf[idx:schema_end].tobytes().decode("utf-8")
        idx = schema_end + 1

        name_end = payload.find(b"\x00", idx)
        relname = buf[idx:name_end].tobytes().decode("utf-8")
        idx = name_end + 1

        _replident = chr(buf[idx])
        idx += 1

        ncols = struct.unpack_from("!H", buf, idx)[0]
        idx += 2

        colnames = []
        for _ in range(ncols):
            _flags = buf[idx]
            idx += 1
            cname_end = payload.find(b"\x00", idx)
            cname = buf[idx:cname_end].tobytes().decode("utf-8")
            idx = cname_end + 1
            _type_oid = struct.unpack_from("!I", buf, idx)[0]
            idx += 4
            _type_mod = struct.unpack_from("!I", buf, idx)[0]
            idx += 4
            colnames.append(cname)

        relation_map[relid] = {
            "schema": nspname,
            "table": relname,
            "columns": colnames,
        }
        return None, None

    if msg_type == "I":
        relid = struct.unpack_from("!I", buf, idx)[0]
        idx += 4
        if relid not in relation_map:
            return None, None
        rel = relation_map[relid]
        if rel["table"] != "products":
            return None, None

        if chr(buf[idx]) == "N":
            idx += 1
            cols = rel["columns"]
            ncols = len(cols)
            product_id = None
            for i in range(ncols):
                kind = chr(buf[idx])
                idx += 1
                if kind in ("n", "u"):
                    continue
                if kind == "t":
                    val_len = struct.unpack_from("!I", buf, idx)[0]
                    idx += 4
                    val_bytes = buf[idx:idx + val_len].tobytes()
                    idx += val_len
                    colname = cols[i]
                    if colname == "product_id":
                        product_id = int(val_bytes.decode("utf-8"))
                else:
                    return None, None
            if product_id is not None:
                return "INSERT", product_id
        return None, None

    if msg_type == "U":
        relid = struct.unpack_from("!I", buf, idx)[0]
        idx += 4
        if relid not in relation_map:
            return None, None
        rel = relation_map[relid]
        if rel["table"] != "products":
            return None, None

        cols = rel["columns"]
        product_id = None

        while idx < len(buf):
            tag = chr(buf[idx])
            idx += 1
            if tag in ("K", "O", "N"):
                for i in range(len(cols)):
                    kind = chr(buf[idx])
                    idx += 1
                    if kind in ("n", "u"):
                        continue
                    if kind == "t":
                        val_len = struct.unpack_from("!I", buf, idx)[0]
                        idx += 4
                        val_bytes = buf[idx:idx + val_len].tobytes()
                        idx += val_len
                        colname = cols[i]
                        if colname == "product_id":
                            product_id = int(val_bytes.decode("utf-8"))
            else:
                break

        if product_id is not None:
            return "UPDATE", product_id
        return None, None

    if msg_type == "D":
        relid = struct.unpack_from("!I", buf, idx)[0]
        idx += 4
        if relid not in relation_map:
            return None, None
        rel = relation_map[relid]
        if rel["table"] != "products":
            return None, None

        cols = rel["columns"]
        product_id = None

        tag = chr(buf[idx])
        idx += 1
        if tag in ("K", "O"):
            for i in range(len(cols)):
                kind = chr(buf[idx])
                idx += 1
                if kind in ("n", "u"):
                    continue
                if kind == "t":
                    val_len = struct.unpack_from("!I", buf, idx)[0]
                    idx += 4
                    val_bytes = buf[idx:idx + val_len].tobytes()
                    idx += val_len
                    colname = cols[i]
                    if colname == "product_id":
                        product_id = int(val_bytes.decode("utf-8"))

        if product_id is not None:
            return "DELETE", product_id

    return None, None


def logical_replication():
    query_conn = connect_pg()
    repl_conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB,
        connection_factory=LogicalReplicationConnection,
    )
    cur = repl_conn.cursor()

    options = {
        "proto_version": "1",
        "publication_names": PUBLICATION_NAME,
    }

    start_lsn = load_lsn_checkpoint()
    kwargs = {"slot_name": REPLICATION_SLOT, "options": options}
    if start_lsn:
        kwargs["start_lsn"] = start_lsn

    cur.start_replication(**kwargs)

    rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    relation_map = {}

    def consume(msg):
        payload = msg.payload
        op, product_id = parse_pgoutput_message(payload, relation_map)

        if op == "INSERT":
            doc = build_product_document(query_conn, product_id)
            if doc:
                index_document(doc)
                ts = datetime.now(timezone.utc).isoformat()
                rds.publish(
                    REDIS_CHANNEL,
                    json.dumps({"table": "products", "operation": "INSERT", "timestamp": ts}),
                )
        elif op == "UPDATE":
            doc = build_product_document(query_conn, product_id)
            if doc:
                index_document(doc)
                ts = datetime.now(timezone.utc).isoformat()
                rds.publish(
                    REDIS_CHANNEL,
                    json.dumps({"table": "products", "operation": "UPDATE", "timestamp": ts}),
                )
        elif op == "DELETE":
            delete_document(product_id)
            ts = datetime.now(timezone.utc).isoformat()
            rds.publish(
                REDIS_CHANNEL,
                json.dumps({"table": "products", "operation": "DELETE", "timestamp": ts}),
            )

        lsn_text = int_to_lsn(msg.data_start)
        save_lsn_checkpoint(lsn_text)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    print("Starting logical replication consume loop...")
    cur.consume_stream(consume)


def main():
    while True:
        try:
            conn = connect_pg()
            conn.close()
            break
        except Exception:
            time.sleep(2)

    while True:
        try:
            r = requests.get(f"{MEILI_HOST}/health")
            if r.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(2)

    init_meili_index()

    conn = connect_pg()
    seed_database_if_empty(conn)
    ensure_replication_slot(conn)

    # Bulk index all current products to Meilisearch (ensures non-empty index)
    bulk_index_all_products(conn)

    conn.close()

    with open("/tmp/cdc_ready", "w") as f:
        f.write("ready")

    logical_replication()


if __name__ == "__main__":
    main()
