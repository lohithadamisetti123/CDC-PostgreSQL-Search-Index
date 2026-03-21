# CDC PostgreSQL → Meilisearch Search Index

This project implements an end‑to‑end Change Data Capture (CDC) pipeline:

- PostgreSQL as the source database (logical replication).
- Python consumer using logical decoding to capture changes.
- Meilisearch as the search index.
- Redis + Node/Express + Server‑Sent Events (SSE)–style endpoint to power a simple dashboard UI.

You can run the entire stack locally with Docker Compose and verify the behavior via curl and the browser UI.

---

## Architecture Overview

- **PostgreSQL (14)**  
  - Contains `products`, `categories`, and `inventory` tables.  
  - Logical replication enabled, with a publication on `products`.  

- **CDC Consumer (Python 3.11)**  
  - Connects via `LogicalReplicationConnection`.  
  - Parses `pgoutput` stream, builds product documents, and indexes into Meilisearch.  
  - Seeds the database with 10,000 fake products (plus inventory) on first run.  
  - Performs an initial bulk index of all products into Meilisearch.

- **Meilisearch**  
  - Hosts a `products` index with `id` as primary key.  
  - Searchable attributes: `name`, `description`, `category`.  
  - Filterable attributes: `category`, `in_stock`.  
  - Sortable attributes: `price`.

- **Redis**  
  - Available for pub/sub; currently not required for the minimal TICK‑based SSE demo.

- **API / Frontend (Node + Express)**  
  - Serves a small dashboard at `http://localhost:8000`.  
  - `/api/search` proxies queries to Meilisearch.  
  - `/api/cdc-stream` exposes a Server‑Sent Events stream used by the UI’s *Live CDC Feed*.

---

## Prerequisites

- Docker and Docker Compose installed.  
- Ports available:
  - `8000` – API / frontend  
  - `7700` – Meilisearch  
  - `5432` – PostgreSQL  
  - `6379` – Redis

---

## Getting Started

Clone the repository:

```bash
git clone https://github.com/lohithadamisetti123/cdc-postgresql-search-index.git
cd cdc-postgresql-search-index
```

Copy the example env file (if provided):

```bash
cp .env.example .env
```

Start the stack:

```bash
docker-compose down
docker-compose up -d
```

Check containers:

```bash
docker-compose ps
```

You should see all services `Up` and `healthy`:

- `cdc-postgres`
- `cdc-meilisearch`
- `cdc-redis`
- `cdc-consumer`
- `cdc-api-frontend`

---

## Verifying the Pipeline

### 1. Database seeded (10k+ products)

```bash
docker exec -it cdc-postgres \
  psql -U cdc_user -d cdc_db -c "SELECT count(*) FROM products;"
```

Expected: count ≥ `10000`.

---

### 2. Meilisearch index and documents

List indexes:

```bash
curl 'http://localhost:7700/indexes' \
  -H 'Authorization: Bearer master_key_placeholder'
```

You should see a `products` index.
List a few documents:

```bash
curl 'http://localhost:7700/indexes/products/documents?limit=3' \
  -H 'Authorization: Bearer master_key_placeholder'
```

Expected: 3 product documents and `total` ≈ `10000+`.

---

### 3. End‑to‑end CDC into Meilisearch

Insert a clearly identifiable product into Postgres:

```bash
docker exec -it cdc-postgres psql -U cdc_user -d cdc_db -c "
INSERT INTO products (name, description, price, category_id)
VALUES ('CDC Final UI Test 3', 'end to end check', 77.77, 1)
RETURNING product_id;
"
```

Search for it via the API:

```bash
curl "http://localhost:8000/api/search?q=CDC%20Final%20UI%20Test%203"
```

Expected: the product appears in `hits` with the correct name and price, proving CDC → Meilisearch → API works. 

---

### 4. Live CDC Stream (SSE)

The API exposes a Server‑Sent Events endpoint:

```bash
curl -N http://localhost:8000/api/cdc-stream
```

You should see a continuous stream of events like:

```text
event: cdc_event
data: {"table":"products","operation":"TICK","timestamp":"2026-03-20T05:58:32.973Z","counter":0}
```

A new `cdc_event` appears every 2 seconds. This is consumed by the frontend to drive the **Live CDC Feed** and live indicator. 

---

### 5. Web UI

Open the dashboard:

```text
http://localhost:8000
```

You should see:

- **Search** panel:
  - Input with `data-testid="search-input"`.
  - Filters for category and “In stock only”.
  - Results list with product name, category, price, and stock status.

- **Live CDC Feed** panel:
  - A running list of events under `data-testid="cdc-feed"`.
  - A live status dot with `data-testid="live-indicator"` that flashes as SSE events arrive.

Try typing `CDC Test Product` in the search box; previously inserted test products will appear in the results.

---

## Run & Stop

To view logs:

```bash
docker-compose logs -f
```

To stop everything:

```bash
docker-compose down
```

---

## Screenshots


### Dashboard – Search & Live CDC Feed
![Dashboard](screenshots/image1.png)

### SSE Stream from /api/cdc-stream (curl)
![SSE stream](screenshots/image2.png)

### Search API curl Output
![Search API curl](screenshots/image3.png)
![Search API curl](screenshots/image4.png)



---

## Demo Video


[Watch the demo video👉](https://youtu.be/t44zvZQsY6I?si=Jpxrh0XfqzpzhgAM)

---
## Notes & Trade‑offs

- The CDC consumer focuses on the `products` table and builds a denormalized document using joins to `categories` and `inventory`. 
- A one‑time bulk index is performed on startup to ensure Meilisearch has all existing rows before live CDC takes over.
- The SSE endpoint currently streams synthetic **TICK** events at 2‑second intervals to keep the UI live and demonstrate real‑time behavior; the underlying CDC → Meilisearch pipeline is tested via actual product inserts and search.
