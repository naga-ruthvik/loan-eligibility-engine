# Loan Eligibility Engine 
The Loan Eligibility Engine accepts a CSV file containing users, saves the users in PostgreSQL, matches them against active loan products, and sends email notifications for eligible matches.

**Stack:** Python 3.12 · PostgreSQL (SQLAlchemy / Psycopg) · AWS (Lambda, S3, SQS, API Gateway) · n8n (workflow automation) · Amazon SES · Firecrawl + Google Gemini (product discovery & matching)

---

## Contents

1. [Architecture at a glance](#1-architecture-at-a-glance)
2. [n8n Workflows in detail](#2-n8n-workflows-in-detail)
3. [How the pipeline actually works](#3-how-the-pipeline-actually-works)
4. [serverless.yml overview](#4-serverlessyml-overview)
5. [Deployed environment](#5-deployed-environment)
6. [Project structure](#6-project-structure)
7. [Database and migrations](#7-database-and-migrations)
8. [Local setup](#8-local-setup)
9. [Run the frontend locally](#9-run-the-frontend-locally)
10. [Deploy to AWS](#10-deploy-to-aws)
11. [Error handling and retries](#11-error-handling-and-retries)
12. [Current prototype limits](#12-current-prototype-limits)
13. [Useful commands](#13-useful-commands)

---

## 1. Architecture at a glance

The application is a small serverless pipeline:

1. A browser asks for a temporary upload URL.
2. The browser uploads the CSV directly to Amazon S3.
3. S3 sends an event to Amazon SQS.
4. AWS Lambda reads the CSV in small batches and saves valid rows to PostgreSQL.
5. When the upload is complete, a Lambda (`triggerMatching`) calls the matching n8n workflow (Workflow B).
6. After matching succeeds, the **same Lambda** calls the notification n8n workflow (Workflow C).

![Architecture Diagram](docs/architecture.png)

## 2. n8n Workflows in detail

### Workflow A — Loan Product Ingestion & Extraction

Scheduled, independent of the other two workflows — keeps the loan product catalog fresh.

![Workflow A Diagram](docs/workflow_a.png)

| Step | Node | What it does |
|---|---|---|
| 1 | **Daily 6AM Trigger** | Starts the run once a day. |
| 2 | **Source List** | A fixed `Set` node listing the pages to crawl (currently a Paisabazaar business-loans page and a BankBazaar personal-loans page). |
| 3 | **Split URLs → Loop URLs** | Turns the list into one item per URL and processes them one at a time. |
| 4 | **Firecrawl Scrape** | Calls the Firecrawl v2 API, returns the page as markdown. On failure, `Log Scrape Error` writes a row to `crawl_errors` and the loop continues. |
| 5 | **LLM Extract Loan Data** | Sends the markdown to Google Gemini with a structured output parser (`Loan Data Schema`) that asks for every distinct loan product on the page — lender, product, category, min income, credit score, age range, interest rate range, other eligibility details, active flag. General rules naming no specific lender (e.g. "most lenders require ₹25k") are stored under lender name `General`/`Multiple Lenders`. Failures log via `Log Extraction Error`. |
| 6 | **Validate Fields** (Code) | Fills a missing lender/product name from the other field, then checks: interest rates ≥ 1 and min ≤ max; credit score between 300–900; income and ages non-negative; min age ≤ max age. Tags each record `is_valid` + an error message if not. |
| 7 | **Is Valid?** | Valid → `Upsert to Postgres`, inserting into `loan_products` and updating on conflict of `(lender_name, product_name)` — refreshes rates, eligibility details, `last_verified_at`, `updated_at`. Invalid → `Log Validation Error` → `crawl_errors`. |
| 8 | **Build Summary** | Runs once all URLs are processed. Counts upserted products and errors across the three log nodes and outputs a one-line summary — this message isn't sent anywhere (no Slack/email hook), so it's only visible in the n8n execution log. |

**Worth knowing:** the upsert key is `(lender_name, product_name)` — two different source pages producing the same pair will overwrite each other, which is fine for this catalog but worth knowing if sources start to collide.

### Workflow B — User–Loan Matching Engine

The core technical workflow. Triggered by `triggerMatching` via the `Match Users Webhook`, with `{ "job_id": "..." }`.

![Workflow B Diagram](docs/workflow_b.png)

| Step | Node | What it does |
|---|---|---|
| 1 | **Match Users Webhook** | Accepts the POST, requires `job_id` in the body. |
| 2 | **Fetch New Users** | `SELECT … FROM users WHERE ingestion_job_id = $1` — pulls id, source id, email, income, credit score, employment status, age for just this job. |
| 3 | **Fetch Active Products** | Selects all `loan_products` where `is_active = true`. *(Unused downstream — Stage 1 re-queries products itself, so this is a harmless redundant query.)* |
| 4 | **Stage 1 — SQL Prefilter (hard filter)** | Cross-joins this job's users against active products, keeping only pairs where income/credit score/age fall inside the product's numeric limits (a null limit = no constraint). Users missing income, credit score, or age are dropped rather than guessed at. |
| 5 | **Any Candidates? (IF)** | No rows → `No Candidates – Log` (NoOp, run ends — no wasted downstream work). Yes → Stage 2. |
| 6 | **Stage 2 — Soft Filter (Code)** | Applies each product's `eligibility_criteria` JSON: allowed/excluded employment statuses, max interest-rate tolerance. Computes a **fit score** (45% income headroom, 40% credit headroom, 15% age proximity to the product's age-range midpoint) and keeps only the **top 5 products per user** — this is what bounds LLM cost regardless of catalog size. |
| 7 | **Batch for LLM** (Split In Batches, size 20) | Loops over surviving pairs 20 at a time. The "done" output is empty — the workflow ends when the loop finishes. |
| 8 | **Aggregate Batch Pairs** | Merges 20 items into one payload with a `pairs` array (ids, user attributes, product name, lender, criteria, fit score). |
| 9 | **LLM Eligibility Check** | Google Gemini (`models/gemini-3.6-flash` — unusual model string, verify it's still valid for your credential). Prompt is the JSON of pairs; system message restricts judgment to qualitative/ambiguous criteria only (e.g. "stable income preferred"). A structured output parser enforces: `eligibility_status` (`likely_eligible` / `not_eligible` / `needs_review`), `match_score` (0–100), `match_reasons.notes`. |
| 10 | **Upsert Matches** | Filters out malformed results, inserts the rest via `jsonb_to_recordset` into `matches`. On conflict `(user_id, loan_product_id)`, updates status/score/reasons/`evaluated_at` — **reruns overwrite, never duplicate.** |
| 11 | **Loop back** | `Upsert Matches` → `Batch for LLM` for the next 20 pairs, until all batches are processed. |

**What this guarantees:** no duplicate match rows on a rerun. **What it doesn't guarantee:** exactly-once LLM evaluation, or that two runs produce identical scores. If one batch of 20 fails at the LLM step, the loop stops and the remaining batches in that run are never processed — there's no retry at the batch level yet.

### Workflow C — Notification & Email Dispatch

Prototype, unpublished. Triggered the same way as Workflow B — a webhook with `{ "job_id": "..." }` — called by `triggerMatching` only after Workflow B returns success.

![Workflow C Diagram](docs/workflow_c.png)

| Step | Node | What it does |
|---|---|---|
| 1 | **Entry** | `Webhook` (POST, `job_id` in body) or `Start from Workflow B` (an Execute Workflow trigger) — both land on `Initialize Job`. |
| 2 | **Initialize Job** | Sets `job_id`, an empty `last_user_id` pagination cursor, `page_size = 10`, `sent_count`/`failed_count = 0`, the test Gmail username, the SES sender address, and a 1-second delay between sends. |
| 3 | **Get User Page** (Postgres) | Joins `users` + `matches` + `loan_products`, keeping only `likely_eligible` or `needs_review` matches. Uses keyset pagination (`id > last_user_id`, ordered by `id`) and `JSON_AGG`s each user's matched products. |
| 4 | **Prepare Page** (Code) | Builds one email per user: HTML + plain-text body listing each lender/product/score/reason, ending with a prototype disclaimer. **Every email is routed to one of two test Gmail plus-aliases** (not the user's real address) — this is test routing, not a bug. Carries the pagination cursor and running counters forward. |
Download, so no real user gets emailed from this build. |
| 5 | **Page Has Users?** | Users → `Loop Over Users`. Empty → `Final Summary`. |
| 6 | **Loop Over Users** (Split In Batches, size 1) | Per user: `Recipient Allowed?` → allowed sends via SES and records sent/failed based on the SES response; not allowed records a failure with "Recipient is not an allowed test address." Both paths wait `delay_seconds` (SES rate-limit buffer) before looping to the next user. |
| 7 | **Update State / More Pages?** | Rolls the page's sent/failed counts into the running totals. If the page was full (`page_count >= page_size`), loops back to `Get User Page` with the new cursor; otherwise moves to `Final Summary`. |
| 8 | **Final Summary** | Returns `job_id`, `status` (`completed` or `completed_with_errors`), `sent_count`, `failed_count`. |

**Worth knowing:** unpublished workflows only fire in test mode. Going live means replacing the alias logic in `Prepare Page` with real user emails and revisiting `Recipient Allowed?`.

---

## 3. How the pipeline actually works

### CSV ingestion, step by step

1. **Request an upload URL** — `POST /ingestions/upload-url`. `createUploadUrl` creates a `job_id`, an S3 key (`incoming/{job_id}/users.csv`), an `ingestion_jobs` row, and a presigned S3 URL. The browser never receives AWS credentials.
2. **Direct S3 upload** — the browser `PUT`s the CSV straight to S3, bypassing API Gateway/Lambda.
3. **S3 → SQS** — an object-created event lands on the ingestion queue, decoupling upload from processing.
4. **Chunked ingestion** — `ingestUsers` reads a bounded number of rows (`INGESTION_BATCH_ROWS=1000` by default), saves a cursor (byte offset, row number, chunk number, headers), and re-queues a continuation message if more rows remain. Chunks for one job run in order; different jobs can run in parallel.
5. **Minimal validation** — only `user_id` is required; numeric fields must be valid, non-negative when present. Invalid rows go to `crawl_errors`; valid rows in the same batch still get processed.
6. **Safe inserts** — a unique constraint on `(ingestion_job_id, source_user_id)` prevents duplicate inserts on a repeated S3/SQS event. On the final chunk, the job is marked `completed` and a durable `webhook_deliveries` record is created with the `job_id`.

### `triggerMatching` — what the code actually does

`triggerMatching` consumes one message at a time off the matching SQS queue (`batchSize: 1`) and runs `process_delivery(delivery_id)`:

1. **Claim the delivery** — loads the `WebhookDelivery` row with `SELECT … FOR UPDATE` (a DB row lock). If it's already `sent`, exits immediately (idempotent no-op). If it's `sending` and still within its lock window (`locked_until` in the future), it also exits — this is what protects against a duplicate SQS delivery triggering two concurrent webhook calls for the same job. Otherwise it sets status `sending`, bumps `attempt_count`, and sets `locked_until = now + N8N_WEBHOOK_LOCK_SECONDS` (600s default).
2. **Call Workflow B** — POSTs the small `{ "job_id": ... }` payload to `N8N_WEBHOOK_URL` with a timeout of `N8N_WEBHOOK_TIMEOUT_SECONDS` (10s default). Any non-2xx response, timeout, or connection error marks the delivery `failed` (with the error and HTTP status saved) and raises — which surfaces as an SQS batch item failure, so SQS retries the whole message.
3. **Call Workflow C** — only reached if step 2 returned 2xx. Same POST, same payload, same timeout, against `N8N_WORKFLOW_C_WEBHOOK_URL`.
4. **Mark sent** — only once *both* calls succeed: the delivery row is reloaded under lock, set to `sent`, `delivered_at` is stamped, `locked_until`/`last_error` are cleared, and `last_http_status` records Workflow C's status code.

**A real implementation detail worth knowing:** there's one status field for the *whole* delivery, not one per webhook. If Workflow B succeeds but Workflow C fails, the delivery is marked `failed` as a whole — and on SQS retry, `process_delivery` runs from the top again, which means **Workflow B's webhook gets called a second time**, not just Workflow C's. This is exactly the prototype limitation already called out in §12 ("review Workflow B idempotency before production use") — Workflow B's own upsert-on-conflict behavior is what keeps this safe today, not anything in `triggerMatching` itself.

---

## 4. `serverless.yml` overview

### Functions

| Function | Trigger | Timeout | Role can do |
|---|---|---|---|
| `createUploadUrl` | `POST /ingestions/upload-url` (HTTP API) | 29s (provider default — 1s under API Gateway's 30s hard cap) | `s3:PutObject` on the upload bucket only. |
| `ingestUsers` | `IngestionQueue` SQS (`batchSize: 1`, partial-batch failure reporting) | 900s (15 min — needed to chew through large CSV chunks) | Read the uploaded object from S3; receive/delete/inspect `IngestionQueue`; send messages to both `IngestionQueue` (chunk continuation) and `MatchingQueue` (handoff on completion). |
| `triggerMatching` | `MatchingQueue` SQS (`batchSize: 1`, partial-batch failure reporting) | 30s | Receive/delete/inspect `MatchingQueue` only — no S3 access, no direct DB IAM (DB access goes through `DATABASE_URL`, not IAM). |

All three share one dependency layer and run on Python 3.12 / x86_64 in `ap-southeast-2`.

### SQS queues

| Queue | Visibility timeout | Why | Backed by |
|---|---|---|---|
| `IngestionQueue` | 5400s (90 min) | Generous headroom over `ingestUsers`'s 15-minute Lambda timeout, so SQS doesn't make an in-flight message visible to another consumer mid-processing. | `IngestionDLQ`, after 5 failed receives, 14-day retention |
| `MatchingQueue` | 180s | Sized against `triggerMatching`'s short 30s timeout with room for the 2xx wait on both webhooks. | `MatchingDLQ`, after 5 failed receives, 14-day retention |

An `IngestionQueuePolicy` scopes `sqs:SendMessage` on `IngestionQueue` to the S3 service principal *and* this specific AWS account, so only this account's bucket can publish events into it. Both queues use SQS-managed server-side encryption.

### Other resources

- **`UploadBucket` (S3)** — AES256 encryption, versioning enabled, all public access blocked, CORS allows `PUT`/`GET`/`HEAD` from any origin (open for the prototype frontend — see §12), incomplete multipart uploads auto-abort after 7 days, and an event notification wires `s3:ObjectCreated:*` under `incoming/*.csv` straight to `IngestionQueue`.
- **IAM roles** — three separate roles (`UploadLambdaRole`, `IngestionLambdaRole`, `MatchingLambdaRole`), each scoped to only what its function needs, all attaching `AWSLambdaBasicExecutionRole` for CloudWatch logging.
- **Outputs** — `UploadBucketName`, `IngestionQueueUrl`, `MatchingQueueUrl` are exposed post-deploy for reference/debugging.
- **Packaging** — `.env` and `venv/` are explicitly excluded from the deployment package, so local secrets never ship.

---

## 5. Deployed environment

```text
service:  loan-eligibility-engine
stage:    dev
region:   ap-southeast-2
stack:    loan-eligibility-engine-dev

endpoint:
  POST  https://1ptrorx4i9.execute-api.ap-southeast-2.amazonaws.com/ingestions/upload-url

functions:
  createUploadUrl    → loan-eligibility-engine-dev-createUploadUrl
  ingestUsers        → loan-eligibility-engine-dev-ingestUsers
  triggerMatching    → loan-eligibility-engine-dev-triggerMatching
```

---

## 6. Project structure

```text
loan-eligibility-engine/
├── serverless.yml              AWS, Lambda, S3, SQS, IAM, and environment setup
├── handler.py                  Small example health endpoint
├── pipeline/
│   ├── config.py                Pipeline environment settings
│   ├── upload.py                Upload URL API Lambda
│   ├── csv_ingestion.py         Streaming CSV parser and minimal validation
│   ├── ingestion.py             S3/SQS ingestion worker
│   └── matching.py              Workflow B then Workflow C delivery worker
├── database/
│   ├── config.py                Database URL and connection settings
│   ├── models.py                SQLAlchemy database models
│   └── session.py                Shared engine and transaction sessions
├── alembic/
│   ├── env.py                   Alembic database configuration
│   └── versions/                Database migration files
├── frontend/
│   ├── index.html               CSV upload page
│   ├── app.js                   Requests the URL and uploads to S3
│   └── styles.css               Upload page styling
├── tests/
│   ├── test_csv_ingestion.py       CSV parsing and validation tests
│   ├── test_pipeline_messages.py   S3/SQS message parsing tests
│   └── test_matching_webhooks.py   Workflow B -> Workflow C ordering test
├── docs/
│   └── pipeline-flow.md         Detailed pipeline and scalability notes
├── requirements.txt              Runtime dependency list
├── requirements.lock             Locked runtime dependencies
├── requirements-dev.txt           Runtime plus development dependencies
├── requirements-dev.lock          Locked development dependencies
├── .env.example                  Safe environment-variable template
└── README.md                     This document
```

**Key files:**

| File | Purpose |
|---|---|
| `pipeline/upload.py` | `create_upload_url` handler — creates the job, S3 key, presigned URL. |
| `pipeline/csv_ingestion.py` | Streaming parser returning a `CsvChunk` (accepted rows, rejected row errors, next byte offset, next row number, more-data flag, headers). No separate `ParsedUserRow` type — kept simple on purpose. |
| `pipeline/ingestion.py` | Parses S3/continuation messages, claims chunks, reads byte ranges from S3, writes users/errors to PostgreSQL, updates job progress, creates continuation messages, creates the durable webhook delivery on completion. |
| `pipeline/matching.py` | Processes the matching SQS message; protects the webhook delivery from duplicate processing; calls Workflow B, then Workflow C after B succeeds; records the result; lets SQS retry on failure. (See §3 for the full walkthrough.) |
| `database/models.py` | Schema: `ingestion_jobs`, `users`, `loan_products`, `matches`, `notifications`, `crawl_errors`, `ingestion_chunks`, `webhook_deliveries`. |
| `database/session.py` | One SQLAlchemy engine per warm process; short-lived, transaction-scoped sessions. |
| `alembic/versions/` | DB migrations — run as a deployment step, never during a Lambda cold start or user request. |
| `frontend/` | Minimal browser page: requests the upload URL, then uploads directly to S3. |
| `tests/` | Offline tests for the parser, message handling, and the Workflow B→C call order. No AWS/n8n/SES calls. |

---

## 7. Database and migrations

PostgreSQL via SQLAlchemy + Psycopg. Both of these `DATABASE_URL` forms work (the app normalizes the first to the explicit Psycopg driver):

```text
postgresql://user:password@host:5432/database
postgresql+psycopg://user:password@host:5432/database
```

```bash
alembic upgrade head     # apply migrations
alembic current           # see current migration
```

Safety guarantees baked into the schema:
- Duplicate users for one job are ignored and recorded as rejected.
- A completed chunk is not processed again.
- A webhook delivery has one durable record per job and event type.
- Parent deletion cascades to related users, chunks, matches, and errors.

---

## 8. Local setup

**Requirements:** Python 3.12 · Node.js + Serverless Framework · a PostgreSQL database · AWS credentials · n8n Workflow B and Workflow C webhook URLs.

**Create a virtual environment**

Windows (PowerShell):
```powershell
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.lock
```

Linux/macOS:
```bash
python3.12 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.lock
```

**Configure environment variables**

```bash
cp .env.example .env          # Windows: Copy-Item .env.example .env
```

Minimum required values in `.env`:

| Variable | Example | Required? |
|---|---|---|
| `DATABASE_URL` | `postgresql+psycopg://username:password@localhost:5432/loan_eligibility` | Yes |
| `N8N_WEBHOOK_URL` | `https://your-n8n-host/webhook/match-users` | Yes |
| `N8N_WORKFLOW_C_WEBHOOK_URL` | `https://your-n8n-host/webhook/workflow-c` | Yes |
| `PRESIGNED_URL_EXPIRES_SECONDS` | `900` | No — has a default |
| `INGESTION_BATCH_ROWS` | `1000` | No — has a default |
| `N8N_WEBHOOK_TIMEOUT_SECONDS` | `10` | No — has a default |
| `N8N_WEBHOOK_LOCK_SECONDS` | `600` | No — has a default |

**Prepare the database**
```bash
alembic upgrade head
```

**Run the tests**
```bash
python -m pytest -q
# Windows, if venv isn't activated:
.\venv\Scripts\python.exe -m pytest -q
```
Tests run offline — no AWS or n8n required.

---

## 9. Run the frontend locally

```bash
python -m http.server 8080 --directory frontend
```

Open `http://localhost:8080`, choose a CSV, click **Upload CSV**. The page shows the returned `job_id` once S3 accepts the file; processing continues in the background.

The deployed upload API URL lives in `frontend/app.js` — update it if the API is deployed to a different stage/account (see §5 for the current `dev` endpoint).

---

## 10. Deploy to AWS

```bash
serverless print      # check rendered config
serverless package    # build the deployment package
serverless deploy     # deploy
```

After deploying:
1. Confirm the Lambda functions and queues were created.
2. Confirm the database is at the latest Alembic migration.
3. Confirm Workflow B waits for matching to finish before returning success (see the flagged inconsistency in §2).
4. Upload a test CSV through the frontend.
5. Check that Workflow B and Workflow C receive the same `job_id`.
6. Check the notification workflow and the SES test inbox.

`.env` and the local virtual environment are not packaged in the deployment.

---

## 11. Error handling and retries

| Scenario | Behavior |
|---|---|
| **Temporary AWS/DB failure** | Lambda raises, SQS retries the message. Chunk state prevents successful work being inserted twice. |
| **Invalid CSV row** | Row rejected, written to `crawl_errors`; other valid rows in the same chunk continue. |
| **Invalid CSV file** (empty, malformed, bad header, missing `user_id`) | Job marked `failed`. Retrying the same file won't help. |
| **Duplicate S3/SQS delivery** | Job/chunk state checked before processing; completed chunks skipped; DB uniqueness constraint protects the `users` table; `triggerMatching`'s row lock + `locked_until` window stops a duplicate webhook call mid-flight. |
| **Workflow B or C failure** | Webhook delivery records the latest error + HTTP status; matching SQS message retried; after repeated failures the message moves to the matching dead-letter queue. On retry, the *whole* delivery re-runs from Workflow B — see the note in §3. |

---

## 12. Useful commands  

```bash
# Install dependencies
python -m pip install -r requirements-dev.lock

# Run tests
python -m pytest -q

# Compile-check Python files
python -m compileall -q pipeline database alembic tests

# Check dependency consistency
python -m pip check

# Check migration state
alembic current

# Render and package Serverless configuration
serverless print
serverless package

# Serve the frontend
python -m http.server 8080 --directory frontend
```