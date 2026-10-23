# Loan Eligibility Engine

The Loan Eligibility Engine accepts a CSV file containing users, stores the
users in PostgreSQL, and starts the external loan-matching and notification
workflows.

The Python project owns the upload and ingestion pipeline. The n8n workflows
own product discovery, loan matching, and email notifications.

## Architecture

```text
Browser
   |
   | Request an upload URL
   v
API Gateway
   |
   v
createUploadUrl Lambda ----------> PostgreSQL
   |
   | Temporary upload permission
   v
Browser -------------------------> S3
                                    |
                                    | Object-created event
                                    v
                             Ingestion SQS Queue
                                    |
                                    v
                             ingestUsers Lambda
                                    |
                                    | Read CSV chunks
                                    v
                                PostgreSQL
                                    |
                                    | Ingestion completed
                                    v
                             Matching SQS Queue
                                    |
                                    v
                          triggerMatching Lambda
                                    |
                                    | Start Workflow B only
                                    v
                        Workflow B - n8n matching
                                    |
                                    | After all matching paths finish
                                    v
                   Workflow C - n8n notification workflow
                                    |
                                    v
                         Amazon SES / test inbox
```

### Important Workflow B and Workflow C rule

The Lambda starts **Workflow B only**. It does not call Workflow C.

After Workflow B has completed its matching work, Workflow B starts Workflow C
inside n8n. Workflow C receives the same `job_id`, reads the completed matches,
and sends the notification emails.

```text
Matching SQS
    |
    v
triggerMatching Lambda
    |
    | Start Workflow B with {"job_id": "..."}
    v
Workflow B in n8n
    |
    | Complete matching and save matches
    | Then hand off to Workflow C inside n8n
    v
Workflow C in n8n
```

Workflow C must be connected from Workflow B after the final matching path. The
no-candidate path should also finish through the handoff when the notification
workflow needs to record that there are no emails to send. Workflow C should be
started once per job.

The Lambda's delivery status means that the request to Workflow B was accepted.
It does not mean that Workflow C has finished sending email.

## External workflow configuration

The n8n workflows are external to this repository. Their definitions are not
stored in the Python project.

The external services are:

- **n8n:** hosts Workflow A, Workflow B, and Workflow C.
- **Firecrawl:** used by Workflow A to fetch source pages.
- **Google Gemini:** used by Workflow A to extract product data and by Workflow B
  to evaluate ambiguous eligibility rules.
- **Amazon SES:** used by Workflow C to send the prototype notification emails.

### Workflow A - product discovery and extraction

Workflow A runs independently on a schedule and keeps the loan-product catalog
fresh. It collects source pages, asks Gemini to extract product information,
validates the extracted fields, and upserts products into PostgreSQL. Invalid
records and crawl failures are written to `crawl_errors`.

Workflow A is an external n8n workflow. Firecrawl and Gemini calls happen in
n8n, not in the Python Lambda code.

### Workflow B - user and loan matching

Workflow B is started with:

```json
{
  "job_id": "the-ingestion-job-id"
}
```

It loads the users for the job, selects active loan products, filters possible
matches, evaluates the remaining criteria, and upserts results into `matches`.

Workflow B then starts Workflow C from n8n after all matching batches and final
database writes are complete. The handoff must not be placed on a path that can
run once per batch. It must run once after the complete matching process.

The Lambda does not make a second HTTP request to Workflow C.

### Workflow C - notification and email dispatch

Workflow C is also an external n8n workflow. It receives the `job_id` from
Workflow B, reads eligible and review matches from PostgreSQL, groups products
by user, and sends the prototype emails through Amazon SES.

The prototype routes messages to approved test inbox aliases instead of using
the email addresses from the sample CSV.

The exact n8n handoff can use the configured n8n workflow-to-workflow mechanism
or an internal n8n HTTP request. Whichever mechanism is used, it must be owned
by n8n and must not be duplicated by the Lambda.

## How the application works

### 1. The browser requests an upload URL

The frontend sends:

```http
POST /ingestions/upload-url
```

The `createUploadUrl` Lambda:

1. Creates a unique `job_id`.
2. Creates an S3 key in this format:

   ```text
   incoming/{job_id}/users.csv
   ```

3. Creates an `ingestion_jobs` record in PostgreSQL.
4. Creates a temporary S3 upload URL.
5. Returns the job ID and upload details to the browser.

The browser never receives AWS credentials and does not send the CSV through
Lambda.

### 2. The browser uploads directly to S3

The browser sends the CSV to S3 with a `PUT` request. S3 stores the complete
file and emits an object-created event after the upload finishes.

Direct upload keeps the large file out of API Gateway and Lambda memory.

### 3. S3 starts ingestion through SQS

The S3 bucket sends `.csv` files under `incoming/` to the ingestion SQS queue.
SQS separates uploading from processing and provides retry behavior when a
temporary failure occurs.

### 4. The ingestion Lambda reads the CSV in chunks

The `ingestUsers` Lambda reads a bounded number of rows at a time. It records:

- The current byte position in S3.
- The next CSV row number.
- The next chunk number.
- The CSV headers.

If more rows remain, the Lambda sends a continuation message to the same queue:

```text
chunk 0 -> chunk 1 -> chunk 2 -> ... -> completed
```

Chunks for one job are processed in order. Different jobs can progress in
parallel when AWS has available capacity.

### 5. CSV validation is intentionally minimal

The prototype checks only the fields needed by the pipeline:

- The CSV has a `user_id` column.
- Each accepted row has a `user_id` value.
- Supplied numeric fields contain valid numbers.
- Supplied numeric fields are not negative.

Strict email validation and extensive business validation are intentionally not
part of this prototype.

Invalid rows are saved as `crawl_errors`. Other valid rows in the same chunk
continue to be processed.

### 6. Valid rows are saved safely

Users are inserted into PostgreSQL. A unique constraint prevents the same
`source_user_id` from being inserted twice for the same upload job:

```text
(ingestion_job_id, source_user_id)
```

This protects the database when S3 or SQS delivers the same event more than
once.

When the final chunk is complete, the job is marked as `completed` and a durable
webhook delivery record is created with the `job_id`.

### 7. The matching Lambda starts Workflow B

The `triggerMatching` Lambda receives the durable delivery ID from the matching
queue. It claims the delivery, loads the saved payload, and sends the following
request to Workflow B:

```json
{
  "job_id": "the-ingestion-job-id"
}
```

The Lambda makes only this one n8n request. It marks the delivery as sent after
Workflow B returns a successful response.

Workflow B then owns the rest of the process and starts Workflow C from n8n
after matching is complete.

## CSV format

The required column is `user_id`. The other supported columns are optional:

```csv
user_id,name,email,monthly_income,credit_score,employment_status,age
u-001,Jane Doe,jane@example.com,3500,720,Employed,31
u-002,John Doe,john@example.com,2800,680,Self-employed,28
```

| CSV column | Database field | Required? |
| --- | --- | --- |
| `user_id` | `source_user_id` | Yes |
| `name` | `full_name` | No |
| `email` | `email` | No |
| `monthly_income` | `monthly_income` | No |
| `credit_score` | `credit_score` | No |
| `employment_status` | `employment_status` | No |
| `age` | `age` | No |

## Project structure

```text
loan-eligibility-engine/
├── serverless.yml              AWS, Lambda, S3, SQS, IAM, and deployment setup
├── pipeline/
│   ├── config.py                Pipeline settings used by Lambda handlers
│   ├── upload.py                Upload URL API Lambda
│   ├── csv_ingestion.py         Streaming CSV parser and minimal validation
│   ├── ingestion.py             S3/SQS ingestion worker
│   └── matching.py              Workflow B delivery worker
├── database/
│   ├── config.py                Database connection settings
│   ├── models.py                SQLAlchemy database models
│   └── session.py               Shared engine and transaction sessions
├── alembic/
│   ├── env.py                   Alembic database configuration
│   └── versions/                Database migration files
├── frontend/
│   ├── index.html               CSV upload page
│   ├── app.js                   Requests the upload URL and uploads to S3
│   └── styles.css               Upload page styling
├── tests/
│   ├── test_csv_ingestion.py    CSV parsing and validation tests
│   ├── test_pipeline_messages.py S3/SQS message parsing tests
│   └── test_matching_webhooks.py Lambda-to-Workflow-B delivery test
├── docs/
│   └── pipeline-flow.md         Detailed pipeline and reliability notes
├── requirements.txt             Runtime dependencies
├── requirements.lock            Locked runtime dependencies
├── requirements-dev.txt         Development dependencies
├── requirements-dev.lock        Locked development dependencies
└── README.md                    This document
```

### Important files

| File | Purpose |
|---|---|
| `serverless.yml` | Defines the AWS functions, queues, bucket, permissions, and deployment settings. |
| `pipeline/upload.py` | Creates an ingestion job and a direct S3 upload URL. |
| `pipeline/csv_ingestion.py` | Reads CSV chunks and performs minimal validation. |
| `pipeline/ingestion.py` | Reads S3 chunks, writes users and errors, updates progress, and creates the matching delivery. |
| `pipeline/matching.py` | Starts Workflow B only and records the delivery result. |
| `database/models.py` | Defines jobs, users, products, matches, errors, chunks, notifications, and webhook deliveries. |
| `database/session.py` | Provides a shared SQLAlchemy engine and short-lived transactions. |
| `alembic/versions/` | Contains database migrations. |
| `frontend/` | Provides the browser upload page. |
| `tests/` | Contains offline tests for the Python pipeline. |

## Database

The application uses PostgreSQL through SQLAlchemy and Psycopg.

The main tables are:

- `ingestion_jobs`: one record for each CSV upload.
- `users`: users imported from a CSV job.
- `loan_products`: loan products and their criteria.
- `matches`: one current result for each user and product pair.
- `notifications`: notification attempts associated with matches.
- `crawl_errors`: rejected rows and processing errors.
- `ingestion_chunks`: progress for each CSV chunk.
- `webhook_deliveries`: durable delivery state for starting Workflow B.

Apply migrations with:

```bash
alembic upgrade head
```

Check the current migration with:

```bash
alembic current
```

Migrations should run as a deployment step, not during a Lambda cold start or a
user request.

## AWS deployment

The service is configured for:

```text
Region:       ap-southeast-2
Python:       3.12
Architecture: x86_64
```

The deployed functions are:

| Function | Trigger | Timeout |
|---|---|---:|
| `createUploadUrl` | `POST /ingestions/upload-url` | 29 seconds |
| `ingestUsers` | Ingestion SQS queue | 900 seconds |
| `triggerMatching` | Matching SQS queue | 30 seconds |

The service also creates:

- An encrypted, versioned S3 upload bucket.
- An ingestion queue and dead-letter queue.
- A matching queue and dead-letter queue.
- S3-to-SQS event notification rules.
- Separate IAM roles for the three Lambda functions.

Useful deployment commands:

```bash
serverless print
serverless package
serverless deploy
```

## Frontend

Start the local frontend from the project root:

```bash
python -m http.server 8080 --directory frontend
```

Open:

```text
http://localhost:8080
```

Choose a CSV file and click **Upload CSV**. The page shows the `job_id` after
S3 accepts the file. Processing continues in the background.

## Retries and failure handling

### Temporary AWS or database failure

The Lambda raises an error and SQS retries the message. Database chunk state
prevents completed work from being inserted twice.

### Invalid CSV row

The row is rejected and recorded in `crawl_errors`. Other valid rows continue.

### Invalid CSV file

An empty file, malformed CSV, invalid header, or missing `user_id` causes the
job to fail. Retrying the same invalid file does not fix it.

### Duplicate S3 or SQS delivery

The job and chunk state are checked before processing. Completed chunks are
skipped, and the database uniqueness constraint protects the users table.

### Workflow B failure

The durable delivery records the latest error and the matching SQS message is
retried. After repeated failures, the message moves to the matching dead-letter
queue.

### Workflow C failure

Workflow C is started and retried by n8n. The Python Lambda does not receive a
separate Workflow C result and does not retry Workflow C itself.

Workflow B and Workflow C should both be idempotent by `job_id`. This prevents
duplicate matches or duplicate emails if a job is started again after an
ambiguous timeout.

## Prototype notes

This is a working prototype, not a production security configuration.

- The upload API does not currently use an authorizer.
- The S3 CORS rule is open for the prototype frontend.
- Webhook authentication and request signing are not implemented here.
- The external n8n workflows are not stored in this repository.
- Database and n8n capacity should be measured before production traffic.
- Workflow C should continue using test inbox aliases until production email
  routing is intentionally enabled.

## Useful commands

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

# Render and package the Serverless configuration
serverless print
serverless package

# Serve the frontend
python -m http.server 8080 --directory frontend
```
