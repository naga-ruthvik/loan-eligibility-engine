# Loan Eligibility Engine

The Loan Eligibility Engine accepts a CSV file containing users, saves the
users in PostgreSQL, and starts a loan-product matching process.

The application is built as a small serverless pipeline:

1. A browser asks for a temporary upload URL.
2. The browser uploads the CSV directly to Amazon S3.
3. S3 sends an event to Amazon SQS.
4. AWS Lambda reads the CSV in small batches and saves valid rows to PostgreSQL.
5. When the upload is complete, another Lambda calls the matching n8n workflow.
6. After matching succeeds, the same Lambda calls the notification n8n workflow.

The project is intentionally simple. CSV validation is minimal, large files are
processed in chunks, and n8n handles the matching and notification workflows.

---

## 1. Architecture at a glance

```text
Browser
   |
   | POST /ingestions/upload-url
   v
API Gateway
   |
   v
createUploadUrl Lambda --------> PostgreSQL
   |
   | temporary presigned URL
   v
Browser -----------------------> S3
                                  |
                                  | object-created event
                                  v
                           Ingestion SQS Queue
                                  |
                                  v
                           ingestUsers Lambda
                                  |
                                  | read CSV chunks
                                  v
                              PostgreSQL
                                  |
                                  | ingestion.completed
                                  v
                           Matching SQS Queue
                                  |
                                  v
                        triggerMatching Lambda
                                  |
                                  | 1. {"job_id": "..."}
                                  v
                       n8n Workflow B: Matching
                                  |
                                  | matching must finish first
                                  v
                       n8n Workflow C: Email
                                  |
                                  v
                         SES / test email inbox
```

### Important Workflow B and Workflow C detail

The application calls the workflows in this order:

```text
triggerMatching Lambda
        |
        | call Workflow B
        | wait for a successful response
        v
call Workflow C
```

Workflow C is the third workflow in the business process. It receives the same
`job_id` as Workflow B. The application calls Workflow C from
`pipeline/matching.py`; Workflow B must not call Workflow C again, or it will be
triggered twice.

Workflow B must be configured to return its successful webhook response only
after the matching nodes have finished. If it responds immediately, Workflow C
could start before the matches are stored.

The application sends this small JSON body to both workflows:

```json
{
  "job_id": "the-ingestion-job-id"
}
```

The actual webhook URLs are provided through environment variables. They are not
hard-coded in the Python application.

---

## 2. How a CSV upload works

### Step 1: The browser asks for an upload URL

The frontend sends:

```http
POST /ingestions/upload-url
```

The `createUploadUrl` Lambda:

1. Creates a unique `job_id`.
2. Creates an S3 object key:

   ```text
   incoming/{job_id}/users.csv
   ```

3. Creates an `ingestion_jobs` record in PostgreSQL.
4. Creates a temporary S3 presigned URL.
5. Returns the job ID and upload URL to the browser.

The browser never receives AWS credentials. The presigned URL only gives the
browser temporary permission to upload that one file.

### Step 2: The browser uploads directly to S3

The browser sends the CSV to S3 with a `PUT` request. The CSV does not pass
through API Gateway or Lambda. This keeps the upload path suitable for larger
files and avoids putting the whole file in Lambda memory.

The upload uses the `text/csv` content type and a regional S3 Signature V4 URL.
The regional URL avoids browser problems caused by S3 redirects.

### Step 3: S3 starts background processing

The S3 bucket sends an object-created message to the ingestion SQS queue when a
CSV under `incoming/` has been uploaded.

SQS separates the upload from the processing. If the database or S3 has a
temporary problem, the message can be retried instead of losing the upload.

### Step 4: The ingestion Lambda reads the CSV in chunks

The `ingestUsers` Lambda does not load the entire file at once. It reads a
limited number of rows, saves them, and records where the next chunk should
start.

The default batch size is:

```text
INGESTION_BATCH_ROWS=1000
```

The saved cursor contains values such as:

- The next S3 byte offset.
- The next CSV row number.
- The next chunk number.
- The CSV headers.

If more rows remain, the Lambda sends a continuation message to the ingestion
queue. A large file therefore becomes a simple sequence:

```text
chunk 0 -> chunk 1 -> chunk 2 -> ... -> completed
```

Chunks for one job are processed in order. Different jobs can still progress in
parallel when AWS has available Lambda capacity.

### Step 5: CSV validation stays minimal

This is a prototype, so the parser checks only what the pipeline needs:

- The CSV has a `user_id` column.
- Each accepted row has a `user_id` value.
- Numeric fields contain valid numbers when they are supplied.
- Numeric fields are not negative.

The parser does not perform strict email validation or extensive business
validation. Invalid rows are rejected and recorded in the `crawl_errors` table;
valid rows in the same batch continue to be processed.

### Step 6: Users are saved safely

Valid rows are inserted into PostgreSQL. The database prevents the same
`source_user_id` from being inserted twice for the same job:

```text
unique (ingestion_job_id, source_user_id)
```

This protects the database when an S3 event or SQS message is delivered more
than once.

When the final chunk is complete, the job is marked as `completed`. The system
also creates a durable webhook delivery record with the `job_id`.

### Step 7: Matching and notification start

The `triggerMatching` Lambda receives the durable delivery ID from the matching
SQS queue. It then:

1. Loads the saved `job_id` from PostgreSQL.
2. Calls Workflow B using `N8N_WEBHOOK_URL`.
3. Waits for a successful 2xx response.
4. Calls Workflow C using `N8N_WORKFLOW_C_WEBHOOK_URL`.
5. Marks the delivery as sent only after both calls succeed.

If either workflow fails, the delivery is marked as failed and SQS can retry the
message. After the configured retry count, the message moves to the matching
dead-letter queue.

The application only triggers the n8n workflows. Workflow B is responsible for
creating or updating matches. Workflow C is responsible for reading the
completed matches and sending the notification emails through its configured
email provider, such as Amazon SES.

---

## 3. CSV format

The required column is `user_id`. The other supported columns are optional:

```csv
user_id,name,email,monthly_income,credit_score,employment_status,age
u-001,Jane Doe,jane@example.com,3500,720,Employed,31
u-002,John Doe,john@example.com,2800,680,Self-employed,28
```

The database stores these fields as:

| CSV column | Database field | Required? |
| --- | --- | --- |
| `user_id` | `source_user_id` | Yes |
| `name` | `full_name` | No |
| `email` | `email` | No |
| `monthly_income` | `monthly_income` | No |
| `credit_score` | `credit_score` | No |
| `employment_status` | `employment_status` | No |
| `age` | `age` | No |

The notification workflow should use the configured test inbox aliases for the
prototype. It should not send messages to email addresses from the sample CSV.

---

## 4. Project structure

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
│   └── session.py               Shared engine and transaction sessions
├── alembic/
│   ├── env.py                   Alembic database configuration
│   └── versions/                Database migration files
├── frontend/
│   ├── index.html               CSV upload page
│   ├── app.js                   Requests the URL and uploads to S3
│   └── styles.css               Upload page styling
├── tests/
│   ├── test_csv_ingestion.py    CSV parsing and validation tests
│   ├── test_pipeline_messages.py S3/SQS message parsing tests
│   └── test_matching_webhooks.py Workflow B -> Workflow C ordering test
├── docs/
│   └── pipeline-flow.md         Detailed pipeline and scalability notes
├── requirements.txt              Runtime dependency list
├── requirements.lock             Locked runtime dependencies
├── requirements-dev.txt           Runtime plus development dependencies
├── requirements-dev.lock          Locked development dependencies
├── .env.example                  Safe environment-variable template
└── README.md                     This document
```

### Main application files

#### `serverless.yml`

This is the deployment definition. It creates and configures:

- AWS Lambda functions.
- An encrypted S3 upload bucket.
- An ingestion SQS queue and dead-letter queue.
- A matching SQS queue and dead-letter queue.
- S3-to-SQS event notifications.
- IAM roles and permissions.
- Lambda environment variables.

The current deployment settings are:

```text
AWS region:   ap-southeast-2
Python:       3.12
Architecture: x86_64
```

All Lambda functions use the configured shared dependency layer.

#### `pipeline/upload.py`

Contains the `create_upload_url` Lambda handler. It creates a job, builds the
S3 key, creates a regional presigned URL, and returns the upload information.

#### `pipeline/csv_ingestion.py`

Contains the small streaming CSV parser. It returns a `CsvChunk` containing:

- Accepted rows.
- Rejected row errors.
- The next byte offset.
- The next row number.
- Whether more data remains.
- The CSV headers.

There is no separate `ParsedUserRow` object. Keeping the result as `CsvChunk`
makes the prototype easier to follow.

#### `pipeline/ingestion.py`

Coordinates the ingestion process. It parses S3 and continuation messages,
claims chunks, reads byte ranges from S3, writes users and errors to PostgreSQL,
updates job progress, creates continuation messages, and creates the durable
webhook delivery when the job is complete.

#### `pipeline/matching.py`

Processes the matching SQS message. It protects the webhook delivery from
duplicate processing, calls Workflow B, calls Workflow C after Workflow B
succeeds, records the result, and allows SQS to retry failed calls.

#### `database/models.py`

Defines the PostgreSQL schema used by the application:

- `ingestion_jobs`: one record for each CSV upload.
- `users`: users imported from a CSV job.
- `loan_products`: loan products and their criteria.
- `matches`: the result for one user and one loan product.
- `notifications`: notification attempts associated with matches.
- `crawl_errors`: rejected rows and processing errors.
- `ingestion_chunks`: progress and state for each CSV chunk.
- `webhook_deliveries`: durable Workflow B and Workflow C delivery state.

#### `database/session.py`

Creates one SQLAlchemy engine per warm process and provides short-lived,
transaction-scoped sessions. A successful session commits; a failed session is
rolled back and closed.

#### `alembic/versions/`

Contains the database migrations. Migrations should be run as a deployment step,
not during a Lambda cold start or a user request.

#### `frontend/`

Contains a simple browser page for selecting and uploading a CSV. The browser
requests the upload URL from the deployed API and then uploads the file directly
to S3.

#### `tests/`

Contains offline tests for the parser, message handling, and Workflow B to
Workflow C call order. The tests do not call AWS, n8n, or SES.

---

## 5. Database and migrations

The application uses PostgreSQL through SQLAlchemy and the Psycopg driver.

The database URL can use either of these forms:

```text
postgresql://user:password@host:5432/database
postgresql+psycopg://user:password@host:5432/database
```

The application normalizes the first form to the explicit Psycopg driver.

After setting `DATABASE_URL`, run:

```bash
alembic upgrade head
```

To see the current migration:

```bash
alembic current
```

The database uses constraints and state fields for safety. For example:

- Duplicate users for one job are ignored and recorded as rejected.
- A completed chunk is not processed again.
- A webhook delivery has one durable record per job and event type.
- Parent deletion cascades to related users, chunks, matches, and errors.

---

## 6. Local setup

### Requirements

Install or provide:

- Python 3.12.
- Node.js and the Serverless Framework for deployment.
- A PostgreSQL database.
- AWS credentials for deployment and local AWS access.
- n8n Workflow B and Workflow C webhook URLs.

### Create a virtual environment

On Windows PowerShell:

```powershell
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.lock
```

On Linux or macOS:

```bash
python3.12 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.lock
```

The lock file pins the runtime and development packages so the project can be
rebuilt consistently.

### Configure environment variables

Copy the example file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Set at least these values in `.env`:

```env
DATABASE_URL=postgresql+psycopg://username:password@localhost:5432/loan_eligibility
N8N_WEBHOOK_URL=https://your-n8n-host/webhook/match-users
N8N_WORKFLOW_C_WEBHOOK_URL=https://your-n8n-host/webhook/workflow-c
```

Never commit `.env`. Use `.env.example` for safe placeholders only.

### Prepare the database

```bash
alembic upgrade head
```

### Run the tests

Use the project virtual environment:

```bash
python -m pytest -q
```

On Windows, if the environment is not activated, use:

```powershell
.\venv\Scripts\python.exe -m pytest -q
```

The tests are designed to run offline and do not require AWS or n8n.

---

## 7. Run the frontend locally

From the project root, start a simple web server:

```bash
python -m http.server 8080 --directory frontend
```

Then open:

```text
http://localhost:8080
```

Choose a CSV file and click **Upload CSV**. The page displays the returned
`job_id` after S3 accepts the file. Processing continues in the background.

The deployed upload API URL is currently stored in:

```text
frontend/app.js
```

If the API is deployed to a different stage or account, update that value before
using the frontend.

---

## 8. Deploy to AWS

The Serverless Framework reads the AWS settings from `serverless.yml` and the
environment variables from the deployment shell or `.env`.

Check the rendered configuration:

```bash
serverless print
```

Create a deployment package:

```bash
serverless package
```

Deploy the service:

```bash
serverless deploy
```

After deployment:

1. Confirm the Lambda functions and queues were created.
2. Confirm the database is at the latest Alembic migration.
3. Confirm Workflow B waits for matching to finish before returning success.
4. Upload a test CSV through the frontend.
5. Check that Workflow B and Workflow C receive the same `job_id`.
6. Check the notification workflow and SES test inbox.

The deployment does not package `.env` or the local virtual environment.

---

## 9. Error handling and retries

### Temporary AWS or database failure

The Lambda raises an error and the SQS message is retried. The database chunk
state prevents successful work from being inserted twice.

### Invalid CSV row

The row is rejected and written to `crawl_errors`. Other valid rows in the same
chunk continue.

### Invalid CSV file

An empty file, malformed CSV, invalid header, or missing `user_id` causes the
job to be marked as failed. Retrying the same invalid file will not fix it.

### Duplicate S3 or SQS delivery

The job and chunk records are checked before processing. Completed chunks are
skipped, and the database uniqueness constraint protects the users table.

### Workflow B or Workflow C failure

The webhook delivery records the latest error and HTTP status. The matching SQS
message is retried. After repeated failures, the message moves to the matching
dead-letter queue.

---

## 10. Current prototype limits

This is a working prototype, not a production security configuration.

- The upload API does not currently use an authorizer.
- The S3 bucket CORS rule is open for the prototype frontend.
- The upload URL is temporary, but anyone who receives it may use it before it
  expires.
- Webhook authentication and request signing are not implemented here.
- Workflow B and Workflow C are external n8n workflows and are not stored in
  this repository.
- The database and n8n capacity should be measured before handling production
  traffic.
- The current matching delivery retries the saved delivery if Workflow C fails;
  review Workflow B idempotency before production use.

Before production use, add authentication, restrict CORS, protect webhook
endpoints, manage secrets through a secret manager, and add monitoring.

---

## 11. Useful commands

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

For the most reliable local checks on Windows, run these commands with the
project interpreter:

```powershell
.\venv\Scripts\python.exe -m pytest -q
.\venv\Scripts\python.exe -m compileall -q pipeline database alembic tests
```
