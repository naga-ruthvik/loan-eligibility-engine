# Pipeline Flow

This document explains how a CSV moves through the application and how the
external n8n workflows fit into the process.

The Python code owns file upload, ingestion, database state, and the start of
the matching workflow. The n8n workflows own product discovery, matching, and
notifications.

## 1. Complete flow

```text
Browser
   |
   | Request an upload URL
   v
API Gateway -> createUploadUrl Lambda -> PostgreSQL
   |
   | Return temporary upload permission
   v
Browser ---------------------------> S3
                                      |
                                      | Object-created event
                                      v
                               Ingestion SQS Queue
                                      |
                                      v
                               ingestUsers Lambda
                                      |
                                      | Read and save chunks
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
                         Workflow B - external n8n
                                      |
                                      | After matching finishes
                                      v
                         Workflow C - external n8n
                                      |
                                      v
                           SES / test inbox
```

The important ownership rule is:

```text
Lambda starts Workflow B.
Workflow B starts Workflow C inside n8n.
Lambda does not call Workflow C.
```

Workflow C must be connected after the final matching database write. The
no-candidate terminal path should also reach the handoff when Workflow C needs
to record that there are no messages to send. The handoff must run once per job,
not once per matching batch.

## 2. External workflow configuration

The n8n workflow definitions are outside this repository.

- **Workflow A** uses Firecrawl and Google Gemini to discover and extract loan
  products.
- **Workflow B** uses PostgreSQL queries and Google Gemini to match users to
  products.
- **Workflow C** reads completed matches and uses Amazon SES to send prototype
  notifications.

Firecrawl, Google Gemini, Amazon SES, and n8n are external workflow services.
They are not Python dependencies and are not executed by the Lambda source in
this repository.

## 3. Upload and ingestion

### Step 1: Request an upload URL

The frontend sends:

```http
POST /ingestions/upload-url
```

The upload Lambda:

1. Creates a unique `job_id`.
2. Creates an S3 key:

   ```text
   incoming/{job_id}/users.csv
   ```

3. Creates an `ingestion_jobs` row in PostgreSQL.
4. Creates a temporary S3 upload URL.
5. Returns the job ID and upload details to the browser.

The browser does not send the CSV body through Lambda. It uploads the file
directly to S3.

### Step 2: S3 sends an event to SQS

After the upload finishes, S3 sends an object-created message to the ingestion
SQS queue. The object key contains the job ID, so the worker can connect the
file to the correct database record.

SQS separates the upload from processing. A temporary database or AWS failure
can therefore be retried without asking the user to upload the file again.

### Step 3: The ingestion Lambda claims a chunk

The ingestion Lambda receives one SQS record and creates or claims an
`ingestion_chunks` row before reading S3.

The database stores the cursor for the next part of the file:

- `next_byte_offset`
- `next_row_number`
- `next_chunk_number`
- The CSV headers

The chunk state prevents two Lambda invocations from processing the same chunk
at the same time.

### Step 4: The worker reads only one part of the file

The first invocation reads from byte `0`. Later invocations use an S3 range read
starting at the saved byte offset.

```text
S3 object
  -> chunk 0
  -> chunk 1
  -> chunk 2
  -> ...
```

The complete CSV is not loaded into Lambda memory. A continuation message is
sent to the ingestion queue when more rows remain.

### Step 5: Rows are minimally validated

The parser checks only what the prototype needs:

- The header contains `user_id`.
- An accepted row has a `user_id` value.
- Supplied numeric values are valid.
- Supplied numeric values are not negative.

Name, email, and employment status are optional. The prototype does not perform
strict email validation or extensive field validation.

The parser returns a simple `CsvChunk` containing accepted rows, rejected row
errors, the next byte offset, the next row number, a more-data flag, and the
headers. There is no separate row wrapper type.

### Step 6: Valid rows are saved safely

Valid rows are inserted into the `users` table. PostgreSQL enforces uniqueness
for:

```text
(ingestion_job_id, source_user_id)
```

If the same source user appears twice for one job, the duplicate is not inserted
and is recorded as a rejected row.

The worker updates the job and chunk counters after each successful chunk. When
the final chunk is complete, the job is marked `completed` and one durable
`webhook_deliveries` record is created with:

```json
{
  "job_id": "the-ingestion-job-id"
}
```

## 4. Starting Workflow B

The matching SQS queue receives a message containing the delivery ID. The
`triggerMatching` Lambda:

1. Loads the delivery from PostgreSQL.
2. Claims it with a database lock.
3. Sends the saved `job_id` payload to Workflow B.
4. Records whether Workflow B returned a successful response.

The request body is:

```json
{
  "job_id": "the-ingestion-job-id"
}
```

The Lambda makes one n8n request only. It does not make a second request to
Workflow C.

When the Lambda records the delivery as `sent`, that means Workflow B accepted
the request. It does not mean that Workflow C has finished sending email.

## 5. Workflow B to Workflow C inside n8n

Workflow B performs the complete matching process:

1. Loads the users for the job.
2. Loads active loan products.
3. Applies hard eligibility filters.
4. Applies soft criteria and selects candidate products.
5. Processes candidate batches.
6. Writes the final results to `matches`.
7. Hands the job to Workflow C inside n8n.

The handoff to Workflow C must be placed after the final matching work, not on a
per-batch branch. If Workflow B has a no-candidate branch, that branch should
also converge on the handoff when the notification workflow needs to finish the
job.

Workflow C receives the same `job_id`. It can then query PostgreSQL for the
completed matches, group products by user, and send test messages through SES.

The exact n8n mechanism can be the configured workflow-to-workflow trigger or an
internal n8n HTTP request. It must be configured in n8n and must not be added as
a second Lambda request.

## 6. Retry behavior

### Ingestion failure

If S3, PostgreSQL, or another temporary dependency fails, the ingestion Lambda
returns a failed SQS item. SQS retries the message. Completed chunk state and
database uniqueness constraints prevent successful work from being duplicated.

### Invalid file

An empty file, invalid header, malformed CSV, or missing `user_id` is recorded as
a permanent job failure. Retrying the same invalid file does not fix it.

### Matching start failure

If the matching Lambda cannot successfully notify Workflow B, the delivery is
marked failed and the matching SQS message is retried. After repeated failures,
the message moves to the matching dead-letter queue.

### Workflow B or Workflow C execution failure

The matching Lambda only knows whether Workflow B accepted the request. Workflow
B and Workflow C need their own n8n retry and error-handling behavior.

Both workflows should be idempotent by `job_id` because a network timeout can
leave the caller unsure whether an earlier execution was accepted.

Workflow B already upserts match rows. Workflow C should also prevent duplicate
emails if the same job is started more than once.

## 7. Why the design handles large files

### S3 stores the file

S3 stores the complete CSV instead of API Gateway or Lambda memory.

### Lambda processes bounded chunks

Memory use depends mostly on the current chunk size, not the complete file size.

### SQS provides buffering and retry

Multiple uploads can wait in the queue while Lambda workers process them. A
failed temporary operation can be retried.

### One job stays ordered

Chunks for one job are processed sequentially. Different jobs may progress in
parallel when AWS has available Lambda capacity.

## 8. Database tables used by the pipeline

| Table | Purpose |
|---|---|
| `ingestion_jobs` | One record for each uploaded CSV. |
| `users` | Valid users imported from CSV. |
| `loan_products` | Products used by external matching workflows. |
| `matches` | Current result for a user and loan product pair. |
| `notifications` | Notification attempts associated with matches. |
| `crawl_errors` | Invalid rows and processing errors. |
| `ingestion_chunks` | Resumable chunk progress and locks. |
| `webhook_deliveries` | Durable state for starting Workflow B. |

## 9. Code map

| File | Responsibility |
|---|---|
| `serverless.yml` | AWS functions, queues, bucket, permissions, and deployment settings. |
| `pipeline/upload.py` | Creates an ingestion job and a direct S3 upload URL. |
| `pipeline/csv_ingestion.py` | Reads CSV chunks and performs minimal validation. |
| `pipeline/ingestion.py` | Coordinates S3 reads, database inserts, continuation messages, and matching delivery creation. |
| `pipeline/matching.py` | Starts Workflow B only and records the delivery result. |
| `database/models.py` | Defines the PostgreSQL models and constraints. |
| `database/session.py` | Provides shared SQLAlchemy connections and transactions. |
| `alembic/versions/` | Contains database migrations. |
| `frontend/` | Contains the browser upload page. |
| `tests/` | Contains offline tests for the Python pipeline. |
