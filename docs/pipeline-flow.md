# CSV Ingestion Flow and Scalability Guide

This document explains the CSV upload pipeline in simple terms. It is written
for a developer who is new to AWS Lambda, S3, SQS, and asynchronous processing.

## 1. The big picture

The frontend does not send the CSV through Lambda. Instead, it uploads the file
directly to S3. S3 then starts the background processing flow.

```text
Frontend
   |
   | 1. Ask for an upload URL
   v
API Gateway -> createUploadUrl Lambda -> PostgreSQL
   |
   | 2. Return a temporary S3 upload URL
   v
Frontend -- 3. Upload CSV directly --> S3
                                      |
                                      | 4. Object-created event
                                      v
                               Ingestion SQS Queue
                                      |
                                      | 5. SQS invokes Lambda
                                      v
                               ingestUsers Lambda
                                      |
                                      | 6. Read CSV in batches
                                      v
                                  PostgreSQL
                                      |
                                      | 7. Job completed
                                      v
                               Matching SQS Queue
                                      |
                                      v
                             triggerMatching Lambda
                                      |
                                      | 8. POST {"job_id": "..."}
                                      v
                                      n8n
```

There are two separate background steps:

1. **Ingestion:** read the CSV and save users.
2. **Matching notification:** tell n8n that ingestion is complete.

Keeping these steps separate means a slow or unavailable n8n endpoint does not
stop the CSV from being saved in PostgreSQL.

## 1.1. The same flow as code

The following snippets are shortened versions of the real project code. They
show how the pieces connect without including every error-handling detail.

### Frontend: request a URL and upload the file

The frontend first asks the API for an upload URL. It then sends the file
directly to S3 using that URL.

```javascript
const response = await fetch(
  "https://1ptrorx4i9.execute-api.ap-southeast-2.amazonaws.com/ingestions/upload-url",
  {
    method: "POST",
  },
);

const upload = await response.json();

await fetch(upload.upload_url, {
  method: "PUT",
  headers: {
    "Content-Type": "text/csv",
  },
  body: csvFile,
});

console.log("Upload started for job:", upload.job_id);
```

The browser sends the CSV to S3. It does not send the CSV body to the Lambda.

### Upload Lambda: create the job and presigned URL

The API Lambda creates the job ID, stores the job, and creates the S3 URL:

```python
job_id = uuid4()
object_key = f"incoming/{job_id}/users.csv"

upload_url = s3_client.generate_presigned_url(
    ClientMethod="put_object",
    Params={
        "Bucket": settings.upload_bucket,
        "Key": object_key,
        "ContentType": "text/csv",
    },
    ExpiresIn=settings.presigned_url_expires_seconds,
)

with session_scope() as session:
    session.add(
        IngestionJob(
            id=job_id,
            s3_bucket=settings.upload_bucket,
            s3_key=object_key,
        )
    )
```

The S3 key contains the job ID, so later workers can connect the S3 file to
the correct database job.

### S3 event: start the ingestion worker

S3 sends a message to SQS after the upload finishes. The ingestion Lambda
receives the SQS record and processes it:

```python
def process_sqs_record(record: dict[str, Any]) -> None:
    work_item = parse_work_item(record.get("body", ""))
    follow_up = _process_work_item(work_item)
    _publish_follow_up(follow_up, work_item)
```

`parse_work_item` extracts values such as:

```python
WorkItem(
    job_id=job_id,
    bucket="generated-bucket",
    key=f"incoming/{job_id}/users.csv",
    chunk_number=0,
    start_byte_offset=0,
    start_row_number=2,
    initial_chunk=True,
)
```

### Ingestion Lambda: read one batch

The worker reads only one batch from S3:

```python
s3_params = {
    "Bucket": work_item.bucket,
    "Key": work_item.key,
}

if work_item.start_byte_offset > 0:
    s3_params["Range"] = f"bytes={work_item.start_byte_offset}-"

response = s3_client.get_object(**s3_params)

chunk = parse_csv_chunk(
    response["Body"],
    start_byte_offset=work_item.start_byte_offset,
    start_row_number=work_item.start_row_number,
    max_rows=settings.ingestion_batch_rows,
    initial_chunk=work_item.initial_chunk,
    headers=work_item.headers,
)
```

The first chunk starts at byte `0`. A later chunk starts from the byte offset
saved by the previous chunk.

### CSV parser: return valid rows and errors

The parser keeps the result simple:

```python
CsvChunk(
    rows=[
        {
            "source_user_id": "u-123",
            "full_name": "Jane Doe",
            "email": "jane@example.com",
            "monthly_income": Decimal("2500"),
            "credit_score": 700,
            "employment_status": "Employed",
            "age": 30,
        }
    ],
    errors=[],
    next_byte_offset=45821,
    next_row_number=1002,
    has_more=True,
    headers=["user_id", "name", "email"],
)
```

`rows` can be inserted into PostgreSQL. `errors` become records in
`crawl_errors`.

### Ingestion Lambda: insert rows safely

The database insert ignores a duplicate source user ID for the same job:

```python
values = [
    {**row, "ingestion_job_id": work_item.job_id}
    for row in csv_chunk.rows
]

statement = (
    pg_insert(User)
    .values(values)
    .on_conflict_do_nothing(
        constraint="uq_users_ingestion_job_source_user_id"
    )
)

session.execute(statement)
```

The unique constraint is the final protection against duplicate database rows.

### Ingestion Lambda: continue with another message

If there are more rows, the worker sends a continuation message:

```python
if csv_chunk.has_more:
    continuation = {
        "job_id": str(work_item.job_id),
        "bucket": work_item.bucket,
        "key": work_item.key,
        "chunk_number": work_item.chunk_number + 1,
        "start_byte_offset": csv_chunk.next_byte_offset,
        "start_row_number": csv_chunk.next_row_number,
        "initial_chunk": False,
        "headers": csv_chunk.headers,
    }

    sqs.send_message(
        QueueUrl=settings.ingestion_queue_url,
        MessageBody=json.dumps(continuation),
    )
```

This creates a simple chain:

```text
S3 event -> chunk 0 -> chunk 1 -> chunk 2 -> completed
```

### Matching Lambda: start Workflow B

After the final chunk, the matching Lambda sends only the job ID to the
matching workflow, Workflow B:

```python
payload = {"job_id": str(job_id)}

request = Request(
    settings.n8n_webhook_url,
    data=json.dumps(payload).encode("utf-8"),
    method="POST",
    headers={"Content-Type": "application/json"},
)

with urlopen(request, timeout=settings.webhook_timeout_seconds) as response:
    if 200 <= response.status < 300:
        delivery.status = "sent"
```

The equivalent request from a terminal would look like this:

```bash
curl -X POST "$N8N_WEBHOOK_URL" \
  -H "Content-Type: application/json" \
  -d '{"job_id":"8c1f..."}'
```

### Workflow B: start Workflow C after matching finishes

The matching Lambda now performs this handoff. It calls Workflow B using
`N8N_WEBHOOK_URL`, waits for a successful response, and then calls Workflow C
using `N8N_WORKFLOW_C_WEBHOOK_URL`.

Configure Workflow B to respond only after its last matching node has finished.
Do not configure Workflow C to be called from inside Workflow B as well, or it
will be triggered twice.

Workflow C webhook:

```text
https://naga-ruthvik.app.n8n.cloud/webhook/d4fbedc6-3ae7-4e5b-811a-a05382774200
```

The Lambda sends the following request to Workflow C:

```text
Method: POST
URL: https://naga-ruthvik.app.n8n.cloud/webhook/d4fbedc6-3ae7-4e5b-811a-a05382774200
Content-Type: application/json
```

Send this body:

```json
{
  "job_id": "8c1f..."
}
```

The Lambda replaces the example value with the actual job ID at runtime.

The important requirement is that Workflow C receives exactly the job ID for
the matching job.

The resulting flow is:

```text
Ingestion complete
        |
        v
Lambda calls Workflow B with {"job_id": "..."}
        |
        v
Workflow B creates/finishes user matches
        |
        v
Matching Lambda calls Workflow C webhook after Workflow B returns
        |
        | {"job_id": "..."}
        v
Workflow C reads matched users and sends test emails through SES
```

Workflow B should call Workflow C only after its matching operation has
completed. Calling the new webhook from the ingestion Lambda would be too
early because matching would not have finished yet.

## 2. Step-by-step flow

### Step 1: The frontend asks for an upload URL

The frontend calls:

```http
POST /ingestions/upload-url
```

The `createUploadUrl` Lambda does three things:

1. Creates a unique `job_id`.
2. Creates an S3 key using that job ID:

   ```text
   incoming/{job_id}/users.csv
   ```

3. Creates an `ingestion_jobs` row in PostgreSQL.

The Lambda then returns a temporary presigned S3 URL:

```json
{
  "job_id": "8c1f...",
  "bucket": "the-generated-bucket",
  "key": "incoming/8c1f.../users.csv",
  "upload_url": "https://...",
  "required_headers": {
    "Content-Type": "text/csv"
  }
}
```

The URL is temporary. The frontend can use it to upload the file without
receiving AWS credentials.

### Step 2: The frontend uploads directly to S3

The frontend sends the CSV to the returned URL:

```http
PUT <upload_url>
Content-Type: text/csv

<CSV contents>
```

The CSV does not pass through API Gateway or Lambda. This is important because
API Gateway and Lambda are not good places to proxy very large files.

S3 stores the file and creates an object-created event after the upload is
complete.

### Step 3: S3 sends a message to SQS

The S3 bucket is configured to send `.csv` files under the `incoming/` prefix
to the ingestion SQS queue.

The S3 event contains information such as:

- Bucket name.
- Object key.
- S3 version ID.

The ingestion code gets the job ID from the key:

```text
incoming/8c1f.../users.csv
         ^^^^^^
         job_id
```

SQS acts as a buffer between S3 and the Lambda. If the database is temporarily
unavailable, the message can wait and be retried instead of losing the upload.

### Step 4: The ingestion Lambda starts processing

The `ingestUsers` Lambda receives one SQS message and checks the related
`ingestion_jobs` row.

Before reading the file, it creates or claims an `ingestion_chunks` row. This
is a simple database lock that prevents two Lambda invocations from processing
the same chunk at the same time.

The job also stores the cursor for the next part of the file:

- `next_byte_offset`
- `next_row_number`
- `next_chunk_number`

These values allow processing to continue later.

### Step 5: The CSV is read in batches

The default batch size is configured by:

```text
INGESTION_BATCH_ROWS=1000
```

The Lambda does not load the entire CSV into memory. It reads a small batch,
for example:

```text
Rows 2 through 1001
```

When the next batch is needed, the Lambda reads the S3 object from the saved
byte offset instead of starting at the beginning:

```text
Range: bytes={next_byte_offset}-
```

After processing a batch, it sends a continuation message to the same SQS
queue:

```json
{
  "job_id": "8c1f...",
  "bucket": "the-generated-bucket",
  "key": "incoming/8c1f.../users.csv",
  "chunk_number": 1,
  "start_byte_offset": 45821,
  "start_row_number": 1002,
  "initial_chunk": false,
  "headers": [
    "user_id",
    "name",
    "email",
    "monthly_income",
    "credit_score",
    "employment_status",
    "age"
  ]
}
```

The next Lambda invocation uses this message to continue from the correct
place.

### Step 6: Rows are minimally validated

This is a prototype, so validation is intentionally small.

The parser checks:

- The CSV contains a `user_id` column.
- Every row has a `user_id` value.
- Numeric fields are valid numbers when supplied.
- Numeric fields are not negative because PostgreSQL rejects negative values.

Fields such as `name`, `email`, and `employment_status` are optional. The
prototype does not perform strict email validation or extensive field-length
validation.

For a valid row, the Lambda creates a database value like:

```python
{
    "source_user_id": "u-123",
    "full_name": "Jane Doe",
    "email": "jane@example.com",
    "monthly_income": 2500,
    "credit_score": 700,
    "employment_status": "Employed",
    "age": 30,
}
```

For an invalid row, the Lambda does not insert a user. It records the problem
in `crawl_errors` with the job ID, row number, source user ID, and error message.

### Step 7: Valid rows are saved in PostgreSQL

Valid rows are inserted into the `users` table in a batch.

The database has a unique constraint on:

```text
(ingestion_job_id, source_user_id)
```

This means the same user cannot be inserted twice for the same upload job.

The job counters are updated after each chunk:

- `processed_rows`: rows examined so far.
- `rejected_rows`: invalid or duplicate rows.
- `next_byte_offset`: where the next chunk starts.
- `next_row_number`: the next CSV row number.
- `next_chunk_number`: the next chunk ID.

### Step 8: The job completes

When there are no more CSV rows:

```text
ingestion_jobs.status = completed
```

The job stores its final counters and completion time.

The code then creates one `WebhookDelivery` record with this payload:

```json
{
  "job_id": "8c1f..."
}
```

The delivery record is created before the notification is sent. This is useful
because the database remembers that the notification still needs to be sent.

### Step 9: n8n is notified

The matching SQS queue receives a message containing the delivery ID.

The `triggerMatching` Lambda loads the saved payload and calls the configured
`N8N_WEBHOOK_URL`:

```http
POST <N8N_WEBHOOK_URL>
Content-Type: application/json
```

Body:

```json
{
  "job_id": "8c1f..."
}
```

If n8n returns a successful 2xx response, the delivery is marked `sent`.

If n8n is unavailable, the Lambda records the error and lets SQS retry the
message.

## 3. Why this can scale better than one large Lambda call

### S3 stores the large file

The file is stored in S3 rather than inside Lambda memory or API Gateway.
S3 is designed for large objects and durable storage.

### Lambda processes only a batch

The ingestion Lambda processes a limited number of rows at a time. A large
file becomes a sequence of small jobs:

```text
CSV file
  -> chunk 0
  -> chunk 1
  -> chunk 2
  -> chunk 3
  -> ...
```

Memory usage depends mostly on the configured batch size, not on the complete
CSV file size.

### SQS provides buffering and retries

SQS separates uploading from processing.

If many files are uploaded at once:

```text
S3 -> SQS queue -> Lambda workers
```

The messages wait in the queue until Lambda can process them. If a temporary
failure occurs, SQS retries the message. After repeated failures, the message
goes to a dead-letter queue for investigation.

### Multiple files can be processed concurrently

The prototype does not set `reservedConcurrency`. This keeps the configuration
simple and avoids reserving part of a small AWS account concurrency quota.
Lambda and the SQS event source can use the account's available unreserved
capacity.

This allows different upload jobs to progress in parallel. The database cursor
still prevents chunks from the same job from being processed out of order.

If the database needs protection later, a reserved concurrency value can be
added after checking the account's Lambda concurrency quota.

### One individual file is still processed sequentially

The current design intentionally processes chunks for one job in order:

```text
chunk 0 -> chunk 1 -> chunk 2 -> chunk 3
```

This makes resumability and duplicate handling easier to understand.

It means that increasing Lambda concurrency does not make one single CSV file
fully parallel. It mainly allows multiple files to be processed at once.

## 4. Current scalability settings

The important settings are in `serverless.yml` and the environment:

| Setting | Current value | Meaning |
| --- | ---: | --- |
| `INGESTION_BATCH_ROWS` | `1000` | Rows processed by one ingestion call |
| Ingestion Lambda concurrency | Account-managed | No reserved concurrency is configured |
| Ingestion Lambda timeout | `900` seconds | Maximum time for one Lambda invocation |
| Ingestion SQS visibility timeout | `5400` seconds | Time SQS hides a message while it is being processed |
| Matching Lambda concurrency | Account-managed | No reserved concurrency is configured |
| SQS retry count | `5` | Attempts before moving to a dead-letter queue |

These values are reasonable prototype defaults. They should be measured and
adjusted after observing real CSV sizes, row sizes, database performance, and
n8n response times.

## 5. What happens when something fails?

### Database temporarily unavailable

The ingestion Lambda raises an error. SQS keeps the message available for a
later retry.

The chunk is protected by database state, so a successful retry does not
create duplicate users.

### CSV has a bad row

The row is rejected and recorded in `crawl_errors`. Other valid rows in the
same batch continue to be processed.

### CSV has a bad header or is malformed

The job is marked `failed`, an extraction error is recorded, and retrying the
same bad file is not useful.

### The same S3 event arrives twice

The database checks the job and chunk state. A completed chunk is not processed
again.

### The same user appears twice

The database uniqueness constraint prevents a duplicate user for that job. The
duplicate is counted as rejected.

### n8n is unavailable

The webhook delivery remains unfinished. SQS retries the matching message. If
all attempts fail, the message goes to the matching dead-letter queue.

## 6. Important scalability limits

The design is efficient, but it is not unlimited.

### PostgreSQL is the main bottleneck

Every valid batch writes to PostgreSQL. As traffic increases, the database may
become the slowest part of the system.

Possible future improvements include:

- Use a managed PostgreSQL service with suitable connection limits.
- Tune the SQLAlchemy connection pool.
- Increase batch size after measuring performance.
- Add database monitoring.
- Consider a database proxy if Lambda connection counts become high.

These are future scaling steps, not requirements for the prototype.

### Larger batches are not always better

Increasing `INGESTION_BATCH_ROWS` reduces the number of SQS messages, but it
also increases:

- Lambda memory usage.
- Database transaction size.
- Time spent processing one message.
- Amount of work repeated after a failure.

The right value should be chosen using measurements rather than guessing.

### n8n may need its own scaling plan

The matching Lambda does not currently have a reserved concurrency value. This
keeps the free-tier configuration simple and avoids an account-quota deployment
failure.

If many jobs complete at the same time, the matching queue grows until the
Lambda can notify n8n. That is safe, but notifications may be delayed.

If n8n needs a strict concurrency limit later, one can be added after checking
the AWS account quota.

## 7. A simple example

Suppose the frontend uploads a file with 2,500 rows and the batch size is
1,000.

```text
Upload completes in S3
        |
        v
SQS message for chunk 0
        |
        v
Lambda reads rows 2-1001
Lambda saves the rows
Lambda sends continuation for chunk 1
        |
        v
Lambda reads rows 1002-2001
Lambda saves the rows
Lambda sends continuation for chunk 2
        |
        v
Lambda reads the remaining 499 rows
Lambda marks the job completed
Lambda creates the webhook delivery
        |
        v
Matching Lambda sends {"job_id": "..."} to n8n
```

If the Lambda fails while processing chunk 1, SQS retries chunk 1. Chunk 0 is
not repeated because the database already marks it as completed.

## 8. Code map

| File | Responsibility |
| --- | --- |
| `serverless.yml` | AWS resources, queues, Lambda functions, permissions, and settings |
| `pipeline/upload.py` | Creates the job and presigned S3 upload URL |
| `pipeline/csv_ingestion.py` | Reads CSV batches and performs minimal validation |
| `pipeline/ingestion.py` | Coordinates S3 reads, database inserts, retries, and continuation messages |
| `pipeline/matching.py` | Sends the completed-job payload to n8n |
| `database/models.py` | Defines jobs, users, chunks, errors, and webhook deliveries |
| `tests/test_csv_ingestion.py` | Tests CSV reading and validation behavior |
| `tests/test_pipeline_messages.py` | Tests S3/SQS message parsing |

## 9. Prototype notes

The current API endpoint does not have an authorizer configured. In this
prototype, anyone who can call the endpoint may be able to request an upload
URL. Before production use, the endpoint should be protected by the
application's authentication and authorization system.

The AWS resources are configured for the `ap-southeast-2` region in
`serverless.yml`. The AWS CLI default region does not control this Serverless
deployment.
