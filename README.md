<!--
title: 'AWS Simple HTTP Endpoint example in Python'
description: 'This template demonstrates how to make a simple HTTP API with Python running on AWS Lambda and API Gateway using the Serverless Framework.'
layout: Doc
framework: v4
platform: AWS
language: python
authorLink: 'https://github.com/serverless'
authorName: 'Serverless, Inc.'
authorAvatar: 'https://avatars1.githubusercontent.com/u/13742415?s=200&v=4'
-->

# Serverless Framework Python HTTP API on AWS

This template demonstrates how to make a simple HTTP API with Python running on AWS Lambda and API Gateway using the Serverless Framework.

The project now includes a PostgreSQL-ready SQLAlchemy database layer. See
the [Database layer](#database-layer) section for configuration and usage.

Lambda uses the shared `clickpe-dependencies` layer for Python dependencies.
The functions run on Python 3.12 and x86_64 so the packaged PostgreSQL driver
matches the Lambda runtime.

## Usage

### CSV upload interface

The project includes a small browser interface under `frontend/`. It requests a
presigned upload URL from the API and uploads the selected CSV directly to S3.
The CSV file does not pass through the browser's API request or Lambda.

After deploying the Serverless application, start a simple local web server
from the project root:

```bash
python -m http.server 8080 --directory frontend
```

Open `http://localhost:8080`, choose a `.csv` file, and click **Upload CSV**.
The frontend is already configured to use the deployed upload API.

The interface displays the resulting `job_id` after S3 accepts the upload.
The ingestion pipeline then processes the file in the background.

After the matching workflow completes, Workflow B should call the Workflow C
n8n webhook with the same `job_id`:

```text
POST https://naga-ruthvik.app.n8n.cloud/webhook/d4fbedc6-3ae7-4e5b-811a-a05382774200
Content-Type: application/json
```

```json
{
  "job_id": "..."
}
```

The deployed matching Lambda now performs this handoff itself: it calls
`N8N_WEBHOOK_URL` first, waits for a successful response, and then calls
`N8N_WORKFLOW_C_WEBHOOK_URL` with the same body. Configure Workflow B to return
its webhook response only after its last matching node has finished.

### Deployment

```
serverless deploy
```

After deploying, you should see output similar to:

```
Deploying "aws-python-http-api" to stage "dev" (us-east-1)

✔ Service deployed to stack aws-python-http-api-dev (85s)

endpoint: GET - https://6ewcye3q4d.execute-api.us-east-1.amazonaws.com/
functions:
  hello: aws-python-http-api-dev-hello (2.3 kB)
```

_Note_: In current form, after deployment, your API is public and can be invoked by anyone. For production deployments, you might want to configure an authorizer. For details on how to do that, refer to [http event docs](https://www.serverless.com/framework/docs/providers/aws/events/apigateway/).

### Invocation

After successful deployment, you can call the created application via HTTP:

```
curl https://xxxxxxx.execute-api.us-east-1.amazonaws.com/
```

Which should result in response similar to the following (removed `input` content for brevity):

```json
{
  "message": "Go Serverless v4.0! Your function executed successfully!"
}
```

### Local development

You can invoke your function locally by using the following command:

```
serverless invoke local --function hello
```

Which should result in response similar to the following:

```json
{
  "statusCode": 200,
  "body": "{\n  \"message\": \"Go Serverless v4.0! Your function executed successfully!\"}"
}
```

Alternatively, it is also possible to emulate API Gateway and Lambda locally by using `serverless-offline` plugin. In order to do that, execute the following command:

```
serverless plugin install -n serverless-offline
```

It will add the `serverless-offline` plugin to `devDependencies` in `package.json` file as well as will add it to `plugins` in `serverless.yml`.

After installation, you can start local emulation with:

```
serverless offline
```

To learn more about the capabilities of `serverless-offline`, please refer to its [GitHub repository](https://github.com/dherault/serverless-offline).

### Bundling dependencies

In case you would like to include 3rd party dependencies, no plugin install is needed — the Serverless Framework bundles Python dependencies natively. Simply add your dependencies to a `requirements.txt` file (`Pipfile` and `pyproject.toml` are also supported but require additional configuration) and, if you don't already have one, add a `custom.pythonRequirements` block to your `serverless.yml` to activate and configure the built-in packaging, for example:

```yml
custom:
  pythonRequirements: {}
```

They will then be automatically injected into the Lambda package during the build process.

## Database layer

The project includes a small SQLAlchemy 2.x database layer under `database/`:

- `database/config.py` loads `DATABASE_URL` and optional pool settings from the
  environment. For local development it also loads `.env` without overriding
  values already present in the process environment.
- `database/models.py` defines the shared SQLAlchemy `Base`, the six ORM
  models, relationships, PostgreSQL constraints, and migration-safe naming
  conventions.
- `database/session.py` creates one process-wide engine and session factory,
  then provides transaction-scoped sessions for individual requests.

Install the pinned dependencies and create a local environment file:

```bash
python -m pip install -r requirements.lock
cp .env.example .env
```

Set `DATABASE_URL` in `.env` to the PostgreSQL connection string for the local
database. The application normalizes a `postgresql://` value to the explicit
`postgresql+psycopg://` SQLAlchemy driver URL. In AWS, provide `DATABASE_URL`
through the deployment environment or a managed secret; do not deploy the
local `.env` file.

Use a session as a short-lived transaction boundary inside an API handler or
service function:

```python
from database.session import session_scope


def perform_database_work():
    with session_scope() as session:
        # Query or mutate ORM models here.
        ...
```

The context manager commits successful work, rolls back failed work, and
closes the session. Do not keep a `Session` globally, create an engine per
request, or dispose the shared engine after every Lambda invocation.

Alembic is configured under `alembic/` with migrations for the six tables,
including `crawl_errors` for recording crawl failures by source URL and
processing stage. Run migrations as a controlled deployment step, not during
Lambda cold starts or API requests:

```bash
alembic upgrade head
```

The migrations use PostgreSQL-specific types and constraints because the
application database is PostgreSQL. Migration `0004_add_uuid_server_defaults`
also enables PostgreSQL-side UUID generation so raw SQL clients can omit UUID
primary keys. The ORM still retains Python-side UUID defaults for SQLAlchemy
inserts.

The ORM currently stores one current match per user/product pair and enforces
one unique `(lender_name, product_name)` pair for loan products. It cascades
dependent records when a parent is deleted. If duplicate loan products already
exist, migration `0003_loan_product_name_unique` stops before changing the
schema so related matches can be reconciled safely first.
The notification service, not the ORM layer, is responsible for routing emails
to the configured test inbox.
