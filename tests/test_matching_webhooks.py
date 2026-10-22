from __future__ import annotations

import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from pipeline import matching


class MatchingWebhookTests(unittest.TestCase):
    def test_workflow_c_is_called_only_after_workflow_b(self) -> None:
        delivery = SimpleNamespace(
            status="pending",
            locked_until=None,
            attempt_count=0,
            payload={"job_id": "job-123"},
            delivered_at=None,
            last_error=None,
            last_http_status=None,
        )
        session = SimpleNamespace(scalar=lambda statement: delivery)

        @contextmanager
        def fake_session_scope():
            yield session

        settings = SimpleNamespace(
            n8n_webhook_url="https://n8n.example/workflow-b",
            workflow_c_webhook_url="https://n8n.example/workflow-c",
            webhook_timeout_seconds=10,
            webhook_lock_seconds=600,
        )

        with (
            patch.object(matching, "session_scope", fake_session_scope),
            patch.object(matching, "get_pipeline_settings", return_value=settings),
            patch.object(
                matching,
                "_post_webhook",
                side_effect=[200, 202],
            ) as post_webhook,
        ):
            matching.process_delivery(uuid4())

        self.assertEqual(
            [call.args[1] for call in post_webhook.call_args_list],
            [settings.n8n_webhook_url, settings.workflow_c_webhook_url],
        )
        self.assertEqual(delivery.status, "sent")
        self.assertEqual(delivery.last_http_status, 202)


if __name__ == "__main__":
    unittest.main()
