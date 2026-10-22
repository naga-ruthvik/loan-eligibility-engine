from __future__ import annotations

import json
import unittest
from uuid import UUID

from pipeline.ingestion import PermanentIngestionError, parse_work_item


class PipelineMessageTests(unittest.TestCase):
    def test_s3_notification_extracts_job_and_decodes_key(self) -> None:
        job_id = "12345678-1234-5678-1234-567812345678"
        body = json.dumps(
            {
                "Records": [
                    {
                        "s3": {
                            "bucket": {"name": "bucket"},
                            "object": {
                                "key": f"incoming/{job_id}/users+file.csv",
                                "versionId": "version-1",
                            },
                        }
                    }
                ]
            }
        )

        item = parse_work_item(body)

        self.assertEqual(item.job_id, UUID(job_id))
        self.assertEqual(item.key, f"incoming/{job_id}/users file.csv")
        self.assertEqual(item.version_id, "version-1")
        self.assertTrue(item.initial_chunk)

    def test_continuation_requires_all_cursor_fields(self) -> None:
        with self.assertRaises(PermanentIngestionError):
            parse_work_item(json.dumps({"job_id": "not-a-uuid"}))


if __name__ == "__main__":
    unittest.main()
