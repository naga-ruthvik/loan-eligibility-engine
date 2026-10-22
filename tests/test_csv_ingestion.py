from __future__ import annotations

import io
import unittest

from pipeline.csv_ingestion import CsvInputError, parse_csv_chunk


CSV = (
    "user_id,name,email,monthly_income,credit_score,employment_status,age\r\n"
    "u-1,Jane Doe,jane@example.com,1000.50,700,Employed,30\r\n"
    'u-2,"Doe, John",john@example.com,2000,720,Self-Employed,40\r\n'
    "u-3,Incomplete,,1000,bad-score,Employed,-1\r\n"
).encode("utf-8")


class CsvIngestionTests(unittest.TestCase):
    def test_chunked_parser_resumes_at_logical_csv_record_boundary(self) -> None:
        first = parse_csv_chunk(
            io.BytesIO(CSV),
            start_byte_offset=0,
            start_row_number=2,
            max_rows=2,
            initial_chunk=True,
        )

        self.assertEqual(
            [row["source_user_id"] for row in first.rows], ["u-1", "u-2"]
        )
        self.assertTrue(first.has_more)
        self.assertEqual(first.next_row_number, 4)

        second = parse_csv_chunk(
            io.BytesIO(CSV[first.next_byte_offset :]),
            start_byte_offset=first.next_byte_offset,
            start_row_number=first.next_row_number,
            max_rows=2,
            initial_chunk=False,
            headers=first.headers,
        )

        self.assertEqual(len(second.rows), 0)
        self.assertEqual(len(second.errors), 1)
        self.assertEqual(second.errors[0]["source_user_id"], "u-3")
        self.assertEqual(
            second.errors[0]["message"],
            "credit_score is not a valid integer; age cannot be negative",
        )
        self.assertFalse(second.has_more)

    def test_validation_accepts_missing_optional_values(self) -> None:
        chunk = parse_csv_chunk(
            io.BytesIO(b"user_id,name\nu-1,Name\n"),
            start_byte_offset=0,
            start_row_number=2,
            max_rows=10,
            initial_chunk=True,
        )

        self.assertEqual(chunk.rows[0]["source_user_id"], "u-1")
        self.assertIsNone(chunk.rows[0]["monthly_income"])
        self.assertEqual(chunk.errors, [])

    def test_missing_user_id_header_is_permanent(self) -> None:
        with self.assertRaises(CsvInputError):
            parse_csv_chunk(
                io.BytesIO(b"name,email\nName,user@example.com\n"),
                start_byte_offset=0,
                start_row_number=2,
                max_rows=10,
                initial_chunk=True,
            )


if __name__ == "__main__":
    unittest.main()
