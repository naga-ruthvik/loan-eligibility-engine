"""Small, streaming CSV reader used by the ingestion Lambda.

The reader intentionally does only the validation needed by this prototype:
``user_id`` must exist, and numeric fields must contain numeric values when
they are provided. Everything else is optional and is stored as-is.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import BinaryIO


class CsvInputError(ValueError):
    """The file is not usable and should not be retried."""


@dataclass(frozen=True, slots=True)
class CsvChunk:
    """Rows plus the cursor needed to read the next part of the file."""

    rows: list[dict[str, object]]
    errors: list[dict[str, object]]
    next_byte_offset: int
    next_row_number: int
    has_more: bool
    headers: list[str]


class ByteTrackingReader:
    """Decode CSV lines while keeping the current byte position in S3."""

    def __init__(self, body: BinaryIO, start_offset: int) -> None:
        self.body = body
        self.position = start_offset
        self.first_line = start_offset == 0

    def __iter__(self) -> ByteTrackingReader:
        return self

    def __next__(self) -> str:
        raw_line = self.body.readline()
        if not raw_line:
            raise StopIteration

        self.position += len(raw_line)
        try:
            encoding = "utf-8-sig" if self.first_line else "utf-8"
            return raw_line.decode(encoding)
        except UnicodeDecodeError as exc:
            raise CsvInputError("CSV must be valid UTF-8") from exc
        finally:
            self.first_line = False


def parse_csv_chunk(
    body: BinaryIO,
    *,
    start_byte_offset: int,
    start_row_number: int,
    max_rows: int,
    initial_chunk: bool,
    headers: list[str] | None = None,
) -> CsvChunk:
    """Read at most ``max_rows`` rows without loading the whole file."""

    if max_rows <= 0:
        raise ValueError("max_rows must be greater than zero")

    line_reader = ByteTrackingReader(body, start_byte_offset)
    reader = csv.reader(line_reader, strict=True)

    if initial_chunk:
        try:
            headers = _read_headers(next(reader))
        except StopIteration as exc:
            raise CsvInputError("CSV file is empty") from exc
        row_number = 2
    else:
        row_number = start_row_number

    if not headers:
        raise CsvInputError("CSV continuation is missing headers")

    rows: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []

    for _ in range(max_rows):
        try:
            raw_row = next(reader)
        except StopIteration:
            return CsvChunk(
                rows, errors, line_reader.position, row_number, False, headers
            )
        except csv.Error as exc:
            raise CsvInputError(f"Malformed CSV near row {row_number}") from exc

        values, error = _parse_row(headers, raw_row, row_number)
        if values is not None:
            rows.append(values)
        if error is not None:
            errors.append(error)
        row_number += 1

    # We deliberately do not read one row ahead. The next SQS message starts
    # at the end of this batch. If the file ended exactly on a batch boundary,
    # the next invocation simply receives an empty chunk and completes safely.
    return CsvChunk(rows, errors, line_reader.position, row_number, True, headers)


def _read_headers(raw_headers: list[str]) -> list[str]:
    """Normalize headers and require only the field the database needs."""

    headers = [header.strip().lower() for header in raw_headers]
    if not headers or any(not header for header in headers):
        raise CsvInputError("CSV header row contains an empty column name")
    if len(headers) != len(set(headers)):
        raise CsvInputError("CSV header row contains duplicate column names")
    if "user_id" not in headers:
        raise CsvInputError("CSV must contain a user_id column")
    return headers


def _parse_row(
    headers: list[str], raw_row: list[str], row_number: int
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    """Convert one CSV row or return a small, readable rejection record."""

    if len(raw_row) > len(headers):
        return None, _error(
            row_number, None, "row contains more values than the header"
        )

    values = {
        header: value.strip()
        for header, value in zip(
            headers, raw_row + [""] * (len(headers) - len(raw_row))
        )
    }
    source_user_id = values.get("user_id", "") or None
    if not source_user_id:
        return None, _error(row_number, None, "user_id is required")

    errors: list[str] = []
    monthly_income = _number(values.get("monthly_income"), "monthly_income", errors)
    credit_score = _integer(values.get("credit_score"), "credit_score", errors)
    age = _integer(values.get("age"), "age", errors)

    if errors:
        return None, _error(row_number, source_user_id, "; ".join(errors))

    return (
        {
            "source_user_id": source_user_id,
            "full_name": values.get("name") or None,
            "email": values.get("email") or None,
            "monthly_income": monthly_income,
            "credit_score": credit_score,
            "employment_status": values.get("employment_status") or None,
            "age": age,
        },
        None,
    )


def _error(
    row_number: int, source_user_id: str | None, message: str
) -> dict[str, object]:
    return {
        "row_number": row_number,
        "source_user_id": source_user_id,
        "message": message,
    }


def _number(
    value: str | None, field_name: str, errors: list[str]
) -> Decimal | None:
    if not value:
        return None
    try:
        number = Decimal(value.replace(",", ""))
    except InvalidOperation:
        errors.append(f"{field_name} is not a valid number")
        return None
    if number < 0:
        errors.append(f"{field_name} cannot be negative")
    return number


def _integer(
    value: str | None, field_name: str, errors: list[str]
) -> int | None:
    if not value:
        return None
    try:
        number = int(value)
    except ValueError:
        errors.append(f"{field_name} is not a valid integer")
        return None
    if number < 0:
        errors.append(f"{field_name} cannot be negative")
    return number
