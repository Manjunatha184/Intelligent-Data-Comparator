from __future__ import annotations

import os
import pickle
import sqlite3
from typing import Any


class SQLiteEvidenceStore:
    """
    Temporary disk-backed store for run-scoped intermediate evidence.

    PostgreSQL remains the persistent storage layer. This store exists
    only to let execution pass detailed evidence between dependent
    tasks without keeping every item in memory.
    """

    def __init__(
        self,
        path: str,
        run_id: str,
        task_id: str,
    ):
        self.path = path
        self.run_id = run_id
        self.task_id = task_id
        self._initialize()

    @classmethod
    def create(
        cls,
        directory: str,
        run_id: str,
        task_id: str,
    ) -> "SQLiteEvidenceStore":

        os.makedirs(
            directory,
            exist_ok=True,
        )

        return cls(
            path=os.path.join(
                directory,
                "intermediate_evidence.sqlite3",
            ),
            run_id=run_id,
            task_id=task_id,
        )

    @classmethod
    def from_ref(
        cls,
        ref: dict[str, Any],
    ) -> "SQLiteEvidenceStore":

        if ref.get("storage") != "SQLITE":
            raise ValueError(
                "Unsupported evidence storage reference"
            )

        return cls(
            path=ref["path"],
            run_id=ref["run_id"],
            task_id=ref["task_id"],
        )

    def _initialize(self) -> None:

        with sqlite3.connect(self.path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS evidence_items (
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    evidence_key TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload BLOB NOT NULL,
                    PRIMARY KEY (
                        run_id,
                        task_id,
                        evidence_key,
                        ordinal
                    )
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                ix_evidence_items_lookup
                ON evidence_items (
                    run_id,
                    task_id,
                    evidence_key,
                    ordinal
                )
                """
            )

    def append_items(
        self,
        evidence_key: str,
        items: list[dict[str, Any]],
    ) -> None:

        if not items:
            return

        start = self.count_items(
            evidence_key
        )

        rows = [
            (
                self.run_id,
                self.task_id,
                evidence_key,
                start + index,
                pickle.dumps(item),
            )
            for index, item in enumerate(items)
        ]

        with sqlite3.connect(self.path) as connection:
            connection.executemany(
                """
                INSERT INTO evidence_items (
                    run_id,
                    task_id,
                    evidence_key,
                    ordinal,
                    payload
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

    def count_items(
        self,
        evidence_key: str,
    ) -> int:

        with sqlite3.connect(self.path) as connection:
            row = connection.execute(
                """
                SELECT COUNT(*)
                FROM evidence_items
                WHERE run_id = ?
                AND task_id = ?
                AND evidence_key = ?
                """,
                (
                    self.run_id,
                    self.task_id,
                    evidence_key,
                ),
            ).fetchone()

        return int(row[0] or 0)

    def iter_batches(
        self,
        evidence_key: str,
        batch_size: int,
    ):

        if batch_size <= 0:
            raise ValueError(
                "batch_size must be greater than zero"
            )

        offset = 0

        while True:
            with sqlite3.connect(self.path) as connection:
                rows = connection.execute(
                    """
                    SELECT payload
                    FROM evidence_items
                    WHERE run_id = ?
                    AND task_id = ?
                    AND evidence_key = ?
                    ORDER BY ordinal
                    LIMIT ?
                    OFFSET ?
                    """,
                    (
                        self.run_id,
                        self.task_id,
                        evidence_key,
                        batch_size,
                        offset,
                    ),
                ).fetchall()

            if not rows:
                break

            yield [
                pickle.loads(row[0])
                for row in rows
            ]

            offset += len(rows)

    def ref(
        self,
        evidence_key: str,
    ) -> dict[str, Any]:

        return {
            "storage": "SQLITE",
            "path": self.path,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "evidence_key": evidence_key,
        }

    @staticmethod
    def cleanup_ref(
        ref: dict[str, Any],
    ) -> None:

        path = ref.get("path")

        if path and os.path.exists(path):
            os.remove(path)

        directory = os.path.dirname(path) if path else ""

        if directory and os.path.isdir(directory):
            try:
                os.rmdir(directory)
            except OSError:
                pass
