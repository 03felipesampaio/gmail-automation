import psycopg
import uuid
from datetime import datetime


async def write_execution(cursor: psycopg.AsyncCursor, execution_uuid: uuid.UUID):
    """Write execution to database."""
    await cursor.execute(
        """
        INSERT INTO executions (execution_id)
        VALUES (%s)
        """,
        (execution_uuid,)
    )


async def write_execution_end_status_and_duration(cursor: psycopg.AsyncCursor, execution_uuid: uuid.UUID, status: str, finished_at: datetime):
    """Write execution end status and duration to database."""
    await cursor.execute(
        """
        UPDATE executions
        SET status = %s, finished_at = %s
        WHERE execution_id = %s
        """,
        (status, finished_at, execution_uuid)
    )