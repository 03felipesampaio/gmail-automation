import psycopg
import psycopg_pool
import uuid
from datetime import datetime


async def start_execution(poll: psycopg_pool.AsyncConnectionPool):
    """Start a new execution.
    
    * Commit: True
    
    Args:
        poll (psycopg_pool.AsyncConnectionPool): Database connection pool
        
    Returns:
        dict: Execution
    """
    execution_uuid = uuid.uuid4()
    async with poll.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO executions (execution_id)
                VALUES (%s)
                RETURNING *
                """,
                (execution_uuid,)
            )
            execution = await cursor.fetchone()
        await conn.commit()
        
    return execution


async def finish_execution_end_status_and_duration(poll: psycopg_pool.AsyncConnectionPool, execution_uuid: uuid.UUID, status: str):
    """Write execution end status and duration to database."""
    async with poll.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE executions
                SET status = %s, finished_at = CURRENT_TIMESTAMP
                WHERE execution_id = %s
                RETURNING *
                """,
                (status, execution_uuid)
            )
            execution = await cursor.fetchone()
        await conn.commit()
    
    return execution