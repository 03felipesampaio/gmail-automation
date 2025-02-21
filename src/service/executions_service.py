import psycopg_pool
from repository import executions_repository
import logging

logger = logging.getLogger("gmail_automation")

async def start_new_execution(pool: psycopg_pool.AsyncConnectionPool) -> dict:
    """
    Start a new execution.
    """
    execution = await executions_repository.start_execution(pool)
    
    return execution


async def finish_execution(pool: psycopg_pool.AsyncConnectionPool, execution_uuid: str, status: str) -> dict:
    """
    Finish
    """
    execution = await executions_repository.finish_execution_end_status_and_duration(pool, execution_uuid, status)
    
    return execution