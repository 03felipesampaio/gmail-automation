import psycopg_pool
import logging
import uuid

logger = logging.getLogger("gmail_automation")


async def start_classifier_new_execution(
    pool: psycopg_pool.AsyncConnectionPool, execution_id: uuid.UUID, classifier_id: int
) -> dict:
    """Start a new classifier execution on the database.

    Args:
        pool (psycopg_pool.AsyncConnectionPool): Async connection pool to acquire connections
        execution_id (uuid.UUID): Execution ID
        classifier_execution_id (uuid.UUID): Classifier execution ID

    Returns:
        dict: Classifier execution data
    """
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            classifier_execution_id = uuid.uuid4()
            try:
                await cursor.execute(
                    "INSERT INTO classifier_executions (execution_id, classifier_execution_id, classifier_id) VALUES (%s, %s, %s) RETURNING *",
                    (execution_id, classifier_execution_id, classifier_id),
                )
                result = await cursor.fetchone()
            except Exception as e:
                logger.error(
                    f"Error writing classifier execution: Classifier ID: {classifier_id}. Classifier execution ID: {classifier_execution_id}. Error: {e}"
                )
                raise e

            return result


async def finish_classifier_execution(
    pool: psycopg_pool.AsyncConnectionPool, classifier_execution_id: uuid.UUID, status: str
) -> None:
    """Finish a classifier execution on the database.

    Args:
        pool (psycopg_pool.AsyncConnectionPool): Async connection pool to acquire connections
        classifier_execution_id (uuid.UUID): Classifier execution ID
        status (str): Classifier execution status
    """
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            try:
                await cursor.execute(
                    "UPDATE classifier_executions SET finished_at = NOW(), status = %s WHERE classifier_execution_id = %s RETURNING *",
                    (status, classifier_execution_id),
                )
                classifier_execution = await cursor.fetchone()
            except Exception as e:
                logger.error(
                    f"Error finishing classifier execution: Classifier execution ID: {classifier_execution_id}. Error: {e}"
                )
                raise e
        await conn.commit()

    return classifier_execution