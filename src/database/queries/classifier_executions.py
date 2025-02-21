import psycopg
import logging
import uuid

logger = logging.getLogger("gmail_automation")


async def start_classifier_new_execution(
    cursor: psycopg.AsyncCursor, execution_id: uuid.UUID, classifier_id: int
) -> dict:
    """Start a new classifier execution on the database.

    Args:
        cursor (psycopg.AsyncCursor): Async cursor to execute queries
        execution_id (uuid.UUID): Execution ID
        classifier_execution_id (uuid.UUID): Classifier execution ID

    Returns:
        dict: Classifier execution data
    """
    # Execute a query
    classifier_execution_id = uuid.uuid4()
    # logger.info(
    #     f"Writing classifier execution: Classifier ID: {classifier_id}. Classifier execution ID: {classifier_execution_id}"
    # )

    await cursor.execute(
        "INSERT INTO classifier_executions (execution_id, classifier_execution_id, classifier_id) VALUES (%s, %s, %s) RETURNING *",
        (execution_id, classifier_execution_id, classifier_id),
    )

    try:
        result = await cursor.fetchone()
    except Exception as e:
        logger.error(
            f"Error writing classifier execution: Classifier ID: {classifier_id}. Classifier execution ID: {classifier_execution_id}. Error: {e}"
        )
        raise e

    # logger.info(
    #     f"Started new classifier execution: Classifier ID: {classifier_id}. Classifier execution ID: {classifier_execution_id}"
    # )

    return result


async def finish_classifier_execution(
    cursor: psycopg.AsyncCursor, classifier_execution_id: uuid.UUID, status: str
) -> None:
    """Finish a classifier execution on the database.

    Args:
        cursor (psycopg.AsyncCursor): Async cursor to execute queries
        classifier_execution_id (uuid.UUID): Classifier execution ID
        status (str): Classifier execution status
    """
    # Execute a query

    try:
        await cursor.execute(
            "UPDATE classifier_executions SET finished_at = NOW(), status = %s WHERE classifier_execution_id = %s",
            (status, classifier_execution_id),
        )
    except Exception as e:
        logger.error(
            f"Error finishing classifier execution: Classifier execution ID: {classifier_execution_id}. Error: {e}"
        )
        raise e

    # logger.info(
    #     f"Finished classifier execution: Classifier execution ID: {classifier_execution_id}. Status: {status}"
    # )
