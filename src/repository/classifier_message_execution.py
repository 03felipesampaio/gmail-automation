import psycopg
import logging
import uuid

logger = logging.getLogger("gmail_automation")


async def start_message_new_execution(
    cursor: psycopg.AsyncCursor, message_id: str, classifier_execution: dict
) -> dict:
    # Execute a query
    # message_execution_id = uuid.uuid4()
    logger.info(
        f"Starting message execution: Message ID: {message_id}. Classifier execution ID: {classifier_execution['classifier_execution_id']}. Classifier ID: {classifier_execution['classifier_id']}"
    )

    await cursor.execute(
        "INSERT INTO classifier_messages_execution (message_id, classifier_execution_id) VALUES (%s, %s) RETURNING *",
        (message_id, classifier_execution["classifier_execution_id"]),
    )

    try:
        result = await cursor.fetchone()
    except Exception as e:
        logger.error(
            f"Error writing message execution: Message ID: {message_id}. Classifier execution ID: {classifier_execution['classifier_execution_id']}. Classifier ID: {classifier_execution['classifier_id']}. Error: {e}"
        )
        raise e

    return result


async def finish_message_execution(
    cursor: psycopg.AsyncCursor, message_execution: dict, status: str
) -> dict:
    """Finish a message execution on the database.
    
    Args:
        cursor (psycopg.AsyncCursor): Async cursor to execute queries
        message_execution (dict): Message execution data
        status (str): Message execution status
    
    Returns:
        dict: Message execution data
    """
    # Execute a query

    await cursor.execute(
        "UPDATE classifier_messages_execution SET finished_at = NOW(), status = %s WHERE message_id = %s AND classifier_execution_id = %s RETURNING *",
        (
            status,
            message_execution["message_id"],
            message_execution["classifier_execution_id"],
        ),
    )
    
    message_execution_updated = await cursor.fetchone()

    logger.info(
        f"Finished message execution: Message execution ID: Message ID: {message_execution['message_id']}. Classifier execution ID: {message_execution['classifier_execution_id']}. Status: {status}"
    )
    
    return message_execution_updated
