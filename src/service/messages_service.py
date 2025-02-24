from typing import Callable
import uuid
from repository import classifier_message_execution_repository
import psycopg_pool
import logging


logger = logging.getLogger("gmail_automation")


async def start_message_execution(
    pool: psycopg_pool.AsyncConnectionPool, message_id: str, classifier_execution: dict
) -> dict:
    message_execution = await classifier_message_execution_repository.start_message_execution(pool, message_id, classifier_execution)
    return message_execution


async def finish_message_execution(
    pool: psycopg_pool.AsyncConnectionPool, classifier_execution_id: uuid.UUID
) -> dict:
    message_execution = await classifier_message_execution_repository.finish_message_execution(pool, classifier_execution_id)
    return message_execution


async def execute_messages_in_batch(
    pool: psycopg_pool.AsyncConnectionPool,
    classifier_execution: dict,
    messages: list[dict],
    message_handler: Callable[[dict], None]
) -> list[dict]:
    """
    Execute messages in batch
    """
    classifier_messages_executions = []
    for message in messages:
        try:
            message_execution = await classifier_message_execution_repository.start_message_execution(
                pool, message["id"], classifier_execution
            )
            logger.info(
                f"Started message execution for message {message['id']} and classifier {classifier_execution['classifier_id']}"
            )
            await message_handler(message)
            execution_status = "SUCCESS"
        except Exception as e:
            logger.error(
                f"Error starting message execution for message {message['id']} and classifier {classifier_execution['classifier_id']}: {e}"
            )
            execution_status = "ERROR"
        finally:
            message_execution_finished = await classifier_message_execution_repository.finish_message_execution(
                pool, message_execution, execution_status
            )
            classifier_messages_executions.append(message_execution_finished)

    return classifier_messages_executions