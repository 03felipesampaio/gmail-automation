import psycopg_pool
from googleapiclient.discovery import Resource
from repository import executions_repository
import logging

from . import classifiers_service

logger = logging.getLogger("gmail_automation")


async def start_new_execution(pool: psycopg_pool.AsyncConnectionPool) -> dict:
    """
    Start a new execution.
    """
    execution = await executions_repository.start_execution(pool)

    return execution


async def finish_execution(
    pool: psycopg_pool.AsyncConnectionPool, execution_uuid: str, status: str
) -> dict:
    """
    Finish
    """
    execution = await executions_repository.finish_execution_end_status_and_duration(
        pool, execution_uuid, status
    )

    return execution


def get_execution_status_from_classifiers_executions(
    classifiers_executions: list[dict],
) -> str:
    """
    Get the execution status from the classifiers executions
    """
    if all(execution["status"] == "SUCCESS" for execution in classifiers_executions):
        return "SUCCESS"
    elif any(execution["status"] == "SUCCESS" for execution in classifiers_executions):
        return "PARTIAL SUCCESS"
    else:
        return "ERROR"


async def run_in_batch(
    pool: psycopg_pool.AsyncConnectionPool, gmail_resource: Resource, userId: str
) -> dict:
    """
    Run classifiers in batch
    """
    execution = await start_new_execution(pool)
    logger.info(f"Started execution {execution['execution_id']}")

    classifiers = await classifiers_service.get_classifiers(pool)
    logger.info(f"Retrieved {len(classifiers)} classifiers")

    classifiers_executions = await classifiers_service.run_all_classifiers_in_batch(
        pool, execution, classifiers, gmail_resource, userId
    )
    logger.info(f"Executed {len(classifiers_executions)} classifiers")

    execution_status = get_execution_status_from_classifiers_executions(
        classifiers_executions
    )
    execution_finished = await finish_execution(
        pool, execution["execution_id"], execution_status
    )
    logger.info(f"Finished execution {execution['execution_id']} with status '{execution_status}'")

    return execution_finished
