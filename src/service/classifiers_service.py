import psycopg_pool
import logging
from googleapiclient.discovery import Resource
import asyncio

from gmail_api import gmail_requests
from repository import classifiers_repository, classifier_executions_repository
from . import messages_service, actions_service


logger = logging.getLogger("gmail_automation")


async def get_classifiers(pool: psycopg_pool.AsyncConnectionPool) -> list[dict]:
    return await classifiers_repository.read_all_classifiers(pool)


async def run_classifier_in_batch(
    pool: psycopg_pool.AsyncConnectionPool,
    execution: dict,
    classifier: dict,
    gmail_resource: Resource,
    userId: str,
) -> dict:
    classifier_execution = (
        await classifier_executions_repository.start_classifier_new_execution(
            pool, execution["execution_id"], classifier["classifier_id"]
        )
    )
    logger.info(
        f"Started classifier execution {classifier_execution['classifier_execution_id']} for classifier {classifier['classifier_id']}"
    )
    # # Run classifier
    try:
        # Get the classifier messages
        classifier_messages_ids = gmail_requests.query_messages(
            gmail_resource, userId, classifier["gmail_query"]
        )
        # Get the actions for the classifier
        classifier_actions = await actions_service.get_classifier_actions(
            pool, classifier["classifier_id"]
        )
        # Get the messages format to the classifier
        classifier_format = actions_service.get_message_format_for_classifier(
            [c["format"] for c in classifier_actions]
        )
        
        # Query the messages from Gmail on the specified format
        # The query_messages function already returns the messages in minimal format
        # so we don't need to query the messages again if the format is minimal
        if format != "minimal":
            messages = gmail_requests.get_messages_in_batch(
                gmail_resource, userId, classifier_messages_ids, classifier_format
            )
        else:
            messages = classifier_messages_ids
        
        # Build the messages handler
        classifier_handler = actions_service.build_message_handler(
            classifier_actions
        )
        
        # For each message, start a new message execution
        await messages_service.execute_messages_in_batch(
            pool, classifier_execution, messages, classifier_handler
        )
        classifier_execution_status = "SUCCESS"
    except Exception as e:
        classifier_execution_status = "ERROR"
        logger.error(f"Error running classifier {classifier['classifier_id']}: {e}")
    # # Finish classifier execution
    finally:
        classifier_execution_finished = (
            await classifier_executions_repository.finish_classifier_execution(
                pool,
                classifier_execution["classifier_execution_id"],
                classifier_execution_status,
            )
        )
        logger.info(
            f"Finished classifier execution {classifier_execution['classifier_execution_id']} for classifier {classifier['classifier_id']}"
        )

    return classifier_execution_finished


async def run_all_classifiers_in_batch(
    pool: psycopg_pool.AsyncConnectionPool,
    execution: dict,
    classifiers: list[dict],
    gmail_resource: Resource,
    userId: str,
) -> list[dict]:
    classifier_executions = []
    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(run_classifier_in_batch(pool, execution, classifier, gmail_resource, userId))
            for classifier in classifiers
        ]

    for task in tasks:
        classifier_executions.append(await task)

    return classifier_executions
