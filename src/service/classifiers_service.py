import psycopg_pool
import logging
from repository import classifiers_repository, classifier_executions_repository
import asyncio


logger = logging.getLogger("gmail_automation")


async def get_classifiers(pool: psycopg_pool.AsyncConnectionPool) -> list[dict]:
    return await classifiers_repository.read_all_classifiers(pool)


async def run_classifier_in_batch(
    pool: psycopg_pool.AsyncConnectionPool, execution: dict, classifier: dict
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
    pool: psycopg_pool.AsyncConnectionPool, execution: dict, classifiers: list[dict]
) -> list[dict]:
    classifier_executions = []
    async with asyncio.TaskGroup() as tg:
        tasks = [tg.create_task(run_classifier_in_batch(pool, execution, classifier)) for classifier in classifiers]
    
    for task in tasks:
        classifier_executions.append(await task)
    
    return classifier_executions
