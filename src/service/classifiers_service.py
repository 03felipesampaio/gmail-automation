import psycopg_pool
import logging
from repository import classifiers_repository


logger = logging.getLogger("gmail_automation")


async def get_classifiers(pool: psycopg_pool.AsyncConnectionPool) -> list[dict]:
    return await classifiers_repository.read_all_classifiers(pool)