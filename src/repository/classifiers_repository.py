import psycopg_pool
import logging

logger = logging.getLogger("gmail_automation")


async def read_all_classifiers(pool: psycopg_pool.AsyncConnectionPool) -> list[dict]:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            # Execute a query
            await cursor.execute("SELECT * FROM classifiers")

            # Retrieve query results
            classifiers = await cursor.fetchall()

    logger.info(f"{len(classifiers)} classifiers retrieved from database")

    return classifiers


async def read_new_classifiers(pool: psycopg_pool.AsyncConnectionPool) -> bool:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            # Execute a query
            await cursor.execute(
                "SELECT * FROM classifiers WHERE name = %s",
            )

            # Retrieve query results
            classifier = await cursor.fetchone()

    return classifier is None
