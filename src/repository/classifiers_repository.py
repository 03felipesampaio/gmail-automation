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


async def get_classifier_by_id(
    pool: psycopg_pool.AsyncConnectionPool, classifier_id: int
) -> dict:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            # Execute a query
            await cursor.execute(
                "SELECT * FROM classifiers WHERE classifier_id = %s", (classifier_id,)
            )

            # Retrieve query results
            classifier = await cursor.fetchone()

    logger.info(f"Classifier with ID '{classifier_id}' retrieved from database")

    return classifier


async def create_classifier(
    pool: psycopg_pool.AsyncConnectionPool, classifier_name: str, gmail_query: str
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                INSERT INTO classifiers (classifier_name, gmail_query)
                VALUES (%s, %s)
                """,
                (classifier_name, gmail_query),
            )
    logger.info(f"Classifier '{classifier_name}' created successfully")


async def update_classifier(
    pool: psycopg_pool.AsyncConnectionPool,
    classifier_id: int,
    classifier_name: str,
    gmail_query: str,
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                UPDATE classifiers
                SET classifier_name = %s, gmail_query = %s
                WHERE id = %s
                """,
                (classifier_name, gmail_query, classifier_id),
            )
    logger.info(f"Classifier with ID '{classifier_id}' updated successfully")


async def delete_classifier(
    pool: psycopg_pool.AsyncConnectionPool, classifier_id: int
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(
                """
                DELETE FROM classifiers
                WHERE id = %s
                """,
                (classifier_id,),
            )
    logger.info(f"Classifier with ID '{classifier_id}' deleted successfully")
