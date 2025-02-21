import psycopg
import logging

logger = logging.getLogger("gmail_automation")
# from psycopg2.extras import RealDictCursor

async def read_all_classifiers(cursor: psycopg.AsyncCursor) -> list[dict]:
    # Execute a query
    await cursor.execute("SELECT * FROM classifiers")

    # Retrieve query results
    classifiers = await cursor.fetchall()
    
    logger.info(f"{len(classifiers)} classifiers retrived from database")

    return classifiers


async def read_new_classifiers(cursor: psycopg.AsyncCursor) -> bool:
    # Execute a query
    cursor.execute(
        "SELECT * FROM classifiers WHERE name = %s",
    )

    # Retrieve query results
    classifier = cursor.fetchone()

    return classifier is None