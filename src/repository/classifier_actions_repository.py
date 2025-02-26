import psycopg
import psycopg_pool
import logging
import uuid

logger = logging.getLogger("gmail_automation")


async def get_classifier_actions(
    pool: psycopg_pool.AsyncConnectionPool, classifier_id: int
) -> list[dict]:
    """Get classifier actions from the database."""
    query = """
    SELECT classifier_action_id, classifier_id, classifier_actions.action_name, classifier_actions.parameters, action_templates.format
    FROM classifier_actions
    JOIN action_templates ON classifier_actions.action_name = action_templates.action_name
    WHERE classifier_id = %s
    """

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, (classifier_id,))
            classifier_actions = await cursor.fetchall()
    return classifier_actions


async def assign_action_to_classifier(
    pool: psycopg_pool.AsyncConnectionPool,
    classifier_id: int,
    action_name: str,
    parameters: dict,
) -> dict:
    """Create a new classifier action in the database."""
    query = """
    INSERT INTO classifier_actions (classifier_id, action_name, parameters)
    VALUES (%s, %s, %s)
    RETURNING classifier_action_id, classifier_id, action_name, parameters
    """

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, (classifier_id, action_name, parameters))
            new_action = await cursor.fetchone()
    return new_action


async def update_classifier_action(
    pool: psycopg_pool.AsyncConnectionPool, classifier_action_id: int, parameters: dict
) -> dict:
    """Update an existing classifier action in the database."""
    query = """
    UPDATE classifier_actions
    SET parameters = %s
    WHERE classifier_action_id = %s
    RETURNING classifier_action_id, classifier_id, action_name, parameters
    """

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, (parameters, classifier_action_id))
            updated_action = await cursor.fetchone()
    return updated_action


async def delete_classifier_action(
    pool: psycopg_pool.AsyncConnectionPool, classifier_action_id: int
) -> dict:
    """Delete a classifier action from the database."""
    query = """
    DELETE FROM classifier_actions
    WHERE classifier_action_id = %s
    RETURNING classifier_action_id, classifier_id, action_name, parameters
    """

    async with pool.connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(query, (classifier_action_id,))
            deleted_action = await cursor.fetchone()
    return deleted_action
