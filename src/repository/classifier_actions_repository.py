import psycopg
import psycopg_pool
import logging
import uuid

logger = logging.getLogger("gmail_automation")

async def get_classifier_actions(pool: psycopg_pool.AsyncConnectionPool, classifier_id: int) -> list[dict]:
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