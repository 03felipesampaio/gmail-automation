import psycopg_pool
from repository import actions_repository

# filepath: /home/alan/projects/gmail-automation/src/service/actions_service.py


async def load_actions(pool: psycopg_pool.AsyncConnectionPool, actions: dict) -> None:
    """Load actions to the database.

    Args:
        pool (psycopg.AsyncConnectionPool): Database connection pool
        actions (dict): Dictionary of actions

    Returns:
        None
    """
    for action in actions.values():
        inserted_action = await actions_repository.add_action(pool, action)
        inserted_parameters = await actions_repository.add_action_parameters(
            pool, action["action_name"], action["parameters"]
        )
