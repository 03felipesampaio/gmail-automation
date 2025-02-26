import psycopg_pool
from typing import Any, Callable
from functools import partial

from repository import actions_repository, classifier_actions_repository
from actions import defined_actions
import logging

logger = logging.getLogger("gmail_automation")


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


def get_message_format_for_classifier(classifier_actions_formats: list[str]) -> str:
    """
    Get the minimal format required by the classifier actions to be executed.

    Ex.:
    - If the classifier actions formats are ['metadata', 'full'], the actions can only be executed with 'full'.
    - If the classifier actions formats are ['minimal', 'metadata'], the actions can only be executed with 'metadata'.
    """
    format_order = {"minimal": 0, "metadata": 1, "full": 2}
    return (
        max(classifier_actions_formats, key=lambda fmt: format_order[fmt])
        if classifier_actions_formats
        else "minimal"
    )


async def get_classifier_actions(
    pool: psycopg_pool.AsyncConnectionPool, classifier_id: int
) -> list[str]:
    # query the database for the classifier actions
    classifier_actions = await classifier_actions_repository.get_classifier_actions(
        pool, classifier_id
    )
    return classifier_actions


def build_message_handler(
    classifier_actions: list[dict],
) -> Callable[[dict], None]:
    """
    Create and setup the function to be executed by the classifiers.

    All classifier actions should behave like a callback function, having only have one parameter, which is the message.
    So in order to execute the actions, a function with the context of the classifier actions and parameters should be created.

    Args:
        classifier_actions (list): List of classifier actions

    Returns:
        Callable: Function to execute the classifier actions
    """
    partial_functions = []

    for classifier_action in classifier_actions:
        action_function = defined_actions.get(classifier_action["action_name"], None)
        if action_function is None:
            logger.error(
                f"There was not any action found with the name '{classifier_action['action_name']}' on action list."
            )
            raise KeyError(
                f"There was not any action found with the name '{classifier_action['action_name']}' on action list."
            )
        partial_functions.append(
            partial(action_function["action"], **classifier_action["parameters"])
        )

    def execute_actions(message: dict) -> None:
        for partial_function in partial_functions:
            partial_function(message=message)

    return execute_actions


async def get_all_actions(pool: psycopg_pool.AsyncConnectionPool) -> list[dict]:
    """Get all actions from the database."""
    actions = await actions_repository.get_all_actions(pool)

    for action in actions:
        action_parameters = await actions_repository.get_action_parameters(
            pool, action["action_name"]
        )
        action["parameters"] = action_parameters

    return actions


async def get_action_by_name(pool: psycopg_pool.AsyncConnectionPool, action_name: str) -> dict:
    """Get an action by its name."""
    action = await actions_repository.get_action_by_name(pool, action_name)
    action_parameters = await actions_repository.get_action_parameters(
        pool, action["action_name"]
    )
    action["parameters"] = action_parameters

    return action


async def assign_action_to_classifier(
    pool: psycopg_pool.AsyncConnectionPool,
    classifier_id: int,
    action_name: str,
    parameters: dict,
) -> dict:
    """Create a new classifier action in the database."""
    return await classifier_actions_repository.assign_action_to_classifier(
        pool, classifier_id, action_name, parameters
    )


async def update_classifier_action(
    pool: psycopg_pool.AsyncConnectionPool, classifier_action_id: int, parameters: dict
) -> dict:
    """Update an existing classifier action in the database."""
    return await classifier_actions_repository.update_classifier_action(
        pool, classifier_action_id, parameters
    )


async def delete_classifier_action(
    pool: psycopg_pool.AsyncConnectionPool, classifier_action_id: int
) -> dict:
    """Delete a classifier action from the database."""
    return await classifier_actions_repository.delete_classifier_action(
        pool, classifier_action_id
    )
