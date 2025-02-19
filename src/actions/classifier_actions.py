from . import defined_actions
from typing import Callable, Any
from functools import partial
import logging

logger = logging.getLogger("gmail_automation")


def get_message_format_for_classifier(classifier_actions_formats: list[str]) -> str:
    """
    Get the minimal format required by the classifier actions to be executed.
    
    Ex.:
    - If the classifier actions formats are ['metadata', 'full'], the actions can only be executed with 'full'.
    - If the classifier actions formats are ['minimal', 'metadata'], the actions can only be executed with 'metadata'.
    """
    format_order = {"minimal": 0, "metadata": 1, "full": 2}
    return max(classifier_actions_formats, key=lambda fmt: format_order[fmt]) if classifier_actions_formats else "minimal"


def get_action_parameter_from_parameter_dict(action_parameter: dict) -> tuple[str, Any]:
    """
    Get the action parameter from the given parameter dictionary.
    """
    return action_parameter["parameter_name"], action_parameter["parameter_default"]


def build_classifier_action_handler(
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
        partial_functions.append(partial(action_function["action"], **classifier_action["parameters"]))

    def execute_actions(message: dict) -> None:
        for partial_function in partial_functions:
            partial_function(message=message)

    return execute_actions
