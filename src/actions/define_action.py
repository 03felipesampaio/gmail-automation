from typing import Callable
import inspect
import logging

logger = logging.getLogger("gmail_automation")

actions = {}


def parse_parameters(parameters: inspect.Signature) -> list[dict]:
    """Parse parameters from the action signature.

    Args:
        parameters (inspect.Signature): Signature of the action
    
    Returns:
        list: Action parameters
    """
    parsed_parameters = []
    for param_name, param in parameters.parameters.items():
        parsed_parameter = {
            'parameter_name': param_name,
            'parameter_type': None if param.annotation == inspect.Parameter.empty else param.annotation,
            'parameter_is_nullable': False,
            'parameter_default': None if param.default == inspect.Parameter.empty else param.default,
        }
        parsed_parameters.append(parsed_parameter)
        
    return parsed_parameters


def define_action(format: str):
    """Decorator to define an action to be executed by the classifiers.

    Args:
        format (str): Gmail message format. The format filters the amount of data from the Gmail message.
    """

    def decorator(func: Callable):
        actions[func.__name__] = {
            "action_name": func.__name__,
            "format": format,
            "parameters": parse_parameters(inspect.signature(func)),
        }
        logger.info(f"Found action: {func.__name__}")
        return func

    return decorator
