from ..define_action import define_action
from pathlib import Path
import json


@define_action(format="full")
def save_to_json(path: str, message: dict) -> dict:
    """Saves Gmail message to a JSON file.
    
    Args:
        message (Message): Gmail message
    """
    dir_messages = Path(path)
    dir_messages.mkdir(parents=True, exist_ok=True)
    
    with open(dir_messages / f"{message['id']}.json", "w") as f:
        json.dump(message, f, ensure_ascii=False, indent=4)
    
    return message


@define_action(format="minimal")
def manage_message_labels(message: dict, add_labels: list[str], remove_labels: list[str], gmail_resource, userId: str) -> dict:
    """Adds a label to a Gmail message.
    
    Args:
        label (str): Label to be added
        message (Message): Gmail message
    """
    # message["labelIds"].append(label)
    
    return message