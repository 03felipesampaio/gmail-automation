from .define_action import define_action
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