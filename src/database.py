from pymongo.collection import Collection
import pendulum
import logging

logger = logging.getLogger("gmail_automation")


def insert_last_history_id(history_collection: Collection, userId: str, history_id: str) -> None:
    """Inserts the last historyId in the database.

    Args:
        history_collection (Collection): MongoDB collection.
        userId (str): User ID.
        history_id (str): Last historyId.
    """
    logger.debug(f"Inserting last historyId {history_id} for user {userId}")
    history_collection.insert_one(
        {'date': pendulum.now(), 'historyId': history_id, 'userId': userId})


def get_last_history_id(history_collection: Collection, userId: str) -> str | None:
    """Gets the last historyId from the database.

    Args:
        history_collection (Collection): MongoDB collection.
        userId (str): User ID.

    Returns:
        str: Last historyId.
    """
    logger.debug(f"Getting last historyId for user {userId}")
    last_history = history_collection.find_one(
        {'userId': userId}, sort=[('date', -1)])
    
    if last_history is not None:
        last_history_id = last_history['historyId']
    else:
        logger.info(f"No historyId found for user {userId}")
        last_history_id = None    
    
    return last_history_id


def update_last_history_id(history_collection: Collection, userId: str, history_id: str) -> str:
    """Updates the last historyId in the database and returns the last one before.


    We do that because a new historyId returns a empty value when queried. So,
    in order to really get the changes between the last historyId and the new one,
    we must need to query the last historyId before the new one.

    Args:
        history_collection (Collection): MongoDB collection.
        userId (str): User ID.
        history_id (str): Last historyId.

    Returns:
        str: Last historyId before the new one.
    """
    last_history_id = get_last_history_id(history_collection, userId)

    insert_last_history_id(history_collection, userId, history_id)
    
    logger.info(f"Last historyId updated for user {userId}. Old: {last_history_id}, New: {history_id}")

    return last_history_id
