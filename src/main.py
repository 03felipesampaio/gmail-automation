# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from googleapiclient.discovery import Resource
from google.cloud import pubsub_v1

# TODO Move all this credentials logic to a separated file
from classfiers import USER_CLASSFIERS, GMAIL_SERVICE, MONGO_DATABASE, CLOUD_STORAGE_CLIENT

# MongoDB libs
from pymongo.collection import Collection

# Date and time libs
import pendulum

# Async lib
import asyncio

# Environment variables
from dotenv import load_dotenv

# Logging
import logging.config
import os
from pathlib import Path
import json
import atexit
import functools
from pprint import pprint

from gmail import GmailClassifier, GmailMessage
import gmail
import pubsub
import database


# Load environment variables
load_dotenv(".env")


logger = logging.getLogger("gmail_automation")


def setup_logging():
    log_dir_path = Path(__file__).parent.parent / "logs"
    log_dir_path.mkdir(exist_ok=True)

    config_file = Path(__file__).parent.parent / "log_config.json"
    logging.config.dictConfig(json.loads(config_file.read_text()))
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


async def run_classfiers(
    classifiers: list[GmailClassifier],
    service: Resource,
    classfier_collection: Collection,
) -> None:
    async with asyncio.TaskGroup() as tg:
        for classifier in classifiers:
            # Check if classfier is new
            classfier_db = classfier_collection.find_one(
                {"name": classifier.name})

            # Creates a new classifier on database if it doesn't exist
            if not classfier_db:
                new_classfier_id = classfier_collection.insert_one(
                    {
                        "name": classifier.name,
                        "query": classifier.query,
                        "lastExecution": None,
                        "deprecated": False,
                        "deprecatedSince": None,
                    }
                ).inserted_id

                classfier_db = classfier_collection.find_one(
                    {"_id": new_classfier_id})

            if classfier_db["deprecated"]:
                continue

            messages = tg.create_task(classifier.classify(
                service,
                after=(
                    pendulum.now()
                    .subtract(months=1)
                    .int_timestamp
                    # pendulum.instance(
                    #     classfier_db["lastExecution"]).int_timestamp
                    # if classfier_db["lastExecution"]
                    # else None
                ),
            ))

            # Update lastExecution field
            classfier_collection.update_one(
                {"_id": classfier_db["_id"]},
                {"$set": {"lastExecution": pendulum.now()}},
            )


def get_new_messages_ids_from_history(history_response: dict, history_collection: Collection, userId: str) -> list[str]:
    messages = []
    
    if "history" not in history_response:
        return messages
    
    for history_item in history_response["history"]:
        if "messagesAdded" not in history_item:
            continue
        
        for message in history_item["messages"]:
            database.insert_last_history_id(history_collection, userId, history_item["id"])
            messages.append(message["id"])
    
    return messages



def sync_since_last_execution(history_collection: Collection, service: Resource, userId: str) -> list[str]:
    """Syncs all new messages from last execution.

    Args:
        history_collection (Collection): MongoDB collection to read/write historyIds
        service (Resource): Gmail service
        userId (str): Gmail user ID

    Raises:
        NotImplementedError: _description_

    Returns:
        list[str]: List of new messages IDs
    """
    history_id = database.get_last_history_id(history_collection, userId)
    logger.info(f"Syncing messages since last execution. Start historyID: {history_id}")
    watcher = pubsub.start_gmail_publisher(GMAIL_SERVICE, userId, os.getenv("PUBSUB_TOPIC"))

    
    if not history_id:
        raise NotImplementedError("Trying to sync since last execution without a last historyId. Implement a batch function here")
    
    history_res = gmail.get_history(service, userId, history_id)    
    new_messages = get_new_messages_ids_from_history(history_res, history_collection, userId)
    
    for new_message in new_messages:
        # Handle messages
        ...

    database.insert_last_history_id(MONGO_DATABASE["historyIds"], "me", watcher["historyId"])

    logger.info(f"Synced {len(new_messages)} new messages since last execution")

    return new_messages





if __name__ == "__main__":
    start = pendulum.now()
    setup_logging()
    logger.info("Starting Gmail Automation execution")

    # First we run the classfiers in batch from the last execution date
    # asyncio.run(run_classfiers(USER_CLASSFIERS,
    #             GMAIL_SERVICE, MONGO_DATABASE["classifiers"]))

    # After that, we setup the Pub/Sub topic to watch for new messages
    new_messages_ids = sync_since_last_execution(MONGO_DATABASE["historyIds"], GMAIL_SERVICE, "me")

    # Now, starts to watch for new messages

    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(subscription=os.getenv("PUBSUB_SUBSCRIPTION"), callback=functools.partial(pubsub.new_message_callback, MONGO_DATABASE["historyIds"], GMAIL_SERVICE, "me"))
 
        # Works for now
        try:
            logger.info("Listening for new messages...")
            while True: 
                continue
        except KeyboardInterrupt:
            logger.warning('Shutting down...')

    logger.info("Closing connections")
    GMAIL_SERVICE.users().stop(userId="me").execute()
    GMAIL_SERVICE.close()
    MONGO_DATABASE.client.close()
    CLOUD_STORAGE_CLIENT.close()
    
    end = pendulum.now()
    logger.info(
        f"Ending Gmail Automation execution. Execution time: {
            end.diff(start).in_seconds()} seconds"
    )
