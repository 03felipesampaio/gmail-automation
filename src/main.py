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
from pprint import pprint

from gmail import GmailClassifier


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


def new_message_callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """Calback function to handle new messages from Pub/Sub.

    Args:
        message (PubSub message): New message from Pub/Sub.
    """
    # print(type(message))
    print(f"Received message:")
    pprint(message.data.decode("utf-8"))
    message.ack()

if __name__ == "__main__":
    start = pendulum.now()
    setup_logging()
    logger.info("Starting Gmail Automation execution")

    # First we run the classfiers in batch from the last execution date
    asyncio.run(run_classfiers(USER_CLASSFIERS,
                GMAIL_SERVICE, MONGO_DATABASE["classifiers"]))

    # After that, we setup the Pub/Sub topic to watch for new messages
    watcher = GMAIL_SERVICE.users().watch(userId="me", body={
        "topicName": os.getenv("PUBSUB_TOPIC")
    }).execute()
    
    # pprint(watcher)

    # Now, starts to watch for new messages

    with pubsub_v1.SubscriberClient() as subscriber:
        future = subscriber.subscribe(subscription=os.getenv("PUBSUB_SUBSCRIPTION"), callback=new_message_callback)
        
        # FIXME This should stop with CTRL+C. But it's not working (I followed Google's documentation BTW)
        # try:
        #     future.result()
        # except KeyboardInterrupt:
        #     print("Shutting down...")
        #     future.cancel()

        # Works for now
        try:
            logger.info("Listening for new messages...")
            while True: 
                continue
        except KeyboardInterrupt:
            logger.warning('Shutting down...')


    GMAIL_SERVICE.users().stop(userId="me").execute()
    GMAIL_SERVICE.close()
    MONGO_DATABASE.client.close()
    CLOUD_STORAGE_CLIENT.close()
    
    end = pendulum.now()
    logger.info(
        f"Ending Gmail Automation execution. Execution time: {
            end.diff(start).in_seconds()} seconds"
    )
