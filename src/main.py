# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from googleapiclient.discovery import Resource

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
from pathlib import Path
import json
import atexit

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


if __name__ == "__main__":
    start = pendulum.now()
    setup_logging()
    logger.info("Starting Gmail Automation execution")

    # First we run the classfiers in batch from the last execution date
    asyncio.run(run_classfiers(USER_CLASSFIERS,
                GMAIL_SERVICE, MONGO_DATABASE["classifiers"]))

    # After that, we setup the Pub/Sub topic to watch for new messages

    # Now, starts to watch for new messages

    GMAIL_SERVICE.close()
    MONGO_DATABASE.client.close()
    CLOUD_STORAGE_CLIENT.close()
    
    end = pendulum.now()
    logger.info(
        f"Ending Gmail Automation execution. Execution time: {
            end.diff(start).in_seconds()} seconds"
    )
