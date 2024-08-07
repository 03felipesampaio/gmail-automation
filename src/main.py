# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from googleapiclient.discovery import Resource
from credentials import refresh_credentials

# MongoDB libs
from pymongo import MongoClient
from pymongo.collection import Collection

# Date and time libs
import pendulum

# Environment variables
from dotenv import load_dotenv
import os

# Logging
import logging.config
from pathlib import Path
import json
import atexit

from gmail import GmailClassifier, GmailMessage

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


def get_label_by_name(
    name: str, service: Resource, label_collection: Collection, userId="me"
) -> dict:
    """Gets a label by name. If the label doesn't exist on Gmail API, but exists in database, it will be created.

    Gmail API is the source of truth, so it will be queried first. If the label is not found, it will be queried on the database.

    Args:
        name (str): Label name
        service (Resource): Gmail API service
        label_collection (Collection): MongoDB collection
        userId (str, optional): Gmail User ID. Defaults to 'me'.

    Returns:
        dict: Label object

    Raises:
        ValueError: If label not found neither on Gmail API nor on database
    """
    label = next(
        (
            x
            for x in service.users()
            .labels()
            .list(userId=userId)
            .execute()
            .get("labels", [])
            if x["name"] == name
        ),
        None,
    )

    db_label = None

    # If label not found on Gmail API, we should query on database
    if not label:
        db_label = label_collection.find_one({"name": name})
        if not db_label:
            raise ValueError(
                f"Label {name} not found, please create it on database or Gmail API"
            )
        db_label.pop("_id")
        # Label was found on database and not in Gmail API, so we should create it
        # Creating label on Gmail API
        label = service.users().labels().create(userId=userId, body=db_label).execute()

    return label


def run_classfiers(
    classifiers: list[GmailClassifier],
    service: Resource,
    classfier_collection: Collection,
) -> None:
    for classifier in classifiers:
        # Check if classfier is new
        classfier_db = classfier_collection.find_one({"name": classifier.name})

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

            classfier_db = classfier_collection.find_one({"_id": new_classfier_id})

        if classfier_db["deprecated"]:
            continue

        messages = classifier.classify(
            service,
            after=(pendulum.now().subtract(months=2).int_timestamp
                # pendulum.instance(classfier_db["lastExecution"]).int_timestamp
                # if classfier_db["lastExecution"]
                # else None
            ),
        )

        classfier_collection.update_one(
            {"_id": classfier_db["_id"]},
            {"$set": {"lastExecution": pendulum.now()}},
        )


if __name__ == "__main__":
    setup_logging()
    
    start = pendulum.now()
    logger.info("Starting Gmail Automation execution")

    # Getting credentials and connections
    # Gmail credentials
    service = refresh_credentials(os.environ.get("GMAIL_CREDENTIALS_PATH"))
    logger.info("Connected to Gmail API")

    # MongoDB connection
    client = MongoClient(os.getenv("CONNECTION_STRING"))
    logger.info("Connected to MongoDB")
    db = client["GmailAutomation"]

    # TODO What if we create a function that returns a dict with name and id?
    # This function must execute in a setup phase
    # We don't expect to receive new labels during runtime, so it should be safe to do it
    query_label = lambda x: get_label_by_name(x, service, db["labels"])

    classifiers = [
        GmailClassifier(
            "NubankPixAutomatico",
            'from:Nubank subject:"Pix programado enviado com sucesso"',
            lambda x: x.add_label(service, query_label("Nubank")["id"]),
        ),
        GmailClassifier(
            "Nubank",
            "from:Nubank",
            lambda x: x.add_label(service, query_label("Nubank")["id"]),
        ),
        GmailClassifier(
            "Clickbus",
            'from:Clickbus subject:"Pedido AROUND 2 confirmado"',
            lambda x: x.add_label(service, query_label("Clickbus")["id"]).write(
                "clickbus.json"
            ),
        ),
    ]

    run_classfiers(classifiers, service, db["classifiers"])

    end = pendulum.now()
    logger.info(f"Ending Gmail Automation execution. Execution time: {end.diff(start).in_seconds()} seconds")
    service.close()
