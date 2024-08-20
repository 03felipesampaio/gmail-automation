# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from googleapiclient.discovery import Resource
from credentials import refresh_credentials

# MongoDB libs
from pymongo import MongoClient
from pymongo.collection import Collection

# Date and time libs
import pendulum

# Async lib
import asyncio

# Environment variables
from dotenv import load_dotenv
import os

# Logging
import logging.config
from pathlib import Path
import json
import atexit

from gmail import GmailClassifier, GmailMessage, CloudStorage, save_attachment_locally

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


def setup_labels(service: Resource, user_labels: dict, userId="me") -> dict:
    """Creates user defined labels on Gmail API and returns all labels.

    Args:
        service (Resource): Gmail API service
        user_labels (dict): User defined labels
        userId (str, optional): Gmail User ID. Defaults to 'me'.
    """
    # Getting all labels from Gmail API
    labels = service.users().labels().list(
        userId=userId).execute().get("labels", [])

    # Getting all labels names from Gmail API
    labels_names = [x["name"] for x in labels]

    # Creating user defined labels on Gmail API
    for label in user_labels:
        if label['name'] not in labels_names:
            logger.info(f"Creating label '{label['name']}' on Gmail API")
            service.users().labels().create(userId=userId, body=label).execute()

    # Queries all labels from Gmail API again
    return service.users().labels().list(userId=userId).execute().get("labels", [])


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
                f"Label {
                    name} not found, please create it on database or Gmail API"
            )
        db_label.pop("_id")
        # Label was found on database and not in Gmail API, so we should create it
        # Creating label on Gmail API
        label = service.users().labels().create(userId=userId, body=db_label).execute()

    return label


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
                    # pendulum.now()
                    # .subtract(months=6)
                    # .int_timestamp
                    pendulum.instance(
                        classfier_db["lastExecution"]).int_timestamp
                    if classfier_db["lastExecution"]
                    else None
                ),
            ))

            # Update lastExecution field
            classfier_collection.update_one(
                {"_id": classfier_db["_id"]},
                {"$set": {"lastExecution": pendulum.now()}},
            )


async def main():
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
    user_labels = list(db["labels"].find())
    for label in user_labels:
        label.pop("_id")

    labels = {l['name']: l for l in setup_labels(service, user_labels)}

    storage = CloudStorage(None)

    classifiers = [
        GmailClassifier(
            "Nubank",
            "from:Nubank",
            lambda x: x.add_label(service, labels["Nubank"]["id"]),
        ),
        GmailClassifier(
            'FaturaNubank',
            'subject:"A fatura do seu cartão Nubank está fechada"',
            lambda x: x.add_label(
                service, labels["Nubank/Fatura Nubank"]["id"])
        ),
        GmailClassifier(
            "Clickbus",
            'from:Clickbus subject:"Pedido AROUND 2 confirmado"',
            lambda x: x.add_label(service, labels["Clickbus"]["id"]).write(
                "clickbus.json", service, userId="me"
            ),
        ),
        GmailClassifier(
            "ClickbusPedidos",
            'from:"Clickbus" subject:("Oba! Sua viagem está confirmada!" OR "Pedido")',
            lambda x: x.add_label(service, labels["Clickbus/Pedidos"]["id"])
        ),
        GmailClassifier(
            'InternetClaro',
            'from:"Fatura Claro"',
            # lambda x: x.add_label(service, labels["Internet Claro"]["id"]).download_attachments(service, save_attachment_locally)
            lambda x: x.add_label(service, labels["Internet Claro"]["id"]).download_attachments(
                service, storage.get_dir('attachments/fatura_claro').write_attachment)
        ),
        GmailClassifier(
            'FaturaInter',
            'subject:"Fatura Cartão Inter"',
            lambda x: x.add_label(service, labels["Fatura Inter"]["id"]).download_attachments(
                service, storage.get_dir('attachments/fatura_inter').write_attachment)
        ),
        GmailClassifier(
            'Preply',
            'from:Preply',
            lambda x: x.add_label(service, labels["Preply"]["id"])
        )
    ]

    await run_classfiers(classifiers, service, db["classifiers"])

    end = pendulum.now()
    logger.info(
        f"Ending Gmail Automation execution. Execution time: {
            end.diff(start).in_seconds()} seconds"
    )
    service.close()


if __name__ == "__main__":
    asyncio.run(main())
