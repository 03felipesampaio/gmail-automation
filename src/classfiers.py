from gmail import GmailClassifier, GmailMessage
from handlers.attachments import AttachmentHandler
from handlers.messages import MessageHandler
from credentials import refresh_credentials

from googleapiclient.discovery import Resource
from google.cloud import storage
import logging
import os

# MongoDB libs
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

logger = logging.getLogger("gmail_automation")


def setup_mongodb() -> Database:
    # MongoDB connection
    client = MongoClient(os.getenv("CONNECTION_STRING"))
    logger.info("Connected to MongoDB")
    db = client["GmailAutomation"]
    
    return db


def setup_labels(service: Resource, user_labels_collection: Collection, userId="me") -> dict:
    """Creates user defined labels on Gmail API and returs all labels as a dict.
    
    Args:
        service (Resource): Gmail API service
        user_labels_collection (Collection): MongoDB labels collection
        userId (str, optional): Gmail User ID. Defaults to 'me'.
        
    Returns:
        dict: Dict with this format -> {label_name: label_id}
    """
    # Getting all labels from Gmail API
    gmail_labels = service.users().labels().list(
        userId=userId).execute().get("labels", [])

    # Getting all labels names from Gmail API
    gmail_labels_names = [x["name"] for x in gmail_labels]

    # Creating user defined labels on Gmail API
    # TODO add filter to only fetch not found labels
    for user_label in user_labels_collection.find():
        if user_label['name'] in gmail_labels_names:
            continue
        
        logger.info(f"Creating label '{user_label['name']}' on Gmail API")
        user_label.pop("_id") # Removes MongoDB _id field, so the req body can be sent to Gmail API
        
        service.users().labels().create(userId=userId, body=user_label).execute()
        
    updated_gmail_labels = service.users().labels().list(userId=userId).execute().get("labels", [])

    # Queries all labels from Gmail API again
    return {l['name']: l['id'] for l in updated_gmail_labels}


# Gmail credentials
GMAIL_SERVICE = refresh_credentials(os.environ.get("GMAIL_CREDENTIALS_PATH"))
logger.info("Connected to Gmail API")

MONGO_DATABASE = setup_mongodb()
labels = setup_labels(GMAIL_SERVICE, MONGO_DATABASE['labels'])

CLOUD_STORAGE_CLIENT = storage.Client()
bucket = CLOUD_STORAGE_CLIENT.get_bucket(os.getenv("BUCKET_NAME"))


USER_CLASSFIERS = [
    GmailClassifier(
        "Nubank",
        "from:Nubank",
        MessageHandler(GMAIL_SERVICE, "me").get_content('full').manage_labels(
            [labels['Nubank']]).save_to_json('messages/nubank').execute,
    ),
    GmailClassifier(
        'FaturaNubank',
        'subject:"A fatura do seu cartão Nubank está fechada"',
        MessageHandler(GMAIL_SERVICE, "me")
            .get_content('full')
            .download_attachments(
                lambda x: AttachmentHandler().write_on_cloud_storage(bucket, x, 'Faturas/Nubank'))
            .manage_labels([labels['Nubank/Fatura Nubank']])
            .execute
    ),
    GmailClassifier(
        "Clickbus",
        'from:Clickbus subject:"Pedido AROUND 2 confirmado"',
        MessageHandler(GMAIL_SERVICE, "me").manage_labels(
            [labels['Clickbus']]).execute,
    ),
    GmailClassifier(
        "ClickbusPedidos",
        'from:"Clickbus" subject:("Oba! Sua viagem está confirmada!" OR "Pedido")',
        MessageHandler(GMAIL_SERVICE, "me").manage_labels(
            [labels['Clickbus/Pedidos']]).execute,
    ),
    GmailClassifier(
        'InternetClaro',
        'from:"Fatura Claro"',
        MessageHandler(GMAIL_SERVICE, "me")
            .get_content('full')
            .manage_labels([labels['Internet Claro']])
            .download_attachments(AttachmentHandler().save_locally('attachments/Claro').execute)
            .execute
    ),
    GmailClassifier(
        'FaturaInter',
        'subject:"Fatura Cartão Inter"',
        MessageHandler(GMAIL_SERVICE, "me")
            .get_content('full')
            .manage_labels([labels['Fatura Inter']])
            .download_attachments(
                AttachmentHandler().write_on_cloud_storage(bucket, 'Faturas/Inter').execute)
            .execute,
    ),
    GmailClassifier(
        'Preply',
        'from:Preply',
        MessageHandler(GMAIL_SERVICE, "me")
            .manage_labels([labels['Preply']])
            .execute
    )
]