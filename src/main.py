# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from http import client
from typing import Callable, Self
from googleapiclient.discovery import Resource
from credentials import refresh_credentials
import json
import pendulum
from pymongo import MongoClient
from pymongo.collection import Collection

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(".env")


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


class GmailMessage:
    """Gmail email message object.
    It contains all the information about a message after querying
    for a message in Gmail API with format equals to "full".
    """

    def __init__(
        self,
        id: str,
        historyId: str,
        internalDate: str,
        labelIds: list[str],
        payload: dict,
        sizeEstimate: int,
        snippet: str,
        threadId: str,
        raw: str | None = None,
    ) -> None:
        self.id = id
        self.historyId = historyId
        self.internalDate = internalDate
        self.labelIds = labelIds
        self.payload = payload
        self.raw = raw
        self.sizeEstimate = sizeEstimate
        self.snippet = snippet
        self.threadId = threadId

        # Loading headers
        self.from_ = next(x for x in self.payload["headers"] if x["name"] == "From")[
            "value"
        ]
        self.subject = next(
            x for x in self.payload["headers"] if x["name"] == "Subject"
        )["value"]
        self.date = next(x for x in self.payload["headers"] if x["name"] == "Date")[
            "value"
        ]

        # Attachments
        pass

    def add_label(self, service: Resource, label_id: str) -> Self:
        """Adds a label to the message.

        Fails if the label doesn't exist in the Gmail account.
        """
        service.users().messages().modify(
            userId="me", id=self.id, body={"addLabelIds": [label_id]}
        ).execute()

        # TODO This method changes Message state, so it should return a new instance or update it?

        return self

    def print(self) -> Self:
        print(self)
        return self

    def write(self, path: str) -> Self:
        with open(path, "w", encoding="utf8") as fp:
            fp.write(json.dumps(self.__dict__, indent=4, ensure_ascii=False))

        return self

    def __repr__(self) -> str:
        return f"<GmailMessage id={self.id} snippet={self.snippet[:25]}>"


class GmailClassifier:
    def __init__(
        self, name: str, query: str, handler: Callable[[GmailMessage], GmailMessage]
    ) -> None:
        self.name = name.strip()
        self.query = query.strip()
        self.handler = handler

    def classify(
        self, service: Resource, userId="me", after: int = None, **service_args
    ) -> list[GmailMessage]:
        """Classify messages based on the query provided and executes handlers to all matched.

        Args:
            service (Resource): Gmail API service
            userId (str, optional): Gmail User ID. Defaults to 'me'.
            after (int, optional): Date to filter messages. Defaults to None.

        Returns:
            list[GmailMessage]: List of classified messages
        """
        if after and not isinstance(after, int):
            raise ValueError(
                f"after must be an integer, received: {type(after)}: {after}"
            )

        after_query = f"after:{after}" if after else ""

        raw_messages = []
        req = (
            service.users()
            .messages()
            .list(
                userId=userId, q=f"{self.query} {after_query}".strip(), **service_args
            )
        )

        print(f"Query: {self.query} {after_query}".strip())

        while req is not None:
            res = req.execute()

            if "messages" in res:
                raw_messages.extend(res.get("messages", []))

            req = service.users().messages().list_next(req, res)

        messages = []
        for raw_message in raw_messages:
            message = self.handler(
                GmailMessage(
                    **service.users()
                    .messages()
                    .get(userId=userId, id=raw_message["id"], format="full")
                    .execute()
                )
            )
            messages.append(message)

        return messages


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
            after=(
                pendulum.instance(classfier_db["lastExecution"]).int_timestamp
                if classfier_db["lastExecution"]
                else None
            ),
        )

        classfier_collection.update_one(
            {"_id": classfier_db["_id"]},
            {"$set": {"lastExecution": pendulum.now()}},
        )


if __name__ == "__main__":
    # Getting credentials and connections
    # Gmail credentials
    service = refresh_credentials(os.environ.get("GMAIL_CREDENTIALS_PATH"))
    # MongoDB connection
    uri = os.getenv("CONNECTION_STRING")
    client = MongoClient(uri)
    db = client["GmailAutomation"]

    # TODO What if we create a function that returns a dict with name and id?
    # This function must execute in a setup phase
    # We don't expect to receive new labels during runtime, so it should be safe to do it
    query_label = lambda x: get_label_by_name(x, service, db["labels"])

    classifiers = [
        GmailClassifier(
            "NubankPixAutomatico",
            'from:Nubank subject:"Pix programado enviado com sucesso"',
            lambda x: x.print().add_label(service, query_label("Nubank")["id"]),
        ),
        GmailClassifier(
            "Nubank",
            "from:Nubank",
            lambda x: x.print().add_label(service, query_label("Nubank")["id"]),
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

    service.close()
