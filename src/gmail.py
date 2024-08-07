from typing import Callable, Self
from googleapiclient.discovery import Resource
import json
import logging.config
import pendulum

# Logger was initialized in the main.py file
logger = logging.getLogger("gmail_automation")


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

        logger.debug(f"Message {self.id} created")

    def add_label(self, service: Resource, label_id: str) -> Self:
        """Adds a label to the message.

        Fails if the label doesn't exist in the Gmail account.
        """
        logger.debug(f"Adding label {label_id} to message {self.id}")
        start = pendulum.now()
        service.users().messages().modify(
            userId="me", id=self.id, body={"addLabelIds": [label_id]}
        ).execute()
        end = pendulum.now()
        logger.debug(
            f"Label {label_id} added to message {self.id} in {end.diff(start).in_seconds()} seconds"
        )

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

        logger.debug(f"Classifier {self.name} created")

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

        logger.debug(
            f"Searching messages with query: '{self.query} {after_query}'".strip()
        )

        start = pendulum.now()
        raw_messages = []
        req = (
            service.users()
            .messages()
            .list(
                userId=userId, q=f"{self.query} {after_query}".strip(), **service_args
            )
        )

        while req is not None:
            res = req.execute()

            if "messages" in res:
                raw_messages.extend(res.get("messages", []))

            req = service.users().messages().list_next(req, res)

        logger.info(
            f"Classfier '{self.name}' found: {len(raw_messages)} messages in {pendulum.now().diff(start).in_seconds()} seconds".strip()
        )

        start = pendulum.now()
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

        end = pendulum.now()
        avg = end.diff(start).in_seconds() / len(messages) if len(messages) else 0
        logger.info(
            f"Classfier '{self.name}' fetched and handled: {len(messages)} messages in {end.diff(start).in_seconds()} seconds. Average: {avg:.2f} seconds".strip()
        )

        return messages
