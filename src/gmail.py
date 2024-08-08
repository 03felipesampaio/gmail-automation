from typing import Callable, Self
from googleapiclient.discovery import Resource
import json
import logging.config
import pendulum
from pprint import pprint

import asyncio

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
        historyId: str | None = None,
        internalDate: str | None = None,
        labelIds: list[str] | None = None,
        payload: dict | None = None,
        sizeEstimate: int | None = None,
        snippet: str | None = None,
        threadId: str | None = None,
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
        # self.from_ = next(x for x in self.payload["headers"] if x["name"] == "From")[
        #     "value"
        # ]
        # self.subject = next(
        #     x for x in self.payload["headers"] if x["name"] == "Subject"
        # )["value"]
        # self.date = next(x for x in self.payload["headers"] if x["name"] == "Date")[
        #     "value"
        # ]

        # Attachments
        pass

        logger.debug(f"Message {self.id} created")

    def reload_message(self, service: Resource, userId="me") -> Self:
        """Loads the message with full format from Gmail API and updates this object.

        Args:
            service (Resource): Gmail API service
            userId (str, optional): Gmail User ID. Defaults to 'me'.

        Returns:
            Self: GmailMessage instance
        """
        logger.debug(f"Loading message {self.id}")
        start = pendulum.now()
        message = (
            service.users()
            .messages()
            .get(userId=userId, id=self.id, format="full")
            .execute()
        )
        for key, value in message.items():
            setattr(self, key, value)
        end = pendulum.now()
        logger.debug(
            f"Message {self.id} loaded in {end.diff(start).in_seconds()} seconds"
        )

        return self

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

    def print(self, service: Resource, userId="me") -> Self:
        if self.payload is None:
            self.reload_message(service, userId=userId)

        pprint(self)
        return self

    def write(self, path: str, service: Resource, userId="me") -> Self:
        """Writes the message to a JSON file."""
        if self.payload is None:
            self.reload_message(service, userId=userId)

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

    def _get_raw_messages(
        self, service: Resource, query: str, userId="me", **service_args
    ) -> list[dict]:
        start = pendulum.now()
        messages = []

        req = service.users().messages().list(userId=userId, q=query, **service_args)

        while req is not None:
            res = req.execute()

            messages.extend(res.get("messages", []))

            req = service.users().messages().list_next(req, res)

        logger.info(
            f"Classfier '{self.name}' found: {len(messages)} messages in {pendulum.now().diff(start).in_seconds()} seconds".strip()
        )

        return messages

    async def classify(
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

        raw_messages = self._get_raw_messages(
            service, f"{self.query} {after_query}".strip(), userId, **service_args
        )

        start = pendulum.now()
        
        # This part must be async
        # loop = asyncio.get_event_loop()
        messages = []
        
        async with asyncio.TaskGroup() as tg:
            for raw_message in raw_messages:
                message = tg.create_task(asyncio.to_thread(GmailMessage(raw_message["id"]).reload_message, service, 'me'))
                # message = GmailMessage(raw_message["id"]).reload_message(service, 'me')
                messages.append(message)
                # messages.append(asyncio.to_thread(self.handler, GmailMessage(raw_message["id"])))
        
        # tasks = [asyncio.create_task(asyncio.to_thread(self.handler, GmailMessage(raw_message["id"]))) for raw_message in raw_messages]
        # done, pending = await asyncio.wait(tasks)
        # for task in done:
        #     messages.append(task.result())
        
        # for raw_message in raw_messages:
        #     # message = asyncio.to_thread(self.handler, GmailMessage(raw_message["id"]))
        #     # message = await loop.run_in_executor(None, self.handler, GmailMessage(raw_message["id"]))
        #     # message = self.handler(GmailMessage(raw_message["id"]))
        #     print(type(message))
        #     raise Exception('')
        #     # Handler right now is sync
        #     # Lets make it async
        #     messages.append(message)

        end = pendulum.now()
        avg = end.diff(start).in_seconds() / len(messages) if len(messages) else 0
        logger.info(
            f"Classfier '{self.name}' fetched and handled: {len(messages)} messages in {end.diff(start).in_seconds()} seconds. Average: {avg:.2f} seconds".strip()
        )

        return messages
