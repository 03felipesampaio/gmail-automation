from typing import Callable, Self, Any
from googleapiclient.discovery import Resource
import json
import logging.config
import pendulum
from pprint import pprint
from pathlib import Path
import base64

import asyncio
from pymongo.collection import Collection

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

        logger.debug(f"Message {self.id} created")

    def update(self, **kwargs) -> Self:
        if 'id' in kwargs and kwargs['id'] != self.id:
            raise ValueError(f"Message ID mismatch. Expected: {
                             self.id}, received: {kwargs['id']}")

        for key, value in kwargs.items():
            setattr(self, key, value)

        return self

    def write(self, path: str, service: Resource, userId="me") -> Self:
        """Writes the message to a JSON file."""
        if self.payload is None:
            self.reload_message(service, userId=userId)

        with open(path, "w", encoding="utf8") as fp:
            fp.write(json.dumps(self.__dict__, indent=4, ensure_ascii=False))

        return self
    
    def to_dict(self) -> dict:
        return self.__dict__

    def __repr__(self) -> str:
        return f"<GmailMessage id={self.id}>"


class GmailClassifier:
    def __init__(
        self, name: str, query: str, handler: Callable[[GmailMessage], GmailMessage]
    ) -> None:
        self.name = name.strip()
        self.query = query.strip()
        self.handler = handler

        logger.debug(f"Classifier {self.name} created")

    def _get_minimal_messages(
        self, service: Resource, query: str, userId="me", **service_args
    ) -> list[dict]:
        """Search for all messages that match the query provided.

        Args:
            service (Resource): Gmail Resource
            query (str): Query to search
            userId (str, optional): User ID. Defaults to "me".

        Returns:
            list[dict]: Fetched messages from Gmail API in the format:
                {
                    "id": str,
                    "threadId": str
                } 
        """
        start = pendulum.now()
        messages = []

        req = service.users().messages().list(userId=userId, q=query, **service_args)

        while req is not None:
            res = req.execute()

            messages.extend(res.get("messages", []))

            req = service.users().messages().list_next(req, res)

        logger.info(
            f"Classfier '{self.name}' found: {len(messages)} messages in {
                pendulum.now().diff(start).in_seconds()} seconds".strip()
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
            f"Searching messages with query: '{
                self.query} {after_query}'".strip()
        )

        raw_messages = self._get_minimal_messages(
            service, f"{self.query} {after_query}".strip(), userId, **service_args
        )

        start = pendulum.now()

        messages = []

        async with asyncio.TaskGroup() as tg:
            gmail_messages = [GmailMessage(**r) for r in raw_messages]
            await tg.create_task(asyncio.to_thread(self.handler, gmail_messages))

            messages.extend(gmail_messages)

        end = pendulum.now()
        avg = end.diff(start).in_seconds() / \
            len(messages) if len(messages) else 0
        logger.info(
            f"Classfier '{self.name}' fetched and handled: {len(messages)} messages in {
                end.diff(start).in_seconds()} seconds. Average: {avg:.2f} seconds".strip()
        )

        return messages


def get_history(service: Resource, userId: str, startHistoryId: str) -> dict:
    req = service.users().history().list(userId="me", startHistoryId=startHistoryId)
    res = req.execute()
    
    if 'nextPageToken' in res:
        logger.warning('Found more than one page in history. This is not expected.')
        
    return res

