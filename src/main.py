# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1
from typing import Callable, Self
from googleapiclient.discovery import Resource
from credentials import refresh_credentials
import json
import pendulum

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(".env")


class GmailMessage:
    """Gmail email message object. 
    It contains all the information about a message after querying 
    for a message in Gmail API with format equals to "full".
    """

    def __init__(self, id: str, historyId: str, internalDate: str, labelIds: list[str], payload: dict, sizeEstimate: int, snippet: str, threadId: str, raw: str | None = None) -> None:
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
        self.from_ = next(x for x in self.payload['headers'] if x['name'] == 'From')[
            'value']
        self.subject = next(
            x for x in self.payload['headers'] if x['name'] == 'Subject')['value']
        self.date = next(x for x in self.payload['headers'] if x['name'] == 'Date')[
            'value']

        # Attachments
        pass

    def add_label(self, service: Resource, label_id: str) -> Self:
        print('Should add label', label_id)
        return self

    def print(self) -> Self:
        print(self)
        return self

    def write(self, path: str) -> Self:
        with open(path, 'w', encoding='utf8') as fp:
            fp.write(json.dumps(self.__dict__, indent=4, ensure_ascii=False))

        return self

    def __repr__(self) -> str:
        return f'<GmailMessage id={self.id} snippet={self.snippet[:25]}>'


class GmailClassifier:
    def __init__(self, name: str, query: str, handler: Callable[[GmailMessage], GmailMessage]) -> None:
        self.name = name.strip()
        self.query = query.strip()
        self.handler = handler

    def classify(self, service: Resource, userId='me', after: str | int = None, **service_args) -> list[GmailMessage]:
        """Classify messages based on the query provided and executes handlers to all matched.

        Args:
            service (Resource): Gmail API service
            userId (str, optional): Gmail User ID. Defaults to 'me'.
            after (str | int, optional): Date to filter messages. Defaults to None.

        Returns:
            list[GmailMessage]: List of classified messages
        """
        after_query = f'after:{after}' if after else ''

        raw_messages = []
        req = service.users().messages().list(
            userId=userId, q=f'{self.query} {after_query}'.strip(), **service_args)

        print(f'Query: {self.query} {after_query}'.strip())

        while req is not None:
            res = req.execute()

            if 'messages' in res:
                raw_messages.extend(res.get('messages', []))

            req = service.users().messages().list_next(req, res)

        messages = []
        for raw_message in raw_messages:
            message = self.handler(GmailMessage(**service.users().messages().get(
                userId=userId, id=raw_message['id'], format='full').execute()))
            messages.append(message)

        return messages


if __name__ == "__main__":
    service = refresh_credentials(os.environ.get("GMAIL_CREDENTIALS_PATH"))

    classifiers = [
        # GmailClassifier('Nubank', 'from:Nubank', lambda x: x.print()),
        GmailClassifier('Clickbus', 'from:Clickbus subject:"Pedido AROUND 2 confirmado"',
                        lambda x: x.add_label(service, 'Clickbus').write('clickbus.json')),
    ]

    for classifier in classifiers:
        messages = classifier.classify(
            service, after=pendulum.now().subtract(days=15).int_timestamp)
        print(f"Classifier: {classifier.name}")
        # print(f"Messages: {messages}")

    service.close()
