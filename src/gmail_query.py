from datetime import datetime
from pendulum.datetime import DateTime
from typing import Callable, Any
from googleapiclient.discovery import Resource
import re


class GmailQuery:
    """A Gmail query constructor to convert the query parameters into strings or to check if a new email matches the query parameters.

    The queries follow Gmail official documentation: https://support.google.com/mail/answer/7190?hl=en
    """

    def __init__(self, from_: str | None = None, to: str | None = None, subject: str | None = None, label: str | None = None, has: str | None = None, filename: str | None = None):
        # Constructor code here
        self.from_ = from_
        self.to = to
        self.subject = subject
        self.label = label
        self.has = has
        self.filename = filename

    def to_gmail_string(self) -> str:
        """Converts the query parameters into a Gmail query string."""
        query = "{from_} {to} {subject} {label} {has} {filename}".format(
            from_=f'from:{self.from_}' if self.from_ else '',
            to=f'to:{self.to}' if self.to else '',
            subject=f'subject:{self.subject}' if self.subject else '',
            label=f'label:{self.label}' if self.label else '',
            has=f'has:{self.has}' if self.has else '',
            filename=f'filename:{self.filename}' if self.filename else ''
        )

        return re.sub(r'\s+', ' ', query).strip()

    def matches(self, email: dict) -> bool:
        return False


class ComposedGmailQuery:
    def __init__(self) -> None:
        pass


# def compose_gmail_query


class EmailClassifier:
    """Create a classifier to categorize emails and apply a handler to them.
    """

    def __init__(self, query: GmailQuery, handler: Callable[[dict], Any]) -> None:
        self.query = query
        self.handler = handler

    def classify(self, email: dict) -> Any:
        """Apply handler to email if it matches the query.
        """
        if self.query.matches(email):
            return self.handler(email)

        return None

    def classify_all(self, service: Resource) -> list[Any]:
        """Query all emails that match the query and apply the handler to them.
        """

        emails = []
        req = service.users().messages().list(
            userId='me', q=self.query.to_gmail_string())
        while req is not None:
            res = req.execute()

            if 'messages' in res:
                emails.extend(res['messages'])

            req = service.users().messages().list_next(req, res)

        return [self.handler(email) for email in emails]


def search_for_new_emails(service, userId: str = 'me', since: DateTime = None, only_unread: bool = False) -> list[dict]:
    """Searches for new emails in the user's inbox.

    Attrs:
        service: The Gmail API service object.
        userId: The user's email address.
        since: The date to search for emails since.
        only_unread: If True, only searches for unread emails.
    """
    query = '{since} {unread}'.format(
        since=f'after:{since.int_timestamp}' if since else '',
        unread='is:unread' if only_unread else ''
    )

    req = service.users().messages().list(userId=userId, q=query)

    emails = []
    while req is not None:
        res = req.execute()

        if 'messages' in res:
            emails.extend(res['messages'])

        req = service.users().messages().list_next(req, res)

    return emails
