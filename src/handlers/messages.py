from typing import Self, Callable, Literal
from pathlib import Path
from gmail import GmailMessage
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
import pendulum
import base64
import json
from pprint import pprint

import logging

logger = logging.getLogger('gmail_automation')


class MessageHandler:
    """Defines a message handler for Gmail messages.  
    The handler create a execution plan for handling all matched messages and execute the actions in batch, 
    avoiding calling the API to each message.

    Execution order:
        1. Handle message content
        2. Download attachments
        3. Manage labels
        4. Removes from trash
        5. Move to trash
    """

    def __init__(self, service: Resource, userId: str) -> None:
        self.service = service
        self.userId = userId
        self._execution_plan: list[Callable[[
            Resource, str, list[GmailMessage]]], None] = []

    def _add_to_execution_plan(self, handler: Callable[[Resource, str, list[GmailMessage]], None]) -> None:
        self._execution_plan.append(handler)

    def _refresh_messages(self, service: Resource, userId: str, messages: list[GmailMessage]) -> None:
        """Refreshes messages content.
        """
        # TODO Split the list in chunks of 500 messages.
        batch_req = service.new_batch_http_request()

        def update_message(message) -> Callable[[str, dict, HttpError], None]:
            def callback(req_id, res, exc):
                if exc is not None:
                    raise exc
                message.update(**res)
            return callback

        for message in messages:
            batch_req.add(service.users().messages().get(
                userId=userId, id=message.id, format='minimal'), request_id=message.id, callback=update_message(message))

        batch_req.execute()

    def get_content(self, format: Literal['minimal', 'full', 'raw', 'metadata'] = 'full') -> Self:
        """Fetch messages content.
        """
        def handler(service, userId, messages):
            batch_req = service.new_batch_http_request()

            def update_message(message) -> Callable[[str, dict, HttpError], None]:
                def callback(req_id, res, exc):
                    if exc is not None:
                        raise exc
                    message.update(**res)
                return callback

            for message in messages:
                batch_req.add(service.users().messages().get(
                    userId=userId, id=message.id, format=format), callback=update_message(message))

            batch_req.execute()

        self._add_to_execution_plan(handler)

        logger.info(f'Add fetch messages content in {
                    format} format to execution plan')

        return self

    def save_to_json(self, path_dir: str | Path) -> Self:
        """Saves messages to a JSON file.

        Args:
            path_dir (str | Path): Path to save the JSON file.
        """
        path_dir = Path(path_dir)
        path_dir.mkdir(parents=True, exist_ok=True)

        def handler(service, userId, messages):
            for message in messages:
                if message.payload is None:
                    raise ValueError(
                        'Message payload is not loaded. Call get() method before save_to_json()')
                file_path = path_dir / f'{message.id}.json'
                file_path.write_text(json.dumps(
                    message.__dict__, indent=4, ensure_ascii=False), encoding='utf8')

        self._add_to_execution_plan(handler)

        return self

    def download_attachments(self, attachment_handler: Callable[[dict], None], filter: Callable[[dict], bool] = lambda x: True) -> Self:
        """Downloads attachments from messages.

        Args:
            attachment_handler (Callable[[dict], None]): Function to handle attachments, only receives the attachment dict.
            filter (Callable[[dict], bool], optional): Function to filter message, only fetches filtered attachments. Defaults to lambda x: True.

                The attachment dict has the following format:
                    {
                        'filename': str,
                        'message_id': str,
                        'date': pendulum.DateTime,
                        'data': bytes
                    }
        """
        def get_callback(message, filename):
            return lambda req_id, res, exc: attachment_handler(
                update_attachment(
                    res,
                    filename=filename,
                    message_id=message.id,
                    date=pendulum.from_timestamp(
                        int(message.internalDate[:-3]))
                )
            )

        def handler(service, userId, messages):
            batch_req = service.new_batch_http_request()

            for message in messages:
                if message.payload is None or 'parts' not in message.payload:
                    raise ValueError(
                        f'Message payload is not loaded. Call get() method before download_attachments(). Message ID: {message.id}')

                for part in message.payload["parts"]:
                    if 'attachmentId' not in part['body'] or not filter(part):
                        continue

                    batch_req.add(
                        service.users().messages().attachments().get(
                            userId=userId, messageId=message.id, id=part['body']['attachmentId']),
                        callback=get_callback(message, part['filename'])
                    )

            batch_req.execute()

        self._add_to_execution_plan(handler)

        return self

    def manage_labels(self, add_labels: list[str] = None, remove_labels: list[str] = None) -> Self:
        """Manages labels for the message.

        Args:
            add_labels (list[str]): Labels Ids to be added to the message. Label must exist.
            remove_labels (list[str]): Labels Ids to be removed from the message. Doesn't fail if the label doesn't exist.
        """
        # TODO batchModify only accepts max of 1000 messages. Break the list in chunks of 500 messages.
        def handler(service, userId, messages):
            if not messages:
                return

            labels = service.users().labels().list(
                userId=userId).execute().get("labels", [])
            labels_ids = [label['id'] for label in labels]

            for l in add_labels or []:
                if l not in labels_ids:
                    raise ValueError(f'Label {l} not found on Gmail API')

            service.users().messages().batchModify(userId=userId, body={
                'addLabelIds': add_labels,
                'removeLabelIds': remove_labels,
                'ids': [message.id for message in messages]
            }).execute()
            # except HttpError as e:
            #     logger.error(f'There is no label with the given ID. {e}')

        self._add_to_execution_plan(handler)
        self._add_to_execution_plan(self._refresh_messages)

        return self

    def to_trash(self) -> Self:
        """Moves messages to trash.
        """
        logger.warning(
            f'After moving messages to trash, they will be only queried again if "in:trash" is used in query.')

        def handler(service, userId, messages):
            batch_req = service.new_batch_http_request()

            for message in messages:
                batch_req.add(service.users().messages().trash(
                    userId=userId, id=message.id))

            batch_req.execute()

        self._add_to_execution_plan(handler)

        return self

    def untrash(self) -> Self:
        """Removes messages from trash.

        Message in trash need to be queried with "in:trash" in query. The normal behavior to queries is just to fetch messages in inbox.
        """

        logger.warning(
            'Messages in trash need to be queried with "in:trash" in query. The normal behavior to queries is just to fetch messages in inbox.')

        def handler(service, userId, messages):
            batch_req = service.new_batch_http_request()

            for message in messages:
                batch_req.add(service.users().messages().untrash(
                    userId=userId, id=message.id))

            batch_req.execute()

        self._add_to_execution_plan(handler)

        return self

    def forward(self) -> Self:
        raise NotImplementedError('Not implemented yet')

    def execute(self, messages: list[GmailMessage]) -> None:
        """Creates a execution plan for handling all matched messages.
        """
        for action in self._execution_plan:
            action(self.service, self.userId, messages)


def update_attachment(attachment: dict, filename: str, message_id: str, date: pendulum.DateTime) -> dict:
    """Updates attachment dict with the new data.

    Returns:
        dict: Attachment dict with the new data in the following format:
            {
                'filename': str,
                'message_id': str,
                'date': pendulum.DateTime,
                'data': bytes
            }
    """
    attachment['data'] = base64.urlsafe_b64decode(attachment["data"])
    attachment['filename'] = filename
    attachment['message_id'] = message_id
    attachment['date'] = date
    return attachment
