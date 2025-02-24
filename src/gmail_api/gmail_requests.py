from googleapiclient.discovery import Resource
import logging
from functools import partial


logger = logging.getLogger("gmail_automation")


BATCH_SIZE = 2


def query_messages(
    gmail_resource: Resource, userId: str, query_string: str, maxResults: int = BATCH_SIZE
) -> list[dict]:
    """
    Get messages from Gmail by query string
    """
    req = (
        gmail_resource.users()
        .messages()
        .list(userId=userId, q=query_string, maxResults=maxResults)
    )
    res = req.execute()

    if "nextPageToken" in res:
        logger.warning(
            f"When fetching messages from query '{query_string}', the service found more messages than the maxResults parameter. Messages found: {res['resultSizeEstimate']}."
        )

    # while "nextPageToken" in res:
    #     response = req.execute()
    #     messages = response.get("messages", [])
    #     req = gmail_resource.users().messages().list_next(req, response)

    return res.get("messages", [])


def get_message(
    gmail_resource: Resource, userId: str, messageId: str, format: str
) -> dict:
    """
    Get a message from Gmail by message ID
    """
    if format not in ["minimal", "metadata", "raw", "full"]:
        raise ValueError(
            f"Invalid format: {format}. The format must be one of 'minimal', 'metadata', or 'full'."
        )

    message = (
        gmail_resource.users()
        .messages()
        .get(userId=userId, id=messageId, format=format)
        .execute()
    )

    return message


def get_messages_in_batch(
    gmail_resource: Resource,
    userId: str,
    messages: list[dict],
    format: str,
    batch_size: int = BATCH_SIZE,
) -> list[dict]:
    """
    Get messages in batch
    """

    def callback(request_id, response, exception, messages_list: list):
        if exception:
            logger.error(f"Error fetching message: {exception}")
            return

        messages_list.append(response)

    messages_loaded = []
    for i in range(0, len(messages), batch_size):
        batch_req = gmail_resource.new_batch_http_request(
            callback=partial(callback, messages_list=messages_loaded)
        )
        for message in messages[i:i + batch_size]:
            batch_req.add(
                gmail_resource.users()
                .messages()
                .get(userId=userId, id=message["id"], format=format)
            )
        batch_req.execute()

    return messages_loaded
