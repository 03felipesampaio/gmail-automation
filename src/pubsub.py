from pymongo.collection import Collection
from googleapiclient.discovery import Resource

from google.cloud import pubsub_v1
import os
from pprint import pprint
import json
import logging
import database
import gmail

logger = logging.getLogger("gmail_automation")

def start_gmail_publisher(gmail_service, userId, pubsub_topic_name: str) -> dict:
    """Starts to watch for new messages on Gmail API.
    When a new message is found, the Gmail API sends a message to the Pub/Sub topic.
    
    After running this function, you should setup a Pub/Sub subscriber to handle the new messages.
    
    Docs: https://developers.google.com/gmail/api/guides/push

    Args:
        gmail_service (Resouce): Gmail resource from googleapiclient.discovery
        userId (str): User ID to watch for new messages.
        pubsub_topic_name (str): A Pub/Sub topic name to send new messages.

    Returns:
        dict: A dict with the watch response. Format:
            - "historyId" (str) : The history ID of the current mailbox state.
            - "expiration" (str) : The expiration timestamp of the watch as an int. Ex.: "1431990098200"
    """
    watch = gmail_service.users().watch(userId=userId, body={
        "topicName": pubsub_topic_name, "labelFilterAction": "include"})

    return watch.execute()


def new_message_callback(history_collection: Collection, gmail_service: Resource, userId: str, message: pubsub_v1.subscriber.message.Message) -> None:
    """Calback function to handle new messages from Pub/Sub.

    The Pub/Sub callback only accepts one argument, the message. So, we need to pass the other arguments using partials functions from functools.

    Args:
        message (PubSub message): New message from Pub/Sub.
    """
    message_data = json.loads(message.data.decode("utf-8"))
    logger.info(f"Received message: {message_data}")
    
    last_history_id = database.update_last_history_id(history_collection, "me", message_data['historyId'])

    if last_history_id is None:
        raise Exception("Last historyId not found, can't proceed without it. At this point, at least the watcher historyId should be in the database")

    if int(last_history_id) >= int(message_data['historyId']):
        logger.info(f"Message already processed: {message_data}")
        message.ack()
        return

    history_res = gmail.get_history(gmail_service, userId, last_history_id)
    
    # print(last_history_id, message_data['historyId'])
    # pprint(history_res)
       
    message_id = None
    
    for history_item in history_res["history"]:
        if "messagesAdded" not in history_item:
            continue
        
        if history_item['id'] == message_data['historyId']:
            message_id = history_item["messages"][0]["id"]
    
            # TODO Add pagination here, if a message is not in the first history response we must keep searching
            pprint(gmail_service.users().messages().get(userId="me", id=message_id, format='full').execute())
    
    # TODO The message need to pass through the classifiers
    
    message.ack()
    logger.info(f"Message processed: {message_data}")

# with pubsub_v1.SubscriberClient() as subscriber:
#     future = subscriber.subscribe(subscription=os.getenv("PUBSUB_SUBSCRIPTION"), callback=new_message_callback)
#     while True:
#         continue
#     try:
#         future.result()
#     except KeyboardInterrupt:
#         print("Shutting down...")
#         future.cancel()
#         future.result()
