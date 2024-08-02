# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1

from credentials import refresh_credentials
from gmail_query import GmailQuery, search_for_new_emails
import json
import pendulum

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(".env")

if __name__ == "__main__":
    service = refresh_credentials(os.environ.get("GMAIL_CREDENTIALS_PATH"))
    
    q = GmailQuery(from_='no-reply@clickbus.com.br', subject='Pedido confirmado')
    print(q.to_gmail_string())
    
    req = (
        service.users()
        .messages()
        .list(userId="me", q=q.to_gmail_string())
    )
    
    print(req.execute())
    
    
    # emails = search_for_new_emails(service, since=pendulum.now().subtract(hours=6), only_unread=True)
    # print(emails)

    # Lets create a label called 'Clickbus' and apply it to the email with from tag as no-reply@clickbus.com.br
    # Checking if the label already exists
    # labels = service.users().labels().list(userId="me").execute()["labels"]

    # Creating label
    # if "Clickbus" not in (l["name"] for l in labels):
    #     res = (
    #         service.users()
    #         .labels()
    #         .create(
    #             userId="me",
    #             body={
    #                 "id": "Clickbus",
    #                 "name": "Clickbus",
    #                 "color": {
    #                     "textColor": "#f3f3f3",
    #                     "backgroundColor": "#d0bcf1",
    #                 },
    #                 "messageListVisibility": "show",
    #                 "labelListVisibility": "labelShow",
    #                 "type": "user",
    #             },
    #         )
    #         .execute()
    #     )

    # clickbus_label = next((l for l in labels if l["name"] == "Clickbus"), res)

    # if not clickbus_label:
    #     raise ValueError("Label Clickbus was not created")

    # # Query for ClickBus emails
    # req = (
    #     service.users()
    #     .messages()
    #     .list(userId="me", q="from:(no-reply@clickbus.com.br)")
    # )

    # emails = []

    # # Does a paginated search for all emails from ClickBus
    # while req is not None:
    #     res = req.execute()
    #     emails.extend(res["messages"])

    #     req = service.users().messages().list_next(req, res)

    # req = (
    #     service.users()
    #     .messages()
    #     .batchModify(
    #         userId="me",
    #         body={
    #             "addLabelIds": [clickbus_label["id"]],
    #             "ids": [e["id"] for e in emails],
    #         },
    #     )
    # )

    # res = req.execute()

    # clickbus_response = service.users().messages().list(userId='me', q="from:(no-reply@clickbus.com.br)").execute()
    service.close()
