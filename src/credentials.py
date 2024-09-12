from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import googleapiclient.http
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
import google_auth_httplib2
import httplib2

import os


SCOPES = ["https://mail.google.com/"]


def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(
        http.credentials, http=httplib2.Http(cache='.cache'))
    return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)


def refresh_credentials(credential_path: str) -> Resource:
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credential_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        # Call the Gmail API
        authorized_http = google_auth_httplib2.AuthorizedHttp(
            creds, http=httplib2.Http(cache='.cache'))
        service = build("gmail", "v1", http=authorized_http,
                        requestBuilder=build_request)

    except HttpError as error:
        # TODO(developer) - Handle errors from gmail API.
        print(f"An error occurred: {error}")

    return service
