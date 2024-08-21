# Gmail Automation

Managing your emails can be a tedious process. 

But why are you spending all this time manually labeling, forwarding, deleting and downloding attachments when **we can handle those things to you**?

This is a self-hosted app to manage all your Gmail messages.

## How it works

First you connect your Gmail account with OAuth2 so the app can start to monitor incoming and old messages.

You create classfiers and defines a handler (a response). When a message that matches the classfier is found, the script will execute the handler.

Create as much classfiers that you need!

### Classfiers

Classfiers are the core of this app. To work properly they need a name to distiguish themselves, a query (or filter) and a handler (a function). You can find the GmailClassfier class at src/gmail.py file.

Ex.:
```python
from gmail import GmailClassfier

# Creates a classfier named Uber, that matches messages which the sender name is 'Uber' 
# and when a message is found, it is printed
classfier = GmailClassfier(name='Uber', query='from:Uber', handler=print)
# Or simply
classfier = GmailClassfier('Uber', 'from:Uber', print)
```

### Queries

To Gmail, queries and filters are separeted things. We use queries to match messagesm and filters are like triggers that someone can define. We just use queries here.

A Gmail query has a string format where you can specify the type of message that you are looking for.  

You can find information about queries [here.](https://support.google.com/mail/answer/7190?hl=en&ref_topic=3394593&sjid=4813072626133924967-SA)

Ex.:
* 'from:Uber' > Matches messages from a sender called Uber
* 'subject:Movies' > Matches messages with Movies in subject
* 'from:Uber has:attachment' >  Matches messages from a sender called Uber and has any attachment

### Handlers

## Setup
* Setup your enviroment from https://developers.google.com/gmail/api/quickstart/python . Go until Set your enviroment part
* Create a .env file
* Get your OAuth file with credentials from Google Cloud and save the path in .env file as GMAIL_CREDENTIALS_PATH 
* Create your Python venv and install libs from requirements.txt