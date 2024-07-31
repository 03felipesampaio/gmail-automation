from datetime import datetime

class GmailQuery:
    def __init__(self, from_: str|None = None, to: str|None = None, subject: str|None = None, label: str|None = None, has: str|None = None, filename: str|None = None):
        # Constructor code here
        self.from_ = from_

    def search_emails(self, query):
        # Method code here
        pass

    def mark_as_read(self, email_id):
        # Method code here
        pass

    def delete_email(self, email_id):
        # Method code here
        pass

    # Add more methods as needed
    

def search_for_new_emails(service, userId: str = 'me', since: datetime = None, only_unread: bool = False) -> dict:
    ...
