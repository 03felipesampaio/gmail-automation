from datetime import datetime

class GmailQuery:
    """A Gmail query constructor to convert the query parameters into strings or to check if a new email matches the query parameters.
    
    The queries follow Gmail official documentation: https://support.google.com/mail/answer/7190?hl=en
    """
    
    def __init__(self, from_: str|None = None, to: str|None = None, subject: str|None = None, label: str|None = None, has: str|None = None, filename: str|None = None):
        # Constructor code here
        self.from_ = from_
        self.to = to
        self.subject = subject
        self.label = label
        self.has = has
        self.filename = filename
        
    def to_gmail_string(self) -> str:
        """Converts the query parameters into a Gmail query string."""
        query = ''
        if self.from_:
            query += f'from:{self.from_} '
        if self.to:
            query += f'to:{self.to} '
        if self.subject:
            query += f'subject:{self.subject} '
        if self.label:
            query += f'label:{self.label} '
        if self.has:
            query += f'has:{self.has} '
        if self.filename:
            query += f'filename:{self.filename} '
        
        return query.strip()
    

def search_for_new_emails(service, userId: str = 'me', since: datetime = None, only_unread: bool = False) -> dict:
    ...
