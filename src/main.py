# Gmail API Documentation by Google => https://googleapis.github.io/google-api-python-client/docs/dyn/gmail_v1

from credentials import refresh_credentials
import json

if __name__ == '__main__':
    service = refresh_credentials()
    
    # print(dir(service.users().getProfile(userId='me')))
    clickbus_response = service.users().messages().list(userId='me', q="from:(no-reply@clickbus.com.br)").execute()
    email_response = service.users().messages().get(userId='me', id=clickbus_response['messages'][0]['id']).execute()
    
    with open('emailResponse.json', 'w') as fp:
        fp.write(json.dumps(email_response, ensure_ascii=False, indent=4))
    
    
    # for email_id in clickbus_response['messages']:
    #     email_response = service.users().messages().get(userId='me', id=email_id['id']).execute()
        # print(email_id)
    
    # print(email_response)
    service.close()
    