from json import JSONDecodeError
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

class GDriveAuth:
  SCOPES = ["https://www.googleapis.com/auth/drive"]

  # creds = None
  def __init__(self):
    """
    Initializes the Auth class.

    This method checks if the token.json file exists and loads the credentials from it.
    If the credentials are not valid or expired, it initiates the authorization flow
    to obtain new credentials and saves them to the token.json file.

    Args:
      None

    Returns:
      None
    """

    try:
      if os.path.exists("token.json"):
        self.creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)
    except JSONDecodeError as e:
      print(e)
      self.creds = None
      
    if not self.creds or not self.creds.valid:
      if self.creds and self.creds.expired and self.creds.refresh_token:
        self.creds.refresh(Request())
      else:
        flow = InstalledAppFlow.from_client_secrets_file(
          r'scripts\gdrive\credentials.json', self.SCOPES
        )
        creds = flow.run_local_server(port=0)

      with open('token.json', 'w') as token:
        token.write(creds.to_json())