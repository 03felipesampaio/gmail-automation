from pathlib import Path
from pendulum import DateTime
from google.cloud import storage
from typing import Self

class BaseAttachmentHandler:
    def __init__(self) -> None:
        pass

    def execute(self, attachment: dict) -> None:
        raise NotImplementedError("Method not implemented")


class SaveLocallyAttachmentHandler (BaseAttachmentHandler):
    def __init__(self, downloads_dir: Path | str, fail_if_file_exists=False) -> None:
        """Creates a handler to save attachments locally. Pass the directory where the attachments will be saved.
        After that, pass the attachment dictionary to the execute method.

        The directory and it parents will be created if they don't exist.

        Args:
            downloads_dir (Path | str): Directory where the attachments will be saved.
            fail_if_file_exists (bool, optional): Fails if the execute method tries to override a existing file. Defaults to False.

        Raises:
            Exception: If the directory can't be created.
        """
        self.downloads_dir = Path(downloads_dir)
        self.fail_if_file_exists = fail_if_file_exists

        try:
            self.downloads_dir.mkdir(exist_ok=True, parents=True)
        except Exception as e:
            raise Exception(f"Error creating downloads directory {
                            self.downloads_dir.as_posix()}") from e

    def execute(self, attachment: dict) -> None:
        """Run the handler to save the attachment locally.

        Args:
            attachment (dict): Attachment dictionary. Must have the following keys:
                - filename (str): The name of the file.
                - date (pendulum.DateTime): The date when the attachment was sent.
                - data (bytes): The attachment data.

        """
        filename, extension = attachment['filename'].split('.')
        complete_filename = f'{
            filename}-{attachment['date'].to_date_string()}.{extension}'
        file_path = self.downloads_dir/complete_filename

        if file_path.exists() and self.fail_if_file_exists:
            raise FileExistsError(
                f"File {file_path.as_posix()} already exists")

        file_path.write_bytes(attachment['data'])


class AttachmentHandler:
    def __init__(self) -> None:
        self._execution_plan = []
    
    def write_on_cloud_storage(self, bucket: storage.Bucket, attachment: dict, path: str = '') -> Self:
        write_attachment_on_cloud_storage(bucket, attachment, path)
    
    def execute(self, attachment: dict) -> None:
        ...
    

def write_attachment_on_cloud_storage(bucket: storage.Bucket, attachment: dict, path: str = '') -> None:
    """Write an attachment on a cloud storage bucket.

    Args:
        bucket (Bucket): Google Cloud Bucket.
        attachment (dict): Attachment dictionary. Must have the following keys:
            - filename (str): The name of the file.
            - date (pendulum.DateTime): The date when the attachment was sent.
            - data (bytes): The attachment data.
    """
    if not bucket.exists():
        raise ValueError(f"Couldn't save attachment {attachment['filename']}. Bucket {bucket.name} doesn't exist")
    
    if path.endswith('/'):
        raise ValueError(f"Path {path} must not end with '/'")
    
    filename, extension = attachment['filename'].split('.')
    complete_filename = f'{attachment['date'].to_date_string()}-{filename}.{extension}'
    blob = bucket.blob(path + '/' + complete_filename)
    blob.upload_from_string(attachment['data'])