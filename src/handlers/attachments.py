from pathlib import Path
from pendulum import DateTime
from google.cloud import storage
from typing import Self, Callable


class BaseAttachmentHandler:
    def __init__(self) -> None:
        pass

    def execute(self, attachment: dict) -> None:
        raise NotImplementedError("Method not implemented")


class AttachmentHandler:
    def __init__(self) -> None:
        self._execution_plan: list[Callable[[dict], None]] = []

    def save_locally(self, downloads_dir: Path | str, fail_if_file_exists=False) -> Self:
        """Add a handler to save attachments locally.

        Args:
            downloads_dir (Path | str): Directory where the attachments will be saved.
            fail_if_file_exists (bool, optional): Fails if the execute method tries to override a existing file. Defaults to False.

        Returns:
            Self: Returns itself to allow method chaining.
        """
        self._execution_plan.append(
            lambda x: save_attachment_locally(
                downloads_dir, x, fail_if_file_exists)
        )

        return self

    def write_on_cloud_storage(self, bucket: storage.Bucket, path: str = '') -> Self:
        """Add a handler to write attachments on a cloud storage bucket.

        Args:
            bucket (storage.Bucket): Google Cloud Bucket object to write on.
            path (str, optional): Directory path. Defaults to ''.

        Returns:
            Self: Returns itself to allow method chaining.
        """
        self._execution_plan.append(
            lambda x: write_attachment_on_cloud_storage(bucket, x, path)
        )

        return self

    def execute(self, attachment: dict) -> None:
        for handler in self._execution_plan:
            handler(attachment)


def save_attachment_locally(downloads_dir: Path | str, attachment: dict, fail_if_file_exists=False) -> None:
    """Save an attachment locally.

    Args:
        downloads_dir (Path | str): Directory where the attachments will be saved.
        attachment (dict): Attachment dictionary. Must have the following keys:
            - filename (str): The name of the file.
            - date (pendulum.DateTime): The date when the attachment was sent.
            - data (bytes): The attachment data.
        fail_if_file_exists (bool, optional): Fails if the execute method tries to override a existing file. Defaults to False.

    Raises:
        FileExistsError: If the file already exists and fail_if_file_exists is True.
    """
    downloads_dir = Path(downloads_dir)
    downloads_dir.mkdir(exist_ok=True, parents=True)

    filename, extension = attachment['filename'].split('.')
    complete_filename = f'{
        attachment['date'].to_date_string()}-{filename}.{extension}'
    file_path = downloads_dir/complete_filename

    if file_path.exists() and fail_if_file_exists:
        raise FileExistsError(
            f"File {file_path.as_posix()} already exists")

    file_path.write_bytes(attachment['data'])


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
        raise ValueError(f"Couldn't save attachment {
                         attachment['filename']}. Bucket {bucket.name} doesn't exist")

    if path.endswith('/'):
        raise ValueError(f"Path {path} must not end with '/'")

    filename, extension = attachment['filename'].split('.')
    complete_filename = f'{
        attachment['date'].to_date_string()}-{filename}.{extension}'
    blob = bucket.blob(path + '/' + complete_filename)

    blob.upload_from_string(attachment['data'])
