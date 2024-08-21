from pathlib import Path
from pendulum import DateTime


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
