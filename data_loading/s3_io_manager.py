from prefect_aws.s3 import S3Bucket
import json
import tempfile
import os
import asyncio

class S3IOManager:
    """
    S3IOManager encapsulates logic for handling I/O operations with scraped data and S3.
    """

    def __init__(self, bucket_name: str):
        """
        Initializes the S3IOManager with the specified S3 bucket.

        Args:
            bucket_name (str): Name of the S3 bucket Prefect block.
        """
        # Load the S3 bucket using the Prefect block name
        self.s3_bucket = S3Bucket.load(bucket_name)
        # Create a temporary directory for storing intermediate JSON files
        self.temp_dir = tempfile.TemporaryDirectory()

    def __del__(self):
        """
        Destructor to clean up the temporary directory.
        """
        if self.temp_dir:
            self.temp_dir.cleanup()

    def list_bucket_contents(self) -> list:
        """
        Lists items in the S3 bucket along with their metadata.

        Returns:
            list: List of items in the S3 bucket.
        """
        return self.s3_bucket.list_objects()

    def save_data_to_json(self, data: dict, filename: str, directory: str = None) -> None:
        """
        Saves the given data to a JSON file in a temporary directory or a specified directory.

        Args:
            data (dict): Data to be saved.
            filename (str): Name of the JSON file.
            directory (str, optional): Directory to save the JSON file. Defaults to None.
        """
        # Ensure the filename ends with .json
        if not filename.endswith('.json'):
            filename = filename + '.json'

        # Determine the path to save the file
        save_path = os.path.join(directory if directory else self.temp_dir.name, filename)

        # Write the data to a JSON file
        with open(save_path, 'w') as file:
            json.dump(data, file)

    async def upload_file_to_s3(self, filepath: str) -> None:
        """
        Uploads a single file to the S3 bucket.

        Args:
            filepath (str): Path to the file to upload.
        """
        await self.s3_bucket.upload_from_path(filepath)

    async def upload_files_to_s3(self, filepaths: list) -> None:
        """
        Asynchronously uploads multiple files to the S3 bucket.

        Args:
            filepaths (list): List of file paths to upload.
        """
        tasks = [self.upload_file_to_s3(filepath) for filepath in filepaths]
        await asyncio.gather(*tasks)

    def upload_directory_to_s3(self, directory: str = None) -> None:
        """
        Uploads all files in a directory (or the temp directory) to the S3 bucket.

        Args:
            directory (str, optional): Directory of files to upload. Defaults to the temp directory.
        """
        # Determine the directory to upload from
        upload_dir = directory if directory else self.temp_dir.name

        # Gather all file paths in the directory
        filepaths = [os.path.join(upload_dir, filename) for filename in os.listdir(upload_dir)]

        # Asynchronously upload all files
        asyncio.run(self.upload_files_to_s3(filepaths))

if __name__ == '__main__':
    # Example usage
    # s3_manager = S3IOManager('my_s3_bucket_block')
    # data = {'key1': 'value1', 'key2': 'value2'}
    # s3_manager.save_data_to_json(data, 'example')
    # s3_manager.upload_directory_to_s3()
    pass
