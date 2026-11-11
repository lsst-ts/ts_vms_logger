# This file is part of ts_vms_logger.
#
# Developed for the Vera C. Rubin Observatory Telescope and Site Systems.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import asyncio
import hashlib
import logging
import os
import time
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def calculate_md5(file_path: Path) -> str:
    """
    Calculate MD5 checksum of a local file.

    Parameters
    ----------
    file_path : `str`
        File path, for which MD5 checksum shall be calculated.

    Returns
    -------
    md5 : `str`
       Calculated MD5 checksum hash.
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        # Read in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class S3Synchronizer:
    """
    Upload local files to S3 service.

    Parameters
    ----------
    base_dir : `Path`
        Local directory path to synchronize.
    bucket_name : `str`
        Destination S3 bucket name.
    s3_prefix : `str`, optional
        Optional folder path inside the S3 bucket. Defaults to empty string.
    time_window : `int`
        Sync files modified within this many seconds (default 2 days = 2 *
        86400s).
    profile_name : `str`, optional
        AWS profile name. AWS credential file is used, and profile is set to
        what is provided. Defaults to "default".
    """

    def __init__(
        self,
        base_dir: Path,
        bucket_name: str,
        s3_prefix: str = "",
        time_window: int = 2 * 86400,
        profile_name: str = "default",
    ):
        self._base_dir = base_dir
        self._bucket_name = bucket_name
        self._s3_prefix = s3_prefix
        self._time_window = time_window

        self._session = boto3.Session(profile_name=profile_name)

    def check_and_sync_file(self, local_path: Path, s3_key: str, delete_after_sync: bool) -> None:
        """
        Synchronous worker function that handles the heavy lifting.
        Instantiates its own boto3 client for thread safety.
        """
        s3_client = self._session.client("s3")
        s3_client.create_bucket(Bucket=self._bucket_name)

        local_md5 = calculate_md5(local_path)
        needs_upload = False

        try:
            response = s3_client.head_object(Bucket=self._bucket_name, Key=s3_key)
            s3_etag = response["ETag"].strip('"')

            if "-" in s3_etag or local_md5 != s3_etag:
                needs_upload = True

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                needs_upload = True
            else:
                logging.error(f"Could not check {s3_key}: {e}")
                return

        if needs_upload:
            logging.info(f"Uploading {local_path} to s3://{self._bucket_name}/{s3_key}")
            try:
                s3_client.upload_file(local_path, self._bucket_name, s3_key)
                if delete_after_sync:
                    os.remove(local_path)
                    logging.debug(f"Deleted {local_path}")
            except Exception as e:
                logging.error(f"Upload failed for {local_path}: {e}")
        else:
            logging.info(f"Skipping {local_path} - it is up to date.")

    async def process_file_async(
        self, semaphore: asyncio.Semaphore, local_path: Path, s3_key: str, delete_after_sync: bool
    ) -> None:
        """
        Async wrapper that limits concurrency and runs the blocking worker in a
        thread.
        """
        async with semaphore:
            # Offload the blocking I/O and network operations to the default
            # ThreadPoolExecutor
            await asyncio.to_thread(self.check_and_sync_file, local_path, s3_key, delete_after_sync)

    async def sync_directory(self, delete_after_sync: bool = False, max_concurrency: int = 10) -> None:
        """
        Asynchronously traverses the directory and schedules non-blocking sync
        tasks.
        """
        current_time = time.time()

        # Restrict concurrent uploads so we don't exhaust system file
        # descriptors or S3 connections
        semaphore = asyncio.Semaphore(max_concurrency)
        tasks = []

        # os.walk is extremely fast for standard local file systems,
        # but the actual file processing is added to the asyncio task loop.
        for root, dirs, files in os.walk(self._base_dir):
            for file in files:
                local_path = Path(root) / file

                file_mtime = os.path.getmtime(local_path)
                if (current_time - file_mtime) < self._time_window:
                    logging.info(
                        f"File {local_path} is not old enough: file mtime: {file_mtime} now: {current_time}"
                    )
                    continue

                relative_path = os.path.relpath(local_path, self._base_dir).replace("\\", "/")
                s3_key = (
                    f"{self._s3_prefix.rstrip('/')}/{relative_path}" if self._s3_prefix else relative_path
                )

                # Create a non-blocking task for each file
                task = asyncio.create_task(
                    self.process_file_async(semaphore, local_path, s3_key, delete_after_sync)
                )
                tasks.append(task)

        if tasks:
            # Wait for all background thread tasks to complete
            await asyncio.gather(*tasks)
            logging.info(f"Successfully processed {len(tasks)} files.")
        else:
            logging.info("No files found matching the time window criteria.")
