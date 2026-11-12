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

import unittest
from pathlib import Path

import boto3
from moto import mock_aws

from lsst.ts.vms.logger import S3Synchronizer
from lsst.ts.vms.logger.s3_synchronizer import calculate_md5


class S3SynchronizerTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_aws = mock_aws()
        self.mock_aws.start()

    def tearDown(self) -> None:
        self.mock_aws.stop()

    async def test_3s_synchronizer(self) -> None:
        this_script = Path(__file__).resolve()

        synchronizer = S3Synchronizer(this_script.parent, "test", "testing", 0)

        await synchronizer.sync_directory(delete_after_sync=False)

        conn = boto3.client("s3")
        response = conn.head_object(Bucket="test", Key="testing/" + this_script.name)

        assert response["ETag"] == f'"{calculate_md5(this_script)}"'


if __name__ == "__main__":
    unittest.main()
