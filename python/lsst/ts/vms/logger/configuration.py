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

from pathlib import Path

import yaml
from pydantic import BaseModel

__all__ = ["load_configuration"]


class Logger(BaseModel):
    workdir: str
    h5py: bool = True
    template: str
    rotate: str


class S3(BaseModel):
    endpoint: str = ""
    bucket_name: str = ""
    prefix: str = ""
    profile: str = ""
    time_window: str = ""
    delete: bool = False


class Configuration(BaseModel):
    logger: Logger
    s3: S3


def load_configuration(config_file: Path) -> Configuration:
    with open(config_file, "r") as f:
        data = yaml.safe_load(f)

    return Configuration.model_validate(data)
