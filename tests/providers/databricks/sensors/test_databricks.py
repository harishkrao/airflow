#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from __future__ import annotations
import unittest
from unittest import mock

from airflow.providers.databricks.sensors.databricks import DatabricksSQLSensor
# from databricks.sql.types import Row

from datetime import datetime, timedelta

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)


class TestDatabricksSQLSensor(unittest.TestCase):
    @mock.patch("airflow.providers.databricks.sensors.databricks.DatabricksSqlHook")
    def test_exec_success_sensor_find_new_events(self, db_mock_class):
        sensor = DatabricksSQLSensor(
            task_id=TASK_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            db_sensor_type="table_changes",
            table_name=DEFAULT_TABLE,
            timestamp=TIMESTAMP_TEST,
        )

        db_mock = db_mock_class.return_value
        db_mock.get_table_version.return_value = 123

        with mock.patch("airflow.providers.databricks.sensors.databricks.DatabricksSQLSensor") as mock_sensor:
            mock_sensor.get_previous_version.return_value = 122
            mock_sensor.set_version.return_value = None
            result = sensor.poke(None)
            assert result

        db_mock_class.assert_called_once_with(
            databricks_conn_id="databricks_default",
            catalog="catalog1",
            schema="schema1",
            table_name="table1",
            db_sensor_type="table_changes",
            timestamp=datetime.now() - timedelta(days=30),
        )

        db_mock.get_table_version.assert_called_once_with(
            DEFAULT_CONN_ID, DEFAULT_CATALOG, DEFAULT_TABLE, DEFAULT_SCHEMA
        )
