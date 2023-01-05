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
"""This module contains Databricks sensors."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context


class DatabricksSQLSensor(BaseSensorOperator):
    """
    Sensor to check for specified conditions on Databricks Delta tables.

    :param databricks_conn_id:str=DatabricksSqlHook.default_conn_name: Specify the name of the connection
    to use with Databricks on Airflow
    :param http_path:str: Specify the path to the sql endpoint
    :param sql_endpoint_name:str: Specify the name of the sql endpoint to use
    :param session_configuration: Pass in the session configuration to be used
    :param http_headers:list[tuple[str, str]]: Pass http headers to the databricks API
    :param catalog:str|None=None: Specify the catalog to use for the query
    :param schema:str|None="default": Specify the schema of the table to be queried
    :param table_name:str: Specify the table that we want to monitor
    :param partition_name:str|: Pass in the partition name to be used for the sensor
    :param handler:Callable[[Any, Any]=fetch_all_handler: Define the handler function that will be used
    to process the results of a query
    :param db_sensor_type:str: Choose the sensor you want to use. Available options: table_partition,
    table_changes.
    :param timestamp:datetime: To be used with query filters or as other argument values for timestamp
    :param caller: Identify the source of this sensor in logs
    :param client_parameters:dict[str, Any]: Additional client parameters
    """

    def __init__(
        self,
        *,
        databricks_conn_id: str = DatabricksSqlHook.default_conn_name,
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration=None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str = "",
        schema: str | None = "default",
        table_name: str,
        partition_name: list | None = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        db_sensor_type: str,
        timestamp: datetime = datetime.now() - timedelta(days=7),
        caller: str = "DatabricksSQLSensor",
        client_parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = "default" if not schema else schema
        self.table_name = table_name
        self.partition_name = partition_name
        self.db_sensor_type = db_sensor_type
        self.timestamp = timestamp
        self.caller = caller
        self.client_parameters = client_parameters or {}
        self.hook_params = kwargs.pop("hook_params", {})
        self.handler = handler

    def _get_hook(self) -> DatabricksSqlHook:
        return DatabricksSqlHook(
            self.databricks_conn_id,
            self._http_path,
            self._sql_endpoint_name,
            self.session_config,
            self.http_headers,
            self.catalog,
            self.schema,
            self.caller,
            **self.client_parameters,
            **self.hook_params,
        )

    @staticmethod
    def _partition_concatenate(partition_values) -> str:
        # TODO: Add loop for multiple partition items returned from the delta table.
        if isinstance(partition_values, list):
            raise AirflowException("Please supply a list of values.")
        if len(partition_values) >= 1:
            for value in partition_values:
                if isinstance(value, datetime):
                    value = value.strftime("%Y-%m-%d")
                elif isinstance(value, (int, float, complex)):
                    value = str(value)
                partition_values.append(value)
        partition_str = ", ".join(partition_values)
        return partition_str

    def _check_table_partitions(self, hook) -> bool:
        if self.catalog is not None:
            complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
            self.log.info("Table name generated from arguments: %s", complete_table_name)
        result = hook.run(
            f"SHOW PARTITIONS {complete_table_name}",
            handler=self.handler if self.do_xcom_push else None,
        )
        partition_name_str = self._partition_concatenate(self.partition_name)
        result_str = self._partition_concatenate(result)

        # if len(result) < 1:
        #     raise AirflowException(
        #         "Table did not have partitions, please check if "
        #         "partitions are defined or if data exists in the table."
        #     )
        # self.log.debug("Executed sensor query to check partitions and received response %s", result)
        # record = result[0]
        # return self.partition_name in record

        return True

    def _check_table_changes(self, hook) -> bool:
        if self.catalog is not None:
            complete_table_name = str(self.catalog + "." + self.schema + "." + self.table_name)
            self.log.info("Table name generated from arguments: %s", complete_table_name)
        else:
            raise AirflowException("Catalog name not specified, aborting query execution.")
        result = hook.run(
            f"""SELECT COUNT(version) as versions from
        (DESCRIBE HISTORY {complete_table_name})
        WHERE timestamp >= \'{self.timestamp}\'""",
            handler=self.handler if self.do_xcom_push else None,
        )
        result_value = result[0][0]
        if result_value > 0:
            return True
        else:
            return False

    template_fields: Sequence[str] = (
        "table_name",
        "schema",
        "partition_name",
    )

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()
        if self.db_sensor_type == "table_partition":
            return self._check_table_partitions(hook)
        elif self.db_sensor_type == "table_changes":
            return self._check_table_changes(hook)
        return False
