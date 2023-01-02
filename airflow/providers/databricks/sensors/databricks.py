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

from datetime import datetime
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
    :param partition_names:list[Any]|: Pass in a list of partition names to be used for the sensor
    :param handler:Callable[[Any, Any]=fetch_all_handler: Define the handler function that will be used
    to process the results of a query
    :param db_sensor_type:str: Choose the sensor you want to use. Available options: table_partition,
    table_changes.
    :param timestamp:datetime: To be used with query filters or as other argument values for timestamp
    :param caller: Identify the source of this sensor in logs
    :param client_parameters:dict[str, Any]: Allow the sensor to be used with both
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
        partition_names: list[Any] | None = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        db_sensor_type: str,
        timestamp: datetime,
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
        self.partition_names = partition_names
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
    def get_previous_version(context: Context, lookup_key):
        return context["ti"].xcom_pull(key=lookup_key, include_prior_dates=True)

    @staticmethod
    def set_version(context: Context, lookup_key, version):
        context["ti"].xcom_push(key=lookup_key, value=version)

    def _check_table_partitions(self, hook) -> bool:
        if not isinstance(self.partition_names, list):
            raise AirflowException("Partition names must be specified as a list, even for single values.")
        result = hook.run(
            f"SHOW PARTITIONS {self.schema}.{self.table_name}",
            handler=self.handler if self.do_xcom_push else None,
        )
        self.log.info("Executed sensor query to check partitions.")
        if len(self.partition_names) > 0:
            for partition in self.partition_names:
                if partition not in result[0]:
                    return False
            return True
        else:
            raise AirflowException("At least one partition name required for comparison!")

    def _check_table_changes(self, hook, context: Context) -> bool:
        # try:
        if self.catalog is not None:
            table_full_name = self.catalog + "_" + self.schema + "_" + self.table_name
        else:
            raise AirflowException("Catalog name not specified, aborting query execution.")
        version = hook.run(
            f"SELECT MAX(version) as current_version from (DESCRIBE HISTORY {table_full_name}) "
            f"WHERE timestamp > {self.timestamp}",
        )
        self.log.info("Version for %s is '%s'", table_full_name, version)
        prev_version = -1
        if context is not None:
            lookup_key = table_full_name
            prev_data = self.get_previous_version(context, lookup_key)
            self.log.debug("prev_data: %s, type=%s", str(prev_data), type(prev_data))
            if isinstance(prev_data, int):
                prev_version = prev_data
            elif prev_data is not None:
                raise AirflowException("Incorrect type for previous XCom Data: %s", type(prev_data))
            if prev_version != version:
                self.set_version(context, lookup_key, version)

        return prev_version < version
        # except AirflowException as exc:
        #     raise exc

    template_fields: Sequence[str] = (
        "table_name",
        "schema",
        "partition_names",
    )

    def poke(self, context: Context) -> bool:
        hook = self._get_hook()
        if self.db_sensor_type == "table_partition":
            return self._check_table_partitions(hook)
        elif self.db_sensor_type == "table_changes":
            return self._check_table_changes(hook, context)
        else:
            return False
