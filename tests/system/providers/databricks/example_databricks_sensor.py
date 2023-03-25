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
from __future__ import annotations

import os
import textwrap
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.sensors.sql import DatabricksSqlSensor

# [Env variable to be used from the OS]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
# [DAG name to be shown on Airflow UI]
DAG_ID = "example_databricks_sensor"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    dag.doc_md = textwrap.dedent(
        """

        This is an example DAG which uses the DatabricksSqlSensor
        sensor. The example task in the DAG executes the provided
        SQL query against the Databricks SQL warehouse and if a
        result is returned, the sensor returns True/succeeds.
        If no results are returned, the sensor returns False/
        fails.

        """
    )
    # [START howto_sensor_databricks_connection_setup]
    # Connection string setup for Databricks workspace.
    connection_id = "databricks_default"
    sql_endpoint_name = "Starter Warehouse"
    # [END howto_sensor_databricks_connection_setup]

    # [START howto_sensor_databricks_sql]
    # Example of using the Databricks SQL Sensor to check existence of data/partitions for a Delta table.
    sql_sensor = DatabricksSqlSensor(
        databricks_conn_id=connection_id,
        sql_endpoint_name=sql_endpoint_name,
        catalog="hive_metastore",
        task_id="sql_sensor_task",
        sql="select * from hive_metastore.temp.sample_table_3 limit 1",
        timeout=60 * 2,
    )
    # [END howto_sensor_databricks_sql]

    (sql_sensor)

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
