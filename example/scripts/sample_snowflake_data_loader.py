# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data into neo4j without using Airflow DAG.
"""

import logging
import os
import sys
import uuid

from elasticsearch.client import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.snowflake_metadata_extractor import SnowflakeMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Disable snowflake logging
logging.getLogger("snowflake.connector.network").disabled = True

SNOWFLAKE_DATABASE_KEY = 'YourSnowflakeDbName'

IGNORED_SCHEMAS = ['\'DVCORE\'', '\'INFORMATION_SCHEMA\'', '\'STAGE_ORACLE\'']

# todo: connection string needs to change
def connection_string():
    # Refer this doc: https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    # for supported connection parameters and configurations
    user = 'username'
    password = 'password'
    account = 'YourSnowflakeAccountHere'
    # specify a warehouse to connect to.
    warehouse = 'yourwarehouse'
    return f'snowflake://{user}:{password}@{account}/{SNOWFLAKE_DATABASE_KEY}?warehouse={warehouse}'


def run_snowflake_job(neo4jConfig, connectionString: str, targetDbName: str):
    where_clause = f"WHERE c.TABLE_SCHEMA not in ({','.join(IGNORED_SCHEMAS)}) \
            AND c.TABLE_SCHEMA not like 'STAGE_%' \
            AND c.TABLE_SCHEMA not like 'HIST_%' \
            AND c.TABLE_SCHEMA not like 'SNAP_%' \
            AND lower(c.COLUMN_NAME) not like 'dw_%';"

    tmp_folder = '/var/tmp/amundsen/tables'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = SnowflakeMetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor,
                       loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        f'extractor.snowflake.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connectionString,
        f'extractor.snowflake.{SnowflakeMetadataExtractor.SNOWFLAKE_DATABASE_KEY}': SNOWFLAKE_DATABASE_KEY,
        f'extractor.snowflake.{SnowflakeMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4jConfig.uri,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4jConfig.username,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4jConfig.password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': targetDbName,
        f'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag'
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    
    try:
        job.launch()
    except Exception as exceptionInstance:
        raise Exception(str(exceptionInstance))
