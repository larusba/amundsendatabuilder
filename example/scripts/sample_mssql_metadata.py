# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch from MS SQL Server
without using an Airflow DAG.

"""

import logging
import sys
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.mssql_metadata_extractor import MSSQLMetadataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

DB_FILE = '/tmp/test.db'
Base = declarative_base()

LOGGER = logging.getLogger(__name__)


# todo: connection string needs to change
def connection_string(windows_auth=False):
    """Generages an MSSQL connection string.


    Keyword Arguments:
        windows_auth {bool} -- set to true if connecting to DB using windows
                                credentials. (default: {False})

    Returns:
        [str] -- [connection string]
    """

    if windows_auth:
        base_string = "mssql+pyodbc://@{host}/{db}" \
                      "?driver=ODBC+Driver+17+for+SQL+Server" \
                      "?trusted_connection=yes&autocommit=true"  # comment to disable autocommit.
        params = {
            "host": "localhost",
            "db": "master"
        }

    else:
        base_string = "mssql+pyodbc://{user}:{pword}@{host}/{db}" \
                      "?driver=ODBC+Driver+17+for+SQL+Server" \
                      "&autocommit=true"  # comment to disable autocommit.
        params = {
            "user": "username",
            "pword": "password",
            "host": "localhost",
            "db": "master"
        }

    return base_string.format(**params)


def run_mssql_job(neo4jConfig, connectionString: str, targetDbName: str):
    where_clause_suffix = "('dbo')"

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    job_config = ConfigFactory.from_dict({
        # MSSQL Loader
        f'extractor.mssql_metadata.{MSSQLMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.mssql_metadata.{MSSQLMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME}': True,
        f'extractor.mssql_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connectionString,
        # NEO4J Loader
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4jConfig.uri,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4jConfig.username,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4jConfig.password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': targetDbName,
        f'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag',  # should use unique tag here like {ds}
    })

    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=MSSQLMetadataExtractor(),
            loader=FsNeo4jCSVLoader()),
        publisher=Neo4jCsvPublisher())
    
    try:
        job.launch()
    except Exception as exceptionInstance:
        raise Exception(str(exceptionInstance))