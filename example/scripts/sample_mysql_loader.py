# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.

"""

import logging
import sys
import textwrap
import uuid

from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.mysql_metadata_extractor import MysqlMetadataExtractor
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
SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
Base = declarative_base()

NEO4J_ENDPOINT = f'bolt://neo4j:7687'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = ''
neo4j_password = ''
neo4j_database = 'testsql'

LOGGER = logging.getLogger(__name__)


# todo: connection string needs to change
def connection_string():
    user = 'root'
    password = 'galileo'
    host = 'mysql'
    port = '3306'
    db = 'classicmodels'
    return "mysql://%s:%s@%s:%s/%s" % (user, password, host, port, db)


def run_mysql_job():
    where_clause_suffix = textwrap.dedent("""
        where c.table_schema = 'mysql'
    """)

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    job_config = ConfigFactory.from_dict({
        f'extractor.mysql_metadata.{MysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.mysql_metadata.{MysqlMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME}': True,
        f'extractor.mysql_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connection_string(),
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4j_endpoint,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4j_user,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4j_password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': neo4j_database,
        f'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'testSQL',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=MysqlMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    return job


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    logging.basicConfig(level=logging.INFO)
    loading_job = run_mysql_job()
    loading_job.launch()
