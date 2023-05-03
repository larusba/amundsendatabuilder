# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data into neo4j without using Airflow DAG.
"""

import logging
import os
import sys
import uuid
from databuilder.models import ImportScheduling, Neo4jConfig

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.db2_metadata_extractor import Db2MetadataExtractor
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

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Disable Db2 logging
logging.getLogger("db2.connector.network").disabled = True

DB2_CONN_STRING = 'db2+ibm_db://username:password@database.host.name:50000/DB;'

IGNORED_SCHEMAS = ['\'SYSIBM\'', '\'SYSIBMTS\'', '\'SYSTOOLS\'', '\'SYSCAT\'', '\'SYSIBMADM\'', '\'SYSSTAT\'']


def create_sample_db2_job(neo4jConfig: Neo4jConfig, importScheduling: ImportScheduling):
    where_clause = f"WHERE c.TABSCHEMA not in ({','.join(IGNORED_SCHEMAS)}) ;"

    tmp_folder = '/var/tmp/amundsen/tables'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    sql_extractor = Db2MetadataExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=sql_extractor,
                       loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        f'extractor.db2_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': importScheduling.connection_string,
        f'extractor.db2_metadata.{Db2MetadataExtractor.DATABASE_KEY}': 'DEMODB',
        f'extractor.db2_metadata.{Db2MetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.FORCE_CREATE_DIR}': True,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': neo4jConfig.uri,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': neo4jConfig.username,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': neo4jConfig.password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': importScheduling.dbName,
        f'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': 'unique_tag'
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job
