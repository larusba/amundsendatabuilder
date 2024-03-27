# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.

"""

import logging
import textwrap

from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.mysql_metadata_extractor import MysqlMetadataExtractor
from databuilder.publisher.configs.publisher_conf_factory import get_conf
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.task.task import DefaultTask
from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.publisher_factory import PublisherFactory

from mdm_importer.app.models import DbConfig

DB_FILE = '/tmp/test.db'
SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
Base = declarative_base()

NEO4J_ENDPOINT = f'bolt://neo4j:7687'

MONGO_CONNECTION = f'mongodb://admin:admin@mongo:27017/galileo?authSource=admin'

LOGGER = logging.getLogger(__name__)

def run_mysql_job(dbConfig: DbConfig, connectionString: str, sourceDbName: str, targetDbName: str):
    where_clause_suffix = textwrap.dedent(f"""
        where c.table_schema = '{sourceDbName}'
    """)

    # LOGGER.info(f"Neo4j Config: \nUri: {neo4jConfig.uri}\nUsername: {neo4jConfig.username}\nPassword: {neo4jConfig.password}")
    # LOGGER.info(f"Import Scheduling: \nConnection String: {importScheduling.connection_string}\nDb Name: {importScheduling.db_name}")
    # print(f"Neo4j Config: \nUri: {neo4jConfig.uri}\nUsername: {neo4jConfig.username}\nPassword: {neo4jConfig.password}")
    # print(f"Import Scheduling: \nConnection String: {importScheduling.connection_string}\nDb Name: {importScheduling.db_name}")
    tmp_folder = f'/var/tmp/mysql_{dbConfig.connection_name}_{sourceDbName}_{targetDbName}/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'
    
    conf_dict: dict = {
        f'extractor.mysql_metadata.{MysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.mysql_metadata.{MysqlMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME}': True,
        f'extractor.mysql_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connectionString,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True
    }
    publisher_conf: dict = get_conf(dbConfig.type, dbConfig, targetDbName, node_files_folder, relationship_files_folder, sourceDbName)
    conf_dict = conf_dict.update(publisher_conf)
    job_config = ConfigFactory.from_dict(conf_dict)

    publisher: Publisher = PublisherFactory.get_instance_by_db_type(dbConfig.type)
    print("PUBLISHER: " + publisher)
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=MysqlMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=publisher)

    try:
        job.launch()
    except Exception as exceptionInstance:
        raise exceptionInstance
