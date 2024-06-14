# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.

"""

import logging
import textwrap
from typing import Any

from pyhocon import ConfigFactory

from databuilder.publisher.configs.publisher_conf_factory import get_conf
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.task.task import DefaultTask
from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.publisher_factory import get_instance_by_db_type
from commons.gdb.domain.GdbVersion import GdbVersion

LOGGER = logging.getLogger(__name__)

def run_postgres_job(dbConfig, connectionString: str, sourceDbName: str, schemaName: str, targetDbName: str, driver: Any):
    where_clause_suffix = textwrap.dedent(f"""
        schemaname = '{schemaName}'
    """)

    tmp_folder = f'/var/tmp/postgres_{dbConfig.connection_name}_{sourceDbName}_{targetDbName}/amundsen/table_metadata'
    node_files_folder = f'{tmp_folder}/nodes/'
    relationship_files_folder = f'{tmp_folder}/relationships/'

    conf_dict = ConfigFactory.from_dict({
        f'extractor.postgres_metadata.{PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY}': where_clause_suffix,
        f'extractor.postgres_metadata.{PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME}': True,
        f'extractor.postgres_metadata.extractor.sqlalchemy.{SQLAlchemyExtractor.CONN_STRING}': connectionString,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR}': True
    })
    gdb_type: GdbVersion = GdbVersion.get_version(dbConfig.type)
    publisher_conf: dict = get_conf(gdb_type, dbConfig, targetDbName, node_files_folder, relationship_files_folder, sourceDbName)
    conf_dict = {**conf_dict, **publisher_conf}
    job_config = ConfigFactory.from_dict(conf_dict)
    publisher: Publisher = get_instance_by_db_type(dbtype=gdb_type, driver=driver)
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=PostgresMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=publisher)

    try:
        job.launch()
    except Exception as exceptionInstance:
        raise exceptionInstance
