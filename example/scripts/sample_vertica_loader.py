# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script which demo how to load data
into Neo4j and Elasticsearch without using an Airflow DAG.

"""

import logging
import os
import sys
import textwrap
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.vertica_metadata_extractor import VerticaMetadataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')

es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)

if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

DB_FILE = '/tmp/test.db'
SQLITE_CONN_STRING = 'sqlite:////tmp/test.db'
Base = declarative_base()

NEO4J_ENDPOINT = 'bolt://{}:{}'.format(neo_host, neo_port)

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

LOGGER = logging.getLogger(__name__)


# specify vertica access credentials, host server, port (default 5433),
# database name (default 'vertica')
def connection_string():
    user = 'username'
    password = 'password'
    host = 'vertica-budget.host'
    port = '5433'
    db = 'vertica'
    return "vertica+vertica_python://%s:%s@%s:%s/%s" % (user, password, host, port, db)


# provide schemas to run extraction on (default 'public')
def run_vertica_job(neo4jConfig, connectionString: str, sourceDbName: str, targetDbName: str):
    where_clause_suffix = textwrap.dedent(f"""
        where c.table_schema = '{sourceDbName}'
    """)

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    job_config = ConfigFactory.from_dict({
        'extractor.vertica_metadata.{}'.format(VerticaMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY):
            where_clause_suffix,
        'extractor.vertica_metadata.{}'.format(VerticaMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            False,
        'extractor.vertica_metadata.{}'.format(VerticaMetadataExtractor.CLUSTER_KEY):
            'vertica_budget',
        'extractor.vertica_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING):
            connectionString,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH):
            node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR):
            node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR):
            relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY):
            neo4jConfig.uri,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER):
            neo4jConfig.username,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD):
            neo4jConfig.password,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': targetDbName,
        f'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG):
            'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=VerticaMetadataExtractor(), loader=FsNeo4jCSVLoader()),
                     publisher=Neo4jCsvPublisher())
    
    try:
        job.launch()
    except Exception as exceptionInstance:
        raise Exception(str(exceptionInstance))
