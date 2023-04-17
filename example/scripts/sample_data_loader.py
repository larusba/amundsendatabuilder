# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script demonstrating how to load data into Neo4j and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_csv_job`: runs a job that extracts table data from a CSV, loads (writes)
  this into a different local directory as a csv, then publishes this data to
  neo4j.
- `run_table_column_job`: does the same thing as `run_csv_job`, but with a csv
  containing column data.
- `create_last_updated_job`: creates a job that gets the current time, dumps it
  into a predefined model schema, and publishes this to neo4j.
- `create_es_publisher_sample_job`: creates a job that extracts data from neo4j
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/amundsen-io/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sys
import uuid

from amundsen_common.models.index_map import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, USER_INDEX_MAP
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import (
    CsvColumnLineageExtractor, CsvExtractor, CsvTableBadgeExtractor, CsvTableColumnExtractor, CsvTableLineageExtractor,
)
from databuilder.extractor.es_last_updated_extractor import EsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.publisher import neo4j_csv_publisher
from databuilder.transformer.base_transformer import ChainedTransformer, NoopTransformer
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel
from databuilder.transformer.generic_transformer import (
    CALLBACK_FUNCTION, FIELD_NAME, GenericTransformer,
)

neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'neo4j')

neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)

Base = declarative_base()

NEO4J_ENDPOINT = f'bolt://{neo_host}:{neo_port}'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_database = 'zozzerieamundsen'

neo4j_user = ''
neo4j_password = ''

LOGGER = logging.getLogger(__name__)


def run_csv_job(file_loc, job_name, model):
    tmp_folder = f'/var/tmp/amundsen/{job_name}'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    csv_extractor = CsvExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=csv_extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())

    job_config = ConfigFactory.from_dict({
        'extractor.csv.file_location': file_loc,
        'extractor.csv.model_class': model,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': neo4j_database,
        'publisher.neo4j.job_publish_tag': 'importMetadata',  # should use unique tag here like {ds}
    })

    DefaultJob(conf=job_config,
               task=task,
               publisher=Neo4jCsvPublisher()).launch()

def create_last_updated_job():
    # loader saves data to these folders and publisher reads it from here
    tmp_folder = '/var/tmp/amundsen/last_updated_data'
    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    task = DefaultTask(extractor=EsLastUpdatedExtractor(),
                       loader=FsNeo4jCSVLoader())

    job_config = ConfigFactory.from_dict({
        'extractor.es_last_updated.model_class':
            'databuilder.models.es_last_updated.ESLastUpdated',

        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_DATABASE_NAME}': neo4j_database,
        'publisher.neo4j.job_publish_tag': 'unique_lastupdated_tag',  # should use unique tag here like {ds}
    })

    return DefaultJob(conf=job_config,
                      task=task,
                      publisher=Neo4jCsvPublisher())


def _str_to_list(str_val):
    return str_val.split(',')


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    logging.basicConfig(level=logging.INFO)
    LOGGER.info("SONO PARTITO!")
    run_csv_job('/home/example/sample_data/sample_table_column_stats.csv', 'test_table_column_stats',
                'databuilder.models.table_stats.TableColumnStats')
    run_csv_job('/home/example/sample_data/sample_table_programmatic_source.csv', 'test_programmatic_source',
                'databuilder.models.table_metadata.TableMetadata')
    run_csv_job('/home/example/sample_data/sample_watermark.csv', 'test_watermark_metadata',
                'databuilder.models.watermark.Watermark')
    run_csv_job('/home/example/sample_data/sample_table_owner.csv', 'test_table_owner_metadata',
                'databuilder.models.table_owner.TableOwner')
    run_csv_job('/home/example/sample_data/sample_column_usage.csv', 'test_usage_metadata',
                'databuilder.models.table_column_usage.ColumnReader')
    run_csv_job('/home/example/sample_data/sample_user.csv', 'test_user_metadata',
                'databuilder.models.user.User')
    run_csv_job('/home/example/sample_data/sample_application.csv', 'test_application_metadata',
                'databuilder.models.application.Application')
    run_csv_job('/home/example/sample_data/sample_table_report.csv', 'test_report_metadata',
                'databuilder.models.report.ResourceReport')
    run_csv_job('/home/example/sample_data/sample_source.csv', 'test_source_metadata',
                'databuilder.models.table_source.TableSource')
    run_csv_job('/home/example/sample_data/sample_tags.csv', 'test_tag_metadata',
                'databuilder.models.table_metadata.TagMetadata')
    run_csv_job('/home/example/sample_data/sample_table_last_updated.csv', 'test_table_last_updated_metadata',
                'databuilder.models.table_last_updated.TableLastUpdated')
    run_csv_job('/home/example/sample_data/sample_schema_description.csv', 'test_schema_description',
                'databuilder.models.schema.schema.SchemaModel')
    run_csv_job('/home/example/sample_data/sample_dashboard_base.csv', 'test_dashboard_base',
                'databuilder.models.dashboard.dashboard_metadata.DashboardMetadata')
    run_csv_job('/home/example/sample_data/sample_dashboard_usage.csv', 'test_dashboard_usage',
                'databuilder.models.dashboard.dashboard_usage.DashboardUsage')
    run_csv_job('/home/example/sample_data/sample_dashboard_owner.csv', 'test_dashboard_owner',
                'databuilder.models.dashboard.dashboard_owner.DashboardOwner')
    run_csv_job('/home/example/sample_data/sample_dashboard_query.csv', 'test_dashboard_query',
                'databuilder.models.dashboard.dashboard_query.DashboardQuery')
    run_csv_job('/home/example/sample_data/sample_dashboard_last_execution.csv', 'test_dashboard_last_execution',
                'databuilder.models.dashboard.dashboard_execution.DashboardExecution')
    run_csv_job('/home/example/sample_data/sample_dashboard_last_modified.csv', 'test_dashboard_last_modified',
                'databuilder.models.dashboard.dashboard_last_modified.DashboardLastModifiedTimestamp')

    create_last_updated_job().launch()
