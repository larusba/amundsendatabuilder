# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script demonstrating how to load data into Neo4j and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_tableau_*_job: executes a job to execute a specific Tableau extractor
  and publish the resulting metadata to neo4j
- `create_es_publisher_sample_job`: creates a job that extracts data from neo4j
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/lyft/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sys
import uuid

from amundsen_common.models.index_map import DASHBOARD_ELASTICSEARCH_INDEX_MAPPING
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.dashboard.tableau.tableau_dashboard_extractor import TableauDashboardExtractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_last_modified_extractor import (
    TableauDashboardLastModifiedExtractor,
)
from databuilder.extractor.dashboard.tableau.tableau_dashboard_query_extractor import TableauDashboardQueryExtractor
from databuilder.extractor.dashboard.tableau.tableau_dashboard_table_extractor import TableauDashboardTableExtractor
from databuilder.extractor.dashboard.tableau.tableau_external_table_extractor import (
    TableauDashboardExternalTableExtractor,
)
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
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

Base = declarative_base()

NEO4J_ENDPOINT = f'bolt://{neo_host}:{neo_port}'

neo4j_endpoint = NEO4J_ENDPOINT

neo4j_user = 'neo4j'
neo4j_password = 'test'

LOGGER = logging.getLogger(__name__)

tableau_base_url = ""
tableau_api_base_url = ""
tableau_api_version = 0
tableau_site_name = ""
tableau_personal_access_token_name = ""
tableau_personal_access_token_secret = ""
tableau_excluded_projects = []
tableau_dashboard_cluster = ""
tableau_dashboard_database = ""
tableau_external_table_cluster = ""
tableau_external_table_schema = ""
tableau_external_table_types = []
tableau_verify_request = None

common_tableau_config = {
    'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
    'publisher.neo4j.neo4j_user': neo4j_user,
    'publisher.neo4j.neo4j_password': neo4j_password,
    'publisher.neo4j.neo4j_encrypted': False,
    'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
}

def run_tableau_metadata_job():
    task = DefaultTask(extractor=TableauDashboardExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_metadata'

    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_metadata.api_base_url': tableau_api_base_url,
        'extractor.tableau_dashboard_metadata.tableau_base_url': tableau_base_url,
        'extractor.tableau_dashboard_metadata.api_version': tableau_api_version,
        'extractor.tableau_dashboard_metadata.site_name': tableau_site_name,
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_metadata.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_metadata.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_metadata.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_metadata.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_metadata.transformer.timestamp_str_to_epoch.timestamp_format':
            "%Y-%m-%dT%H:%M:%SZ",
        'extractor.tableau_dashboard_metadata.verify_request': tableau_verify_request,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_last_modified_job():
    task = DefaultTask(extractor=TableauDashboardLastModifiedExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_user'

    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_last_modified.api_base_url': tableau_api_base_url,
        'extractor.tableau_dashboard_last_modified.api_version': tableau_api_version,
        'extractor.tableau_dashboard_last_modified.site_name': tableau_site_name,
        'extractor.tableau_dashboard_last_modified.tableau_personal_access_token_name':
            tableau_personal_access_token_name,
        'extractor.tableau_dashboard_last_modified.tableau_personal_access_token_secret':
            tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_last_modified.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_last_modified.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_last_modified.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_last_modified.transformer.timestamp_str_to_epoch.timestamp_format':
            "%Y-%m-%dT%H:%M:%SZ",
        'extractor.tableau_dashboard_last_modified.verify_request': tableau_verify_request,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_query_job():
    task = DefaultTask(extractor=TableauDashboardQueryExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_query'

    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_query.api_base_url': tableau_api_base_url,
        'extractor.tableau_dashboard_query.api_version': tableau_api_version,
        'extractor.tableau_dashboard_query.site_name': tableau_site_name,
        'extractor.tableau_dashboard_query.tableau_personal_access_token_name': tableau_personal_access_token_name,
        'extractor.tableau_dashboard_query.tableau_personal_access_token_secret': tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_query.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_query.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_query.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_query.transformer.timestamp_str_to_epoch.timestamp_format': "%Y-%m-%dT%H:%M:%SZ",
        'extractor.tableau_dashboard_query.verify_request': tableau_verify_request,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_table_job():
    task = DefaultTask(extractor=TableauDashboardTableExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_table'

    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_dashboard_table.api_base_url': tableau_api_base_url,
        'extractor.tableau_dashboard_table.api_version': tableau_api_version,
        'extractor.tableau_dashboard_table.site_name': tableau_site_name,
        'extractor.tableau_dashboard_table.tableau_personal_access_token_name': tableau_personal_access_token_name,
        'extractor.tableau_dashboard_table.tableau_personal_access_token_secret': tableau_personal_access_token_secret,
        'extractor.tableau_dashboard_table.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_dashboard_table.cluster': tableau_dashboard_cluster,
        'extractor.tableau_dashboard_table.database': tableau_dashboard_database,
        'extractor.tableau_dashboard_table.external_cluster_name': tableau_external_table_cluster,
        'extractor.tableau_dashboard_table.external_schema_name': tableau_external_table_schema,
        'extractor.tableau_dashboard_table.transformer.timestamp_str_to_epoch.timestamp_format': "%Y-%m-%dT%H:%M:%SZ",
        'extractor.tableau_dashboard_table.verify_request': tableau_verify_request,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


def run_tableau_external_table_job():
    task = DefaultTask(extractor=TableauDashboardExternalTableExtractor(), loader=FsNeo4jCSVLoader())

    tmp_folder = '/var/tmp/amundsen/tableau_dashboard_external_table'

    node_files_folder = f'{tmp_folder}/nodes'
    relationship_files_folder = f'{tmp_folder}/relationships'

    dict_config = common_tableau_config
    dict_config.update({
        'extractor.tableau_external_table.api_base_url': tableau_api_base_url,
        'extractor.tableau_external_table.api_version': tableau_api_version,
        'extractor.tableau_external_table.site_name': tableau_site_name,
        'extractor.tableau_external_table.tableau_personal_access_token_name': tableau_personal_access_token_name,
        'extractor.tableau_external_table.tableau_personal_access_token_secret': tableau_personal_access_token_secret,
        'extractor.tableau_external_table.excluded_projects': tableau_excluded_projects,
        'extractor.tableau_external_table.cluster': tableau_dashboard_cluster,
        'extractor.tableau_external_table.database': tableau_dashboard_database,
        'extractor.tableau_external_table.external_cluster_name': tableau_external_table_cluster,
        'extractor.tableau_external_table.external_schema_name': tableau_external_table_schema,
        'extractor.tableau_external_table.external_table_types': tableau_external_table_types,
        'extractor.tableau_external_table.verify_request': tableau_verify_request,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'task.progress_report_frequency': 100,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
    })
    job_config = ConfigFactory.from_dict(dict_config)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())

    job.launch()


if __name__ == "__main__":
    # Uncomment next line to get INFO level logging
    # logging.basicConfig(level=logging.INFO)

    run_tableau_metadata_job()
    run_tableau_external_table_job()
    run_tableau_table_job()
    run_tableau_query_job()
    run_tableau_last_modified_job()

    job_es_table = create_es_publisher_sample_job(
        elasticsearch_index_alias='table_search_index',
        elasticsearch_doc_type_key='table',
        entity_type='table',
        model_name='databuilder.models.table_elasticsearch_document.TableESDocument')
    job_es_table.launch()

    job_es_dashboard = create_es_publisher_sample_job(
        elasticsearch_index_alias='dashboard_search_index',
        elasticsearch_doc_type_key='dashboard',
        model_name='databuilder.models.dashboard_elasticsearch_document.DashboardESDocument',
        entity_type='dashboard',
        elasticsearch_mapping=DASHBOARD_ELASTICSEARCH_INDEX_MAPPING)
    job_es_dashboard.launch()
