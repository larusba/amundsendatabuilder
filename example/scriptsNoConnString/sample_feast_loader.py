# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script for extracting Feast feature tables

Usage:
    python3 sample_feast_loader.py [feast_core] [neo4j_endpoint] [es_url]

For example:
    python sample_feast_loader.py feast-feast-core:6565 bolt://neo4j http://elasticsearch:9200
"""

import sys
import uuid

from elasticsearch.client import Elasticsearch
from pyhocon import ConfigFactory

from databuilder.extractor.feast_extractor import FeastExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.task import DefaultTask

feast_repository_path = sys.argv[1]
neo4j_endpoint = sys.argv[2]

neo4j_user = "neo4j"
neo4j_password = "test"


def create_feast_job_config():
    tmp_folder = "/var/tmp/amundsen/table_metadata"
    node_files_folder = f"{tmp_folder}/nodes/"
    relationship_files_folder = f"{tmp_folder}/relationships/"

    job_config = ConfigFactory.from_dict(
        {
            f"extractor.feast.{FeastExtractor.FEAST_REPOSITORY_PATH}": feast_repository_path,
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}": node_files_folder,
            f"loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}": relationship_files_folder,
            f"publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}": node_files_folder,
            f"publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}": relationship_files_folder,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}": neo4j_endpoint,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}": neo4j_user,
            f"publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}": neo4j_password,
            "publisher.neo4j.job_publish_tag": "some_unique_tag",  # TO-DO unique tag must be added
        }
    )
    return job_config

if __name__ == "__main__":
    feast_job = DefaultJob(
        conf=create_feast_job_config(),
        task=DefaultTask(extractor=FeastExtractor(), loader=FsNeo4jCSVLoader()),
        publisher=neo4j_csv_publisher.Neo4jCsvPublisher(),
    )
    feast_job.launch()
