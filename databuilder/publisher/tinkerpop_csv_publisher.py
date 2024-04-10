# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import typing
import pandas

from typing import List, Set, Dict
from pyhocon import ConfigTree, ConfigFactory
from gremlin_python.driver.client import Client
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from commons.gdb.domain.TinkerpopClient import TinkerpopClient
from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.publisher_config_constants import (TinkerpopCsvPublisherConfigs, PublishBehaviorConfigs, PublisherConfigs)
from io import open
from os import listdir
from os.path import isfile, join
from jinja2 import Template
from pyhocon import ConfigFactory, ConfigTree

# Config keys
# A directory that contains CSV files for nodes
NODE_FILES_DIR = PublisherConfigs.NODE_FILES_DIR
# A directory that contains CSV files for relationships
RELATION_FILES_DIR = PublisherConfigs.RELATION_FILES_DIR
# A end point for the Gremlin DB
GREMLIN_URI = TinkerpopCsvPublisherConfigs.GREMLIN_URI

GREMLIN_USER = TinkerpopCsvPublisherConfigs.GREMLIN_DB_USER
GREMLIN_PASSWORD = TinkerpopCsvPublisherConfigs.GREMLIN_DB_PASSWORD
GREMLIN_DATABASE_NAME = TinkerpopCsvPublisherConfigs.GREMLIN_DATABASE_NAME
TINKERPOP_GRAPHS = TinkerpopCsvPublisherConfigs.TINKERPOP_GRAPHS

# A transaction size that determines how often it commits.
GREMLIN_TRANSACTION_SIZE = TinkerpopCsvPublisherConfigs.GREMLIN_TRANSACTION_SIZE
# A progress report frequency that determines how often it report the progress.
GREMLIN_PROGRESS_REPORT_FREQUENCY = 'neo4j_progress_report_frequency'

GREMLIN_MAX_CONN_LIFE_TIME_SEC = TinkerpopCsvPublisherConfigs.GREMLIN_MAX_CONN_LIFE_TIME_SEC

# This will be used to provide unique tag to the node and relationship
JOB_PUBLISH_TAG = PublisherConfigs.JOB_PUBLISH_TAG

# any additional fields that should be added to nodes and rels through config
ADDITIONAL_FIELDS = PublisherConfigs.ADDITIONAL_PUBLISHER_METADATA_FIELDS

# Neo4j property name for published tag
PUBLISHED_TAG_PROPERTY_NAME = PublisherConfigs.PUBLISHED_TAG_PROPERTY_NAME

# Property name for last updated timestamp
LAST_UPDATED_EPOCH_MS = PublisherConfigs.LAST_UPDATED_EPOCH_MS

# A boolean flag to indicate if publisher_metadata (e.g. published_tag,
# publisher_last_updated_epoch_ms)
# will be included as properties of the Gremlin nodes
ADD_PUBLISHER_METADATA = PublishBehaviorConfigs.ADD_PUBLISHER_METADATA

RELATION_PREPROCESSOR = 'relation_preprocessor'

# CSV HEADER
# A header with this suffix will be pass to Arcade statement without quote
UNQUOTED_SUFFIX = ':UNQUOTED'
# A header for Node label
NODE_LABEL_KEY = 'LABEL'
# A header for Node key
NODE_KEY_KEY = 'KEY'
# Required columns for Node
NODE_REQUIRED_KEYS = {NODE_LABEL_KEY}

# Relationship relates two nodes together
# Start node label
RELATION_START_LABEL = 'START_LABEL'
# Start node key
RELATION_START_KEY = 'START_KEY'
# End node label
RELATION_END_LABEL = 'END_LABEL'
# Node node key
RELATION_END_KEY = 'END_KEY'
# Type for relationship (Start Node)->(End Node)
RELATION_TYPE = 'TYPE'
# Type for reverse relationship (End Node)->(Start Node)
RELATION_REVERSE_TYPE = 'REVERSE_TYPE'
# Required columns for Relationship
RELATION_REQUIRED_KEYS = {RELATION_START_LABEL, RELATION_START_KEY,
                          RELATION_END_LABEL, RELATION_END_KEY,
                          RELATION_TYPE, RELATION_REVERSE_TYPE}

DEFAULT_CONFIG = ConfigFactory.from_dict({GREMLIN_TRANSACTION_SIZE: 500,
                                          GREMLIN_PROGRESS_REPORT_FREQUENCY: 500,
                                          GREMLIN_MAX_CONN_LIFE_TIME_SEC: 50,
                                          ADDITIONAL_FIELDS: {},
                                          ADD_PUBLISHER_METADATA: True})

# transient error retries and sleep time
RETRIES_NUMBER = 5
SLEEP_TIME = 2

LOGGER = logging.getLogger(__name__)

class TinkerpopCsvPublisher(Publisher):
    """
    This Publisher takes two folders for input and publishes to Tinkerpop DBs.
    One folder will contain CSV file(s) for Node where the other folder will contain CSV
    file(s) for Relationship.
    """

    def __init__(self) -> None:
        super(TinkerpopCsvPublisher, self).__init__()

    def init(self, conf: ConfigTree) -> None:
        conf = conf.with_fallback(DEFAULT_CONFIG)

        self._count: int = 0
        self._progress_report_frequency = conf.get_int(GREMLIN_PROGRESS_REPORT_FREQUENCY)
        self._node_files = self._list_files(conf, NODE_FILES_DIR)
        self._node_files_iter = iter(self._node_files)

        self._relation_files = self._list_files(conf, RELATION_FILES_DIR)
        self._relation_files_iter = iter(self._relation_files)

        uri = conf.get_string(GREMLIN_URI)

        self._driver: TinkerpopClient = self.create_tinkerpop_driver(uri, conf)
        self._db_name = conf.get_string(GREMLIN_DATABASE_NAME)

        conn_tinkerpop_graphs: typing.Dict[str, str] = conf.get(TINKERPOP_GRAPHS, {})
        self._current_traversal_name: str = conn_tinkerpop_graphs.get(self._db_name)
        self._gremlin_client: Client = self._driver.clients.get(self._current_traversal_name)

        self._transaction_size = conf.get_int(GREMLIN_TRANSACTION_SIZE)

        self.labels: Set[str] = set()
        self.publish_tag: str = conf.get_string(JOB_PUBLISH_TAG)
        self.additional_fields: Dict = conf.get(ADDITIONAL_FIELDS)
        self.add_publisher_metadata: bool = conf.get_bool(ADD_PUBLISHER_METADATA)
        if self.add_publisher_metadata and not self.publish_tag:
            raise Exception(f'{JOB_PUBLISH_TAG} should not be empty')

        self._relation_preprocessor = conf.get(RELATION_PREPROCESSOR)

        LOGGER.info('Publishing Node csv files %s, and Relation CSV files %s',
                    self._node_files,
                    self._relation_files)

    def create_tinkerpop_driver(connection_string: str, conf: ConfigTree) -> TinkerpopClient:
        gremlin_user: str = conf.get_string(GREMLIN_USER)
        gremlin_password: str = conf.get_string(GREMLIN_PASSWORD)
        conn_tinkerpop_graphs: typing.Dict[str, str] = conf.get(TINKERPOP_GRAPHS, {})
        clients: typing.Dict[str, Client] = {}
        tinkerpop_graphs: typing.Dict[str, GraphTraversalSource] = {}
        for key in conn_tinkerpop_graphs:
            clients[key] = Client(
                url=connection_string, traversal_source=conn_tinkerpop_graphs[key], username=gremlin_user or "", password=gremlin_password or ""
            )
            tinkerpop_graphs[key] = traversal().withRemote(
                DriverRemoteConnection(
                    url=connection_string, traversal_source=conn_tinkerpop_graphs[key],
                    username=gremlin_user or "", password=gremlin_password or ""
                )
            )
        return TinkerpopClient(clients, tinkerpop_graphs)
    
    def g(self) -> GraphTraversalSource:
        return self._driver.tinkerpop_graphs.get((self._db_name))

    def _list_files(self, conf: ConfigTree, path_key: str) -> List[str]:
        """
        List files from directory
        :param conf:
        :param path_key:
        :return: List of file paths
        """
        if path_key not in conf:
            return []

        path = conf.get_string(path_key)
        return [join(path, f) for f in listdir(path) if isfile(join(path, f))]

    def publish_impl(self) -> None:  # noqa: C901
        """
        Publishes Nodes first and then Relations
        :return:
        """

        start = time.time()

        LOGGER.info('Publishing Node files: %s', self._node_files)
        try:
            while True:
                try:
                    node_file = next(self._node_files_iter)
                    self._publish_node(node_file)
                except StopIteration:
                    break

            LOGGER.info('Publishing Relationship files: %s', self._relation_files)
            while True:
                try:
                    relation_file = next(self._relation_files_iter)
                    self._publish_relation(relation_file)
                except StopIteration:
                    break

            LOGGER.info('Committed total %i statements', self._count)

            # TODO: Add statsd support
            LOGGER.info('Successfully published. Elapsed: %i seconds', time.time() - start)
        except Exception as e:
            LOGGER.exception('Failed to publish. Rolling back.')
            raise e

    def get_scope(self) -> str:
        return 'publisher.arcade'

    def _publish_node(self, node_file: str) -> None:
        """
        Iterate over the csv records of a file, each csv record transform to a gremlin statement to create nodes
        and will be executed.
        """

        with open(node_file, 'r', encoding='utf8') as node_csv:
            for node_record in pandas.read_csv(node_csv,
                                               na_filter=False).to_dict(orient="records"):
                stmt = self.create_node_merge_statement(node_record=node_record)
                self._execute_statement(stmt)

    def create_node_merge_statement(self, node_record: dict) -> str:
        """
        Creates node creation statement
        :param node_record:
        """
        template = Template("""
            {{ TRAVERSAL }}.V().hasLabel('{{ LABEL }}').{{ MATCH_KEY }}.fold().
                            coalesce(unfold().{{ PROP_BODY }},
                            addV('{{ LABEL }}').{{ PROP_BODY }}).iterate()
        """)

        match_key_body = self._match_key_body(node_record, NODE_KEY_KEY)
        prop_body = self._create_props_body(node_record, NODE_REQUIRED_KEYS)

        return template.render(TRAVERSAL=self._current_traversal_name, 
                               LABEL=node_record[NODE_LABEL_KEY],
                               MATCH_KEY=match_key_body,
                               PROP_BODY=prop_body,
                               update=(True))

    def _publish_relation(self, relation_file: str) -> None:
        """
        Creates relation between two nodes.
        :param relation_file:
        :return:
        """

        with open(relation_file, 'r', encoding='utf8') as relation_csv:
            for rel_record in pandas.read_csv(relation_csv, na_filter=False).to_dict(orient="records"):
                exception_may_exist = True
                retries_for_exception = RETRIES_NUMBER
                while exception_may_exist and retries_for_exception > 0:
                    try:
                        stmt = self.create_relationship_merge_statement(rel_record=rel_record)
                        self._execute_statement(stmt)
                        exception_may_exist = False
                    except Exception as e:
                        if retries_for_exception > 0:
                            retries_for_exception = retries_for_exception-1
                        else:
                            raise e

    def create_relationship_merge_statement(self, rel_record: dict) -> str:
        """
        Creates relationship merge statement
        :param rel_record:
        :return:
        """
        template = Template("""
            {{ TRAVERSAL }}.V().hasLabel('{{ START_LABEL }}').has('key', '{{ START_KEY }}').as('source')
                           .V().hasLabel('{{ END_LABEL }}').has('key', {{ END_KEY }}).as('target')
                           .coalesce(in({{ TYPE }}).where(eq('source')), addE({{ TYPE }}).from(select('source')))
                           .iterate()
        """)

        return template.render(TRAVERSAL=self._current_traversal_name,
                               START_LABEL=rel_record[RELATION_START_LABEL],
                               START_KEY=rel_record[RELATION_START_KEY],
                               END_LABEL=rel_record[RELATION_END_LABEL],
                               END_KEY=rel_record[RELATION_END_KEY],
                               TYPE=rel_record[RELATION_TYPE])

    def _create_props_param(self, record_dict: dict) -> dict:
        params = {}
        for k, v in record_dict.items():
            if k.endswith(UNQUOTED_SUFFIX):
                k = k[:-len(UNQUOTED_SUFFIX)]

            params[k] = v
        return params

    def _match_key_body(self, record_dict: dict, key: str) -> str:
        try:
            val = record_dict.get(key)
            return f"has('{key}', {val})"
        except Exception as e:
            raise e
        
    def _create_props_body(self,
                           record_dict: dict,
                           excludes: Set) -> str:
        """
        Creates properties body with params required for resolving template.

        e.g: for each record in record_dict will append to the gremlin statement
        something like => .property('prop_key', prop_value)

        :param record_dict: A dict represents CSV row
        :param excludes: set of excluded columns that does not need to be in properties
        (e.g: KEY, LABEL ...)
        :param identifier: identifier that will be used in Gremlin query as shown on above example
        :return: Properties body for Gremlin statement
        """
        props = []
        for k, v in record_dict.items():
            if k in excludes:
                continue

            if k.endswith(UNQUOTED_SUFFIX):
                k = k[:-len(UNQUOTED_SUFFIX)]

            val = v if isinstance(v, int) or isinstance(v, float) else f"'{v}'"
            props.append(f"property('{k.lower()}', {val})")

        if self.add_publisher_metadata:
            props.append(f"property('{PUBLISHED_TAG_PROPERTY_NAME}', '{self.publish_tag}')")
            props.append(f"property('{LAST_UPDATED_EPOCH_MS}', '{time.time()}'")

        # add additional metatada fields from config
        for k, v in self.additional_fields.items():
            val = v if isinstance(v, int) or isinstance(v, float) else f"'{v}'"
            props.append(f"property('{k.lower()}', {val})")

        return '.'.join(props)

    def _execute_statement(self,
                           stmt: str) -> None:
        """
        Executes statement against ArcadeDB.
        :param stmt:
        :param tx:
        :param count:
        :return:
        """
        try:
            LOGGER.debug('Executing statement: %s', stmt)
            self._gremlin_client.submit(str(stmt))

            self._count += 1
            if self._count > 1 and self._count % self._transaction_size == 0:
                LOGGER.info(f'Committed {self._count} statements so far')

            if self._count > 1 and self._count % self._progress_report_frequency == 0:
                LOGGER.info(f'Processed {self._count} statements so far')

        except Exception as e:
            LOGGER.exception('Failed to execute Gremlin query')
            raise e
