from commons.utils.common_models import GdbVersion
from databuilder.publisher.publisher_config_constants import (
    Neo4jCsvPublisherConfigs, PublisherConfigs, CypherCsvPublisherConfigs, TinkerpopCsvPublisherConfigs
)

import time

def get_publisher_tag(dbType: GdbVersion) -> str:
    if dbType == GdbVersion.NEO4J_4_4_X.value or dbType == GdbVersion.NEO4J_5_X.value:
        return "publisher.neo4j"
    elif dbType == GdbVersion.MEMGRAPH_2_9_0.value:
        return "publisher.cypher"
    elif dbType == GdbVersion.JANUSGRAPH_1_0_X.value:
        raise Exception("GDB Type not supported")
    elif dbType == GdbVersion.ARCADEDB_23_X_X.value:
        return "publisher.arcade"

def get_additional_props_by_db_type(dbType: GdbVersion, dbConfig, targetDbName: str) -> dict:
    if dbType == GdbVersion.NEO4J_4_4_X.value or dbType == GdbVersion.NEO4J_5_X.value:
        return {
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_END_POINT_KEY}': dbConfig.uri,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_USER}': dbConfig.username,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_PASSWORD}': dbConfig.password,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_DATABASE_NAME}': targetDbName,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_ENCRYPTED}': False
        }
    elif dbType == GdbVersion.MEMGRAPH_2_9_0.value:
        return {
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_URI}': dbConfig.uri,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DB_USER}': dbConfig.username,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DB_PASSWORD}': dbConfig.password,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DATABASE_NAME}': targetDbName
        }
    elif dbType == GdbVersion.JANUSGRAPH_1_0_X.value:
        return {}
    elif dbType == GdbVersion.ARCADEDB_23_X_X.value:
        return {
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_URI}': dbConfig.uri,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DB_USER}': dbConfig.username,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DB_PASSWORD}': dbConfig.password,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DATABASE_NAME}': targetDbName,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.TINKERPOP_GRAPHS}': dbConfig.tinkerpopGraphs
        }

def get_conf(dbType: GdbVersion, dbConfig, targetDbName: str, node_files_folder: str, relationship_files_folder: str, sourceDbName: str):
    generic_conf = {
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.NODE_FILES_DIR}': node_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.RELATION_FILES_DIR}': relationship_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.JOB_PUBLISH_TAG}': f'{sourceDbName}_{targetDbName}_{int(time.time() * 1000)}',  # should use unique tag here like {ds}
    }
    specific_conf: dict = get_additional_props_by_db_type(dbType, dbConfig, targetDbName)
    conf: dict = {**generic_conf, **specific_conf}
    return conf