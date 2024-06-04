from commons.gdb.domain.GdbVendor import GdbVendor

from databuilder.publisher.publisher_config_constants import (
    Neo4jCsvPublisherConfigs, PublisherConfigs, CypherCsvPublisherConfigs, TinkerpopCsvPublisherConfigs
)
from commons.gdb.domain.GdbVersion import GdbVersion
import time

def get_publisher_tag(dbType: GdbVersion) -> str:
    if dbType.vendor == GdbVendor.NEO4J:
        return "publisher.neo4j"
    elif dbType.vendor.is_cypher_based():
        return "publisher.cypher"
    elif dbType.vendor == GdbVendor.ARCADEDB:
        return "publisher.arcade"
    else:
        raise Exception("GDB Type not supported")

def get_additional_props_by_db_type(dbType: GdbVersion, dbConfig, targetDbName: str) -> dict:
    if dbType.vendor == GdbVendor.NEO4J:
        return {
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_END_POINT_KEY}': dbConfig.uri,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_USER}': dbConfig.username,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_PASSWORD}': dbConfig.password,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_DATABASE_NAME}': targetDbName,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_ENCRYPTED}': False
        }
    elif dbType.vendor.is_cypher_based():
        return {
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_URI}': dbConfig.uri,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DB_USER}': dbConfig.username,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DB_PASSWORD}': dbConfig.password,
            f'publisher.cypher.{CypherCsvPublisherConfigs.CYPHER_DATABASE_NAME}': targetDbName
        }
    elif dbType.vendor == GdbVendor.ARCADEDB:
        return {
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_URI}': dbConfig.uri,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DB_USER}': dbConfig.username,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DB_PASSWORD}': dbConfig.password,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.GREMLIN_DATABASE_NAME}': targetDbName,
            f'publisher.arcade.{TinkerpopCsvPublisherConfigs.TINKERPOP_GRAPHS}': dbConfig.tinkerpopGraphs
        }
    else:
        return {}

def get_conf(dbType: GdbVersion, dbConfig, targetDbName: str, node_files_folder: str, relationship_files_folder: str, sourceDbName: str):
    generic_conf = {
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.NODE_FILES_DIR}': node_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.RELATION_FILES_DIR}': relationship_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.JOB_PUBLISH_TAG}': f'{sourceDbName}_{targetDbName}_{int(time.time() * 1000)}',  # should use unique tag here like {ds}
    }
    specific_conf: dict = get_additional_props_by_db_type(dbType, dbConfig, targetDbName)
    conf: dict = {**generic_conf, **specific_conf}
    return conf
