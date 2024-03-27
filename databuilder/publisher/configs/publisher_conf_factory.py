from commons.utils.common_models import GdbVersion
from databuilder.publisher.publisher_config_constants import (
    Neo4jCsvPublisherConfigs, PublisherConfigs
)
from datetime import datetime, timezone, timedelta

from mdm_importer.app.models import DbConfig

def get_publisher_tag(dbType: GdbVersion) -> str:
    if dbType is GdbVersion.NEO4J_4_4_X | GdbVersion.NEO4J_5_X:
        return "publisher.neo4j"
    elif dbType is GdbVersion.MEMGRAPH_2_9_0:
        return ""
    elif dbType is GdbVersion.JANUSGRAPH_1_0_X:
        return ""
    elif dbType is GdbVersion.ARCADEDB_23_X_X:
        return ""

def get_additional_props_by_db_type(dbType: GdbVersion, dbConfig: DbConfig, targetDbName: str) -> dict:
    if dbType is GdbVersion.NEO4J_4_4_X | GdbVersion.NEO4J_5_X:
        return {
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_END_POINT_KEY}': dbConfig.uri,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_USER}': dbConfig.username,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_PASSWORD}': dbConfig.password,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_DATABASE_NAME}': targetDbName,
            f'publisher.neo4j.{Neo4jCsvPublisherConfigs.NEO4J_ENCRYPTED}': False
        }
    elif dbType is GdbVersion.MEMGRAPH_2_9_0:
        return {}
    elif dbType is GdbVersion.JANUSGRAPH_1_0_X:
        return {}
    elif dbType is GdbVersion.ARCADEDB_23_X_X:
        return {}

def get_conf(dbType: GdbVersion, dbConfig: DbConfig, targetDbName: str, node_files_folder: str, relationship_files_folder: str, sourceDbName: str):
    generic_conf = {f'publisher.neo4j.{PublisherConfigs.NODE_FILES_DIR}': node_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.RELATION_FILES_DIR}': relationship_files_folder,
        f'{get_publisher_tag(dbType)}.{PublisherConfigs.JOB_PUBLISH_TAG}': f'{sourceDbName}_{format(datetime.now(timezone(timedelta(hours=+1), "UTC")))}',  # should use unique tag here like {ds}
    }
    specific_conf: dict = get_additional_props_by_db_type(dbType, dbConfig, targetDbName)
    conf = dict(generic_conf.update(specific_conf))
    return conf