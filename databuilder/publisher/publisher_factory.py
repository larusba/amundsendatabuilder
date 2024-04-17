
from typing import Any
from commons.utils.common_models import GdbVersion

from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.tinkerpop_csv_publisher import TinkerpopCsvPublisher
from databuilder.publisher.cypher_csv_publisher import CypherCsvPublisher

def get_instance_by_db_type(dbtype: GdbVersion, driver: Any) -> Publisher: # to use from services as db_service(gdb_service.get_client(), gdb_service.get_type())
    if dbtype == GdbVersion.NEO4J_4_4_X.value or dbtype == GdbVersion.NEO4J_5_X.value:
        return Neo4jCsvPublisher(driver)
    elif dbtype == GdbVersion.MEMGRAPH_2_9_0.value:
        return CypherCsvPublisher(driver)
    elif dbtype == GdbVersion.JANUSGRAPH_1_0_X.value:
        return TinkerpopCsvPublisher(driver)
    elif dbtype == GdbVersion.ARCADEDB_23_X_X.value:
        return TinkerpopCsvPublisher(driver)
    else:
        raise Exception(str("graphdb.connection.type.unrecognized"))