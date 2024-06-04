
from typing import Any

from commons.gdb.domain.GdbVendor import GdbVendor
from commons.gdb.domain.GdbVersion import GdbVersion

from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.tinkerpop_csv_publisher import TinkerpopCsvPublisher
from databuilder.publisher.cypher_csv_publisher import CypherCsvPublisher

def get_instance_by_db_type(dbtype: GdbVersion, driver: Any) -> Publisher: # to use from services as db_service(gdb_service.get_client(), gdb_service.get_type())
    if dbtype.vendor == GdbVendor.NEO4J:
        return Neo4jCsvPublisher(driver)
    elif dbtype.vendor.is_cypher_based():
        return CypherCsvPublisher(driver)
    elif dbtype.vendor.is_gremlin_based():
        return TinkerpopCsvPublisher(driver)
    else:
        raise Exception(str("graphdb.connection.type.unrecognized"))
