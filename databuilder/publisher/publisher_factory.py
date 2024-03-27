
from commons.utils.common_models import GdbVersion

from databuilder.publisher.metaclass.singleton import Singleton
from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.tinkerpop_csv_publisher import TinkerpopCsvPublisher
from databuilder.publisher.arcade_csv_publisher import ArcadeCsvPublisher
from databuilder.publisher.cypher_csv_publisher import CypherCsvPublisher


class PublisherFactory(metaclass=Singleton):

    def __init__(self):
        self.stored_implementation: Publisher = None

    def __del__(self):
        if self.stored_implementation:
            del self.stored_implementation
            self.stored_implementation = None

    def get_instance_by_db_type(self, dbtype: GdbVersion) -> Publisher: # to use from services as db_service(gdb_service.get_client(), gdb_service.get_type())
        if self.stored_implementation is not None:
            return self.stored_implementation

        if dbtype is GdbVersion.NEO4J_4_4_X:
            self.stored_implementation = Neo4jCsvPublisher()
        elif dbtype is GdbVersion.NEO4J_5_X:
            self.stored_implementation = Neo4jCsvPublisher()
        elif dbtype is GdbVersion.MEMGRAPH_2_9_0:
            self.stored_implementation = CypherCsvPublisher()
        elif dbtype is GdbVersion.JANUSGRAPH_1_0_X:
            self.stored_implementation = TinkerpopCsvPublisher()
        elif dbtype is GdbVersion.ARCADEDB_23_X_X:
            self.stored_implementation = ArcadeCsvPublisher()
        else:
            raise Exception(str("graphdb.connection.type.unrecognized"))

        return self.stored_implementation

    def reset_stored_implementation(self):
        self.stored_implementation = None