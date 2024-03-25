
from commons.gdb.domain.GdbVersion import GdbVersion

from api.src.galileo.gdb.impl.ArcadeDbService import ArcadeDbService
from api.src.galileo.gdb.impl.JanusDbService import JanusDbService
from api.src.galileo.gdb.impl.MemgraphDbService import MemgraphDbService
from api.src.galileo.gdb.impl.Neo4j5DbService import Neo4j5DbService
from api.src.galileo.gdb.impl.Neo4jDbService import Neo4jDbService
from api.src.galileo.gdb.inter.GdbServiceInterface import GdbServiceInterface

from commons.gdb.service.SingletonClass import SingletonClass
from databuilder.publisher.base_publisher import Publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.publisher.tinkerpop_csv_publisher import TinkerpopCsvPublisher
from databuilder.publisher.arcade_csv_publisher import ArcadeCsvPublisher


class PublisherFactory(metaclass=SingletonClass):

    def __init__(self):
        self.stored_implementation: Publisher = None

    def __del__(self):
        if self.stored_implementation:
            del self.stored_implementation
            self.stored_implementation = None

    def db_service(self, driver, dbtype: GdbVersion) -> Publisher: # to use from services as db_service(gdb_service.get_client(), gdb_service.get_type())
        if self.stored_implementation is not None:
            return self.stored_implementation

        if dbtype is GdbVersion.NEO4J_4_4_X:
            self.stored_implementation = Neo4jCsvPublisher(driver)
        elif dbtype is GdbVersion.NEO4J_5_X:
            self.stored_implementation = Neo4jCsvPublisher(driver)
        elif dbtype is GdbVersion.MEMGRAPH_2_9_0:
            self.stored_implementation = MemgraphDbService(driver)
        elif dbtype is GdbVersion.JANUSGRAPH_1_0_X:
            self.stored_implementation = TinkerpopCsvPublisher(driver)
        elif dbtype is GdbVersion.ARCADEDB_23_X_X:
            self.stored_implementation = ArcadeCsvPublisher(driver)
        else:
            raise Exception(str("graphdb.connection.type.unrecognized"))

        return self.stored_implementation

    def reset_stored_implementation(self):
        self.stored_implementation = None