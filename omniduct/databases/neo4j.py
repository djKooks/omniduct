from __future__ import absolute_import

from omniduct.utils.debug import logger

from .base import DatabaseClient
from . import cursor_formatters


class Neo4jClient(DatabaseClient):

    PROTOCOLS = ['neo4j']
    DEFAULT_PORT = 7687

    def _init(self):
        self.__driver = None

    # Connection
    def _connect(self):
        from neo4j.v1 import GraphDatabase
        logger.info('Connecting to Neo4J graph database ...')
        self.__driver = GraphDatabase.driver("bolt://{}:{}".format(self.host, self.port))

    def _is_connected(self):
        try:
            return self.__driver is not None
        except:
            return False

    def _disconnect(self):
        logger.info('Disconnecting from Neo4J graph database ...')
        try:
            self.__driver.close()
        except:
            pass
        self.__driver = None

    # Querying
    def _execute(self, statement, cursor=None, async=False):
        session = self.__driver.session()
        result = session.run(statement)
        # TODO this is a hack since result is not a cursor
        result.close = result.detach
        result.fetchall = result.records
        return result

    def _get_formatter(self, format, cursor, **format_opts):
        return cursor_formatters.RawCursorFormatter(cursor)

    def _table_exists(self, table, schema=None):
        raise Exception('tables do not apply to graph databases')

    def _table_desc(self, table, **kwargs):
        raise Exception('tables do not apply to graph databases')

    def _table_head(self, table, n=10, **kwargs):
        raise Exception('tables do not apply to graph databases')

    def _table_list(self, table, schema=None):
        raise Exception('tables do not apply to graph databases')

    def _table_props(self, table, **kwargs):
        raise Exception('tables do not apply to graph databases')
