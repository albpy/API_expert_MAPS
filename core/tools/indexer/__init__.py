# from utils.indexer.middlewares import CachingMiddleware
from .database import Indexer
from .queries import Query, where
from .storages import RedisStorage


class GlobalIndexer:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GlobalIndexer, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self.indexer = Indexer(storage=RedisStorage)
            self.__item_master_index = self.indexer.index("item_master")
    
    @property
    def ItemMaster_Index(self):
        return self.__item_master_index

_ = GlobalIndexer()

__all__ = ('GlobalIndexer', 'Indexer', 'Query', 'where')