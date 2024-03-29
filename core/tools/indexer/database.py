"""
This module contains the main component of Indexer: the database.
"""
from typing import Dict, Iterator, Set, Type

from .index import Document, Index
from .storages import JSONStorage, Storage
from .utils import with_typehint

# The index's base class. This is used to add type hinting from the Index
# class to Indexer. Currently, this supports PyCharm, Pyright/VS Code and MyPy.
IndexBase: Type[Index] = with_typehint(Index)


class Indexer(IndexBase):
    """
    The main class of Indexer.

    The ``Indexer`` class is responsible for creating the storage class instance
    that will store this database's documents, managing the database
    indexs as well as providing access to the default index.

    For index management, a simple ``dict`` is used that stores the index class
    instances accessible using their index name.

    Default index access is provided by forwarding all unknown method calls
    and property access operations to the default index by implementing
    ``__getattr__``.

    When creating a new instance, all arguments and keyword arguments (except
    for ``storage``) will be passed to the storage class that is provided. If
    no storage class is specified, :class:`~indexer.storages.JSONStorage` will be
    used.

    .. admonition:: Customization

        For customization, the following class variables can be set:

        - ``index_class`` defines the class that is used to create indexs,
        - ``default_index_name`` defines the name of the default index, and
        - ``default_storage_class`` will define the class that will be used to
          create storage instances if no other storage is passed.

        .. versionadded:: 4.0

    .. admonition:: Data Storage Model

        Data is stored using a storage class that provides persistence for a
        ``dict`` instance. This ``dict`` contains all indexs and their data.
        The data is modelled like this::

            {
                'index1': {
                    0: {document...},
                    1: {document...},
                },
                'index2': {
                    ...
                }
            }

        Each entry in this ``dict`` uses the index name as its key and a
        ``dict`` of documents as its value. The document ``dict`` contains
        document IDs as keys and the documents themselves as values.

    :param storage: The class of the storage to use. Will be initialized
                    with ``args`` and ``kwargs``.
    """

    #: The class that will be used to create index instances
    #:
    #: .. versionadded:: 4.0
    index_class = Index

    #: The name of the default index
    #:
    #: .. versionadded:: 4.0
    default_index_name = '_default'

    #: The class that will be used by default to create storage instances
    #:
    #: .. versionadded:: 4.0
    default_storage_class = JSONStorage

    def __init__(self, *args, **kwargs) -> None:
        """
        Create a new instance of Indexer.
        """

        storage = kwargs.pop('storage', self.default_storage_class)

        # Prepare the storage
        self._storage: Storage = storage(*args, **kwargs)

        self._opened = True
        self._indexs: Dict[str, Index] = {}

    def __repr__(self):
        args = [
            'indexs={}'.format(list(self.indexs())),
            'indexs_count={}'.format(len(self.indexs())),
            'default_index_documents_count={}'.format(self.__len__()),
            'all_indexs_documents_count={}'.format(
                ['{}={}'.format(index, len(self.index(index)))
                 for index in self.indexs()]),
        ]

        return '<{} {}>'.format(type(self).__name__, ', '.join(args))

    def index(self, name: str, **kwargs) -> Index:
        """
        Get access to a specific index.

        If the index hasn't been accessed yet, a new index instance will be
        created using the :attr:`~indexer.database.Indexer.index_class` class.
        Otherwise, the previously created index instance will be returned.

        All further options besides the name are passed to the index class which
        by default is :class:`~indexer.index.Index`. Check its documentation
        for further parameters you can pass.

        :param name: The name of the index.
        :param kwargs: Keyword arguments to pass to the index class constructor
        """

        if name in self._indexs:
            return self._indexs[name]

        index = self.index_class(self.storage, name, **kwargs)
        self._indexs[name] = index

        return index

    def indexs(self) -> Set[str]:
        """
        Get the names of all indexs in the database.

        :returns: a set of index names
        """

        # Indexer stores data as a dict of indexs like this:
        #
        #   {
        #       '_default': {
        #           0: {document...},
        #           1: {document...},
        #       },
        #       'index1': {
        #           ...
        #       }
        #   }
        #
        # To get a set of index names, we thus construct a set of this main
        # dict which returns a set of the dict keys which are the index names.
        #
        # Storage.read() may return ``None`` if the database file is empty,
        # so we need to consider this case to and return an empty set in this
        # case.

        return set(self.storage.read() or {})

    def drop_indexs(self) -> None:
        """
        Drop all indexs from the database. **CANNOT BE REVERSED!**
        """

        # We drop all indexs from this database by writing an empty dict
        # to the storage thereby returning to the initial state with no indexs.
        self.storage.write({})

        # After that we need to remember to empty the ``_indexs`` dict, so we'll
        # create new index instances when a index is accessed again.
        self._indexs.clear()

    def drop_index(self, name: str) -> None:
        """
        Drop a specific index from the database. **CANNOT BE REVERSED!**

        :param name: The name of the index to drop.
        """

        # If the index is currently opened, we need to forget the index class
        # instance
        if name in self._indexs:
            del self._indexs[name]

        data = self.storage.read()

        # The database is uninitialized, there's nothing to do
        if data is None:
            return

        # The index does not exist, there's nothing to do
        if name not in data:
            return

        # Remove the index from the data dict
        del data[name]

        # Store the updated data back to the storage
        self.storage.write(data)

    @property
    def storage(self) -> Storage:
        """
        Get the storage instance used for this Indexer instance.

        :return: This instance's storage
        :rtype: Storage
        """
        return self._storage

    def close(self) -> None:
        """
        Close the database.

        This may be needed if the storage instance used for this database
        needs to perform cleanup operations like closing file handles.

        To ensure this method is called, the Indexer instance can be used as a
        context manager::

            with Indexer('data.json') as db:
                db.insert({'foo': 'bar'})

        Upon leaving this context, the ``close`` method will be called.
        """
        self._opened = False
        self.storage.close()

    def __enter__(self):
        """
        Use the database as a context manager.

        Using the database as a context manager ensures that the
        :meth:`~indexer.database.Indexer.close` method is called upon leaving
        the context.

        :return: The current instance
        """
        return self

    def __exit__(self, *args):
        """
        Close the storage instance when leaving a context.
        """
        if self._opened:
            self.close()

    def __getattr__(self, name):
        """
        Forward all unknown attribute calls to the default index instance.
        """
        return getattr(self.index(self.default_index_name), name)

    # Here we forward magic methods to the default index instance. These are
    # not handled by __getattr__ so we need to forward them manually here

    def __len__(self):
        """
        Get the total number of documents in the default index.

        >>> db = Indexer('db.json')
        >>> len(db)
        0
        """
        return len(self.index(self.default_index_name))

    def __iter__(self) -> Iterator[Document]:
        """
        Return an iterator for the default index's documents.
        """
        return iter(self.index(self.default_index_name))
