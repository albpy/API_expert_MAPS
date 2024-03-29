"""
This module implements indexs, the central place for accessing and manipulating
data in Indexer.
"""

from collections import defaultdict
from typing import ( Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Union, cast, Tuple)

from .queries import QueryLike
from .storages import Storage
from .utils import LRUCache

__all__ = ("Document", "Index")


class Document(dict):
    """
    A document stored in the index.

    This class provides a way to access both a document's content and
    its ID using ``doc.doc_id``.
    """

    def __init__(self, value: Mapping, doc_id: int):
        super().__init__(value)
        self.doc_id = doc_id


class Index:
    """
    Represents a single Indexer index.

    It provides methods for accessing and manipulating documents.

    .. admonition:: Query Cache

        As an optimization, a query cache is implemented using a
        :class:`~indexer.utils.LRUCache`. This class mimics the interface of
        a normal ``dict``, but starts to remove the least-recently used entries
        once a threshold is reached.

        The query cache is updated on every search operation. When writing
        data, the whole cache is discarded as the query results may have
        changed.

    .. admonition:: Customization

        For customization, the following class variables can be set:

        - ``document_class`` defines the class that is used to represent
          documents,
        - ``document_id_class`` defines the class that is used to represent
          document IDs,
        - ``query_cache_class`` defines the class that is used for the query
          cache
        - ``default_query_cache_capacity`` defines the default capacity of
          the query cache

        .. versionadded:: 4.0


    :param storage: The storage instance to use for this index
    :param name: The index name
    :param cache_size: Maximum capacity of query cache
    """

    #: The class used to represent documents
    #:
    #: .. versionadded:: 4.0
    document_class = Document

    #: The class used to represent a document ID
    #:
    #: .. versionadded:: 4.0
    document_id_class = int

    #: The class used for caching query results
    #:
    #: .. versionadded:: 4.0
    query_cache_class = LRUCache

    #: The default capacity of the query cache
    #:
    #: .. versionadded:: 4.0
    default_query_cache_capacity = 10

    def __init__(
        self,
        storage: Storage,
        name: str,
        cache_size: int = default_query_cache_capacity,
    ):
        """
        Create a index instance.
        """

        self._storage = storage
        self._name = name
        self._query_cache: LRUCache[QueryLike, List[Document]] = self.query_cache_class(
            capacity=cache_size
        )

        self._next_id = None

    def __repr__(self):
        args = [
            "name={!r}".format(self.name),
            "total={}".format(len(self)),
            "storage={}".format(self._storage),
        ]

        return "<{} {}>".format(type(self).__name__, ", ".join(args))

    @property
    def name(self) -> str:
        """
        Get the index name.
        """
        return self._name

    @property
    def storage(self) -> Storage:
        """
        Get the index storage instance.
        """
        return self._storage

    def insert(self, document: Mapping) -> int:
        """
        Insert a new document into the index.

        :param document: the document to insert
        :returns: the inserted document's ID
        """

        # Make sure the document implements the ``Mapping`` interface
        if not isinstance(document, Mapping):
            raise ValueError("Document is not a Mapping")

        # First, we get the document ID for the new document
        if isinstance(document, Document):
            # For a `Document` object we use the specified ID
            doc_id = document.doc_id

            # We also reset the stored next ID so the next insert won't
            # re-use document IDs by accident when storing an old value
            self._next_id = None
        else:
            # In all other cases we use the next free ID
            doc_id = self._get_next_id()

        # Now, we update the index and add the document
        def updater(index: dict):
            if doc_id in index:
                raise ValueError(f"Document with ID {str(doc_id)} " f"already exists")

            # By calling ``dict(document)`` we convert the data we got to a
            # ``dict`` instance even if it was a different class that
            # implemented the ``Mapping`` interface
            index[doc_id] = dict(document)

        # See below for details on ``Index._update``
        self._update_index(updater)

        return doc_id

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """
        Insert multiple documents into the index.

        :param documents: an Iterable of documents to insert
        :returns: a list containing the inserted documents' IDs
        """
        doc_ids = []

        def updater(index: dict):
            for document in documents:
                # Make sure the document implements the ``Mapping`` interface
                if not isinstance(document, Mapping):
                    raise ValueError("Document is not a Mapping")

                if isinstance(document, Document):
                    # Check if document does not override an existing document
                    if document.doc_id in index:
                        raise ValueError(
                            f"Document with ID {str(document.doc_id)} "
                            f"already exists"
                        )

                    # Store the doc_id, so we can return all document IDs
                    # later. Then save the document with its doc_id and
                    # skip the rest of the current loop
                    doc_id = document.doc_id
                    doc_ids.append(doc_id)
                    index[doc_id] = dict(document)
                    continue

                # Generate new document ID for this document
                # Store the doc_id, so we can return all document IDs
                # later, then save the document with the new doc_id
                doc_id = self._get_next_id()
                doc_ids.append(doc_id)
                index[doc_id] = dict(document)

        # See below for details on ``Index._update``
        self._update_index(updater)

        return doc_ids

    def all(self) -> List[Document]:
        """
        Get all documents stored in the index.

        :returns: a list with all documents.
        """

        # iter(self) (implemented in Index.__iter__ provides an iterator
        # that returns all documents in this index. We use it to get a list
        # of all documents by using the ``list`` constructor to perform the
        # conversion.

        return list(iter(self))

    def search(self, cond: QueryLike, return_keys: Optional[List[str]] = None, orient: str = "records", unique: Optional[bool]=False, check_cached: Optional[bool]=False) -> Union[List[Document], List[Dict[str, Any]]]:
        """
        Search for all documents matching a 'where' cond.

        :param cond: the condition to check against
        :param return_keys: list of keys to return
        :param orient: the orientation of the output ("records" or "list")
        :param unique: return unique values in case of "list" orientation
        :param check_cached: return the cached results if the same query is used before

        :returns: list of matching documents in the specified orientation
        """
        
        if orient not in ["records", "list"]:
            raise ValueError("Invalid orient value. Accepted values are 'records' or 'list'.")

        if check_cached:
            # We check the query cache to see if it has results for this query
            cached_results = self._query_cache.get(cond)
            if cached_results is not None:
                return cached_results[:]

        # Perform the search by applying the query to all documents.
        # Then, only if the document matches the query, convert it
        # to the document class and document ID class.
        docs = [
            self.document_class(doc, self.document_id_class(doc_id))
            for doc_id, doc in self._read_index().items()
            if cond(doc)
        ]

        if return_keys:
            filtered_docs = []
            for doc in docs:
                doc_keys = dict(doc).keys()
                filtered_doc = {}
                for r_key in return_keys:
                    if r_key in doc_keys:
                        filtered_doc[r_key] = doc[r_key]
                filtered_docs.append(filtered_doc)
            docs = filtered_docs

        # This weird `getattr` dance is needed to make MyPy happy as
        # it doesn't know that a query might have a `is_cacheable` method
        # that is not declared in the `QueryLike` protocol due to it being
        # optional.
        is_cacheable: Callable[[], bool] = getattr(cond, "is_cacheable", lambda: True)
        if is_cacheable():
            # Update the query cache
            self._query_cache[cond] = docs[:]

        if orient == "records":
            return docs
        elif orient== "list":
            docs_list = defaultdict(list)

            for item in docs:
                for key, value in item.items():
                    if unique and value in docs_list[key]: continue
                    docs_list[key].append(value)
            
            return dict(docs_list)

    def get(
        self,
        cond: Optional[QueryLike] = None,
        doc_id: Optional[int] = None,
        doc_ids: Optional[List] = None,
    ) -> Optional[Union[Document, List[Document]]]:
        """
        Get exactly one document specified by a query or a document ID.
        However, if multiple document IDs are given then returns all
        documents in a list.

        Returns ``None`` if the document doesn't exist.

        :param cond: the condition to check against
        :param doc_id: the document's ID
        :param doc_ids: the document's IDs(multiple)

        :returns: the document(s) or ``None``
        """
        index = self._read_index()

        if doc_id is not None:
            # Retrieve a document specified by its ID
            raw_doc = index.get(str(doc_id), None)

            if raw_doc is None:
                return None

            # Convert the raw data to the document class
            return self.document_class(raw_doc, doc_id)

        elif doc_ids is not None:
            # Filter the index by extracting out all those documents which
            # have doc id specified in the doc_id list.

            # Since document IDs will be unique, we make it a set to ensure
            # constant time lookup
            doc_ids_set = set(str(doc_id) for doc_id in doc_ids)

            # Now return the filtered documents in form of list
            return [
                self.document_class(doc, self.document_id_class(doc_id))
                for doc_id, doc in index.items()
                if doc_id in doc_ids_set
            ]

        elif cond is not None:
            # Find a document specified by a query
            # The trailing underscore in doc_id_ is needed so MyPy
            # doesn't think that `doc_id_` (which is a string) needs
            # to have the same type as `doc_id` which is this function's
            # parameter and is an optional `int`.
            for doc_id_, doc in self._read_index().items():
                if cond(doc):
                    return self.document_class(doc, self.document_id_class(doc_id_))

            return None

        raise RuntimeError("You have to pass either cond or doc_id or doc_ids")

    def contains(
        self, cond: Optional[QueryLike] = None, doc_id: Optional[int] = None
    ) -> bool:
        """
        Check whether the index contains a document matching a query or
        an ID.

        If ``doc_id`` is set, it checks if the db contains the specified ID.

        :param cond: the condition use
        :param doc_id: the document ID to look for
        """
        if doc_id is not None:
            # Documents specified by ID
            return self.get(doc_id=doc_id) is not None

        elif cond is not None:
            # Document specified by condition
            return self.get(cond) is not None

        raise RuntimeError("You have to pass either cond or doc_id")

    def update(
        self,
        fields: Union[Mapping, Callable[[Mapping], None]],
        cond: Optional[QueryLike] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        """
        Update all matching documents to have a given set of fields.

        :param fields: the fields that the matching documents will have
                       or a method that will update the documents
        :param cond: which documents to update
        :param doc_ids: a list of document IDs
        :returns: a list containing the updated document's ID
        """

        # Define the function that will perform the update
        if callable(fields):

            def perform_update(index, doc_id):
                # Update documents by calling the update function provided by
                # the user
                fields(index[doc_id])

        else:

            def perform_update(index, doc_id):
                # Update documents by setting all fields from the provided data
                index[doc_id].update(fields)

        if doc_ids is not None:
            # Perform the update operation for documents specified by a list
            # of document IDs

            updated_ids = list(doc_ids)

            def updater(index: dict):
                # Call the processing callback with all document IDs
                for doc_id in updated_ids:
                    perform_update(index, doc_id)

            # Perform the update operation (see _update_index for details)
            self._update_index(updater)

            return updated_ids

        elif cond is not None:
            # Perform the update operation for documents specified by a query

            # Collect affected doc_ids
            updated_ids = []

            def updater(index: dict):
                _cond = cast(QueryLike, cond)

                # We need to convert the keys iterator to a list because
                # we may remove entries from the ``index`` dict during
                # iteration and doing this without the list conversion would
                # result in an exception (RuntimeError: dictionary changed size
                # during iteration)
                for doc_id in list(index.keys()):
                    # Pass through all documents to find documents matching the
                    # query. Call the processing callback with the document ID
                    if _cond(index[doc_id]):
                        # Add ID to list of updated documents
                        updated_ids.append(doc_id)

                        # Perform the update (see above)
                        perform_update(index, doc_id)

            # Perform the update operation (see _update_index for details)
            self._update_index(updater)

            return updated_ids

        else:
            # Update all documents unconditionally

            updated_ids = []

            def updater(index: dict):
                # Process all documents
                for doc_id in list(index.keys()):
                    # Add ID to list of updated documents
                    updated_ids.append(doc_id)

                    # Perform the update (see above)
                    perform_update(index, doc_id)

            # Perform the update operation (see _update_index for details)
            self._update_index(updater)

            return updated_ids

    def update_multiple(
        self,
        updates: Iterable[Tuple[Union[Mapping, Callable[[Mapping], None]], QueryLike]],
    ) -> List[int]:
        """
        Update all matching documents to have a given set of fields.

        :returns: a list containing the updated document's ID
        """

        # Define the function that will perform the update
        def perform_update(fields, index, doc_id):
            if callable(fields):
                # Update documents by calling the update function provided
                # by the user
                fields(index[doc_id])
            else:
                # Update documents by setting all fields from the provided
                # data
                index[doc_id].update(fields)

        # Perform the update operation for documents specified by a query

        # Collect affected doc_ids
        updated_ids = []

        def updater(index: dict):
            # We need to convert the keys iterator to a list because
            # we may remove entries from the ``index`` dict during
            # iteration and doing this without the list conversion would
            # result in an exception (RuntimeError: dictionary changed size
            # during iteration)
            for doc_id in list(index.keys()):
                for fields, cond in updates:
                    _cond = cast(QueryLike, cond)

                    # Pass through all documents to find documents matching the
                    # query. Call the processing callback with the document ID
                    if _cond(index[doc_id]):
                        # Add ID to list of updated documents
                        updated_ids.append(doc_id)

                        # Perform the update (see above)
                        perform_update(fields, index, doc_id)

        # Perform the update operation (see _update_index for details)
        self._update_index(updater)

        return updated_ids

    def upsert(self, document: Mapping, cond: Optional[QueryLike] = None) -> List[int]:
        """
        Update documents, if they exist, insert them otherwise.

        Note: This will update *all* documents matching the query. Document
        argument can be a indexer.index.Document object if you want to specify a
        doc_id.

        :param document: the document to insert or the fields to update
        :param cond: which document to look for, optional if you've passed a
        Document with a doc_id
        :returns: a list containing the updated documents' IDs
        """

        # Extract doc_id
        if isinstance(document, Document) and hasattr(document, "doc_id"):
            doc_ids: Optional[List[int]] = [document.doc_id]
        else:
            doc_ids = None

        # Make sure we can actually find a matching document
        if doc_ids is None and cond is None:
            raise ValueError(
                "If you don't specify a search query, you must "
                "specify a doc_id. Hint: use a index.Document "
                "object."
            )

        # Perform the update operation
        try:
            updated_docs: Optional[List[int]] = self.update(document, cond, doc_ids)
        except KeyError:
            # This happens when a doc_id is specified, but it's missing
            updated_docs = None

        # If documents have been updated: return their IDs
        if updated_docs:
            return updated_docs

        # There are no documents that match the specified query -> insert the
        # data as a new document
        return [self.insert(document)]

    def remove(
        self,
        cond: Optional[QueryLike] = None,
        doc_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        """
        Remove all matching documents.

        :param cond: the condition to check against
        :param doc_ids: a list of document IDs
        :returns: a list containing the removed documents' ID
        """
        if doc_ids is not None:
            # This function returns the list of IDs for the documents that have
            # been removed. When removing documents identified by a set of
            # document IDs, it's this list of document IDs we need to return
            # later.
            # We convert the document ID iterator into a list, so we can both
            # use the document IDs to remove the specified documents and
            # to return the list of affected document IDs
            removed_ids = list(doc_ids)

            def updater(index: dict):
                for doc_id in removed_ids:
                    index.pop(doc_id)

            # Perform the remove operation
            self._update_index(updater)

            return removed_ids

        if cond is not None:
            removed_ids = []

            # This updater function will be called with the index data
            # as its first argument. See ``Index._update`` for details on this
            # operation
            def updater(index: dict):
                # We need to convince MyPy (the static type checker) that
                # the ``cond is not None`` invariant still holds true when
                # the updater function is called
                _cond = cast(QueryLike, cond)

                # We need to convert the keys iterator to a list because
                # we may remove entries from the ``index`` dict during
                # iteration and doing this without the list conversion would
                # result in an exception (RuntimeError: dictionary changed size
                # during iteration)
                for doc_id in list(index.keys()):
                    if _cond(index[doc_id]):
                        # Add document ID to list of removed document IDs
                        removed_ids.append(doc_id)

                        # Remove document from the index
                        index.pop(doc_id)

            # Perform the remove operation
            self._update_index(updater)

            return removed_ids

        raise RuntimeError("Use truncate() to remove all documents")

    def truncate(self) -> None:
        """
        Truncate the index by removing all documents.
        """

        # Update the index by resetting all data
        self._update_index(lambda index: index.clear())

        # Reset document ID counter
        self._next_id = None

    def count(self, cond: QueryLike) -> int:
        """
        Count the documents matching a query.

        :param cond: the condition use
        """

        return len(self.search(cond))

    def clear_cache(self) -> None:
        """
        Clear the query cache.
        """

        self._query_cache.clear()

    def __len__(self):
        """
        Count the total number of documents in this index.
        """

        return len(self._read_index())

    def __iter__(self) -> Iterator[Document]:
        """
        Iterate over all documents stored in the index.

        :returns: an iterator over all documents.
        """

        # Iterate all documents and their IDs
        for doc_id, doc in self._read_index().items():
            # Convert documents to the document class
            yield self.document_class(doc, self.document_id_class(doc_id))

    def _get_next_id(self):
        """
        Return the ID for a newly inserted document.
        """

        # If we already know the next ID
        if self._next_id is not None:
            next_id = self._next_id
            self._next_id = next_id + 1

            return next_id

        # Determine the next document ID by finding out the max ID value
        # of the current index documents

        # Read the index documents
        index = self._read_index()

        # If the index is empty, set the initial ID
        if not index:
            next_id = 1
            self._next_id = next_id + 1

            return next_id

        # Determine the next ID based on the maximum ID that's currently in use
        max_id = max(self.document_id_class(i) for i in index.keys())
        next_id = max_id + 1

        # The next ID we will return AFTER this call needs to be larger than
        # the current next ID we calculated
        self._next_id = next_id + 1

        return next_id

    def _read_index(self) -> Dict[str, Mapping]:
        """
        Read the index data from the underlying storage.

        Documents and doc_ids are NOT yet transformed, as
        we may not want to convert *all* documents when returning
        only one document for example.
        """

        # Retrieve the indexs from the storage
        indexs = self._storage.read()

        if indexs is None:
            # The index is empty
            return {}

        # Retrieve the current index's data
        try:
            index = indexs[self.name]
        except KeyError:
            # The index does not exist yet, so it is empty
            return {}

        return index

    def _update_index(self, updater: Callable[[Dict[int, Mapping]], None]):
        """
        Perform a index update operation.

        The storage interface used by Indexer only allows to read/write the
        complete index data, but not modifying only portions of it. Thus,
        to only update portions of the index data, we first perform a read
        operation, perform the update on the index data and then write
        the updated data back to the storage.

        As a further optimization, we don't convert the documents into the
        document class, as the index data will *not* be returned to the user.
        """

        indexs = self._storage.read()

        if indexs is None:
            # The index is empty
            indexs = {}

        try:
            raw_index = indexs[self.name]
        except KeyError:
            # The index does not exist yet, so it is empty
            raw_index = {}

        # Convert the document IDs to the document ID class.
        # This is required as the rest of Indexer expects the document IDs
        # to be an instance of ``self.document_id_class`` but the storage
        # might convert dict keys to strings.
        index = {
            self.document_id_class(doc_id): doc for doc_id, doc in raw_index.items()
        }

        # Perform the index update operation
        updater(index)

        # Convert the document IDs back to strings.
        # This is required as some storages (most notably the JSON file format)
        # don't support IDs other than strings.
        indexs[self.name] = {str(doc_id): doc for doc_id, doc in index.items()}

        # Write the newly updated data back to the storage
        self._storage.write(indexs)

        # Clear the query cache, as the index contents have changed
        self.clear_cache()
