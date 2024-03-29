from datetime import datetime
from abc import ABC, abstractmethod
from copy import deepcopy

from .database import Indexer
from .middlewares import Middleware

iteritems = getattr(dict, 'iteritems', dict.items)
itervalues = getattr(dict, 'itervalues', dict.values)

class Serializer(ABC):
    """
    The abstract base class for Serializers.
    Allows Indexer to handle arbitrary objects by running them through a list
    of registered serializers.
    Every serializer has to tell which class it can handle.
    """

    OBJ_CLASS: object

    @abstractmethod
    def encode(self, obj):
        """
        Encode an object.
        :param obj:
        :return:
        :rtype: str
        """
        raise NotImplementedError('To be overridden!')

    @abstractmethod
    def decode(self, s):
        """
        Decode an object.
        :param s:
        :type s: str
        :return:
        """
        raise NotImplementedError('To be overridden!')


def _enumerate_element(element):
    """
    Make an element enumerable.

    For dicts: return an iterator over the items (key, value).
    For lists/tuples: return an iterator over (index, item)
    """

    if isinstance(element, dict):
        return iteritems(element)
    else:
        return enumerate(element)


def _decode_deep(element, serializer, tag):
    """
    Recursively decode an element.

    Takes into account elements in nested dicts, lists and tuples
    """

    for key, value in _enumerate_element(element):
        try:
            typed_tag = tag
            if type(value) == bytes:
                typed_tag = tag.encode()
            if value.startswith(typed_tag):
                encoded = value[len(typed_tag):]
                element[key] = serializer.decode(encoded)

        except AttributeError:
            # Not a string
            if isinstance(value, (dict, list, tuple)):
                _decode_deep(value, serializer, tag)


def _encode_deep(element, serializer, tag, obj_class):
    """
    Recursively encode an element.

    Takes into account elements in nested dicts, lists and tuples
    """

    for key, value in _enumerate_element(element):
        if isinstance(value, obj_class):
            encoded = serializer.encode(value)
            element[key] = (tag if type(encoded) == str else tag.encode()) + encoded

        elif isinstance(value, (dict, list, tuple)):
            _encode_deep(value, serializer, tag, obj_class)


def has_encodeable(element, obj_class):
    """
    Check whether the element in question has an encodeable item.
    """

    found_encodeable = False

    for key, value in _enumerate_element(element):
        if isinstance(value, (dict, list, tuple)):
            found_encodeable |= has_encodeable(value, obj_class)
        else:
            found_encodeable |= isinstance(value, obj_class)

    return found_encodeable


class SerializationMiddleware(Middleware):
    """
    Provide custom serialization for Indexer.
    This middleware allows users of Indexer to register custom serializations.
    The serialized data will be passed to the wrapped storage and data that
    is read from the storage will be deserialized.
    """

    def __init__(self, storage_cls=Indexer.default_storage_class):
        super(SerializationMiddleware, self).__init__(storage_cls)

        self._serializers = {}

    def register_serializer(self, serializer, name):
        """
        Register a new Serializer.
        When reading from/writing to the underlying storage, Indexer
        will run all objects through the list of registered serializers
        allowing each one to handle objects it recognizes.
        .. note:: The name has to be unique among this database instance.
                  Re-using the same name will overwrite the old serializer.
                  Also, registering a serializer will be reflected in all
                  indexs when reading/writing them.
        :param serializer: an instance of the serializer
        :type serializer: indexer.serialize.Serializer
        """
        self._serializers[name] = serializer

    def read(self):
        data = self.storage.read()

        if data is None:
            return None

        for serializer_name in self._serializers:
            serializer = self._serializers[serializer_name]
            tag = '{{{0}}}:'.format(serializer_name)  # E.g:'{TinyDate}:'

            for index_name in data:
                index = data[index_name]

                for eid in index:
                    _decode_deep(data[index_name][eid], serializer, tag)

        return data

    def write(self, data):
        # We only make a copy of the data if any serializer would overwrite
        # existing data.
        data_copied = False

        for serializer_name in self._serializers:
            # If no serializers are registered, this code will just look up
            # the serializer list and continue. But if there are serializers,
            # the inner loop will run very often.
            # For that reason, the lookup of the serialized class is pulled
            # out into the outer loop:

            serializer = self._serializers[serializer_name]
            obj_class = serializer.OBJ_CLASS
            tag = '{{{0}}}:'.format(serializer_name)

            for index_name in data:
                index = data[index_name]

                for eid in index:
                    # Before writing, copy data if we haven't already.
                    if not data_copied and has_encodeable(data[index_name][eid],
                                                         obj_class):
                        data = deepcopy(data)
                        data_copied = True

                    item = data[index_name][eid]
                    _encode_deep(item, serializer, tag, obj_class)

        self.storage.write(data)

class DateTimeSerializer(Serializer):
    OBJ_CLASS = datetime  # The class this serializer handles

    def encode(self, obj):
        return obj.isoformat()

    def decode(self, s):
        return datetime.fromisoformat(s)