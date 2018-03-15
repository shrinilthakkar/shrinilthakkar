import base64
import datetime
import re
from abc import ABCMeta, abstractmethod
from math import isnan, isinf
from uuid import UUID

import bson
import bson.json_util

from moengage.commons.loggers.context_logger import ContextLogger

RE_TYPE = type(re.compile(""))
try:
    from bson.regex import Regex

    RE_TYPES = (RE_TYPE, Regex)
except ImportError:
    RE_TYPES = (RE_TYPE,)


class DocumentFormatterBase(ContextLogger):
    __metaclass__ = ABCMeta

    """Basic DocumentFormatter that preserves numbers, base64-encodes binary,
    and stringifies everything else.
    """

    def __init__(self, db_name):
        self.db_name = db_name

    def transform_value(self, value):
        # This is largely taken from bson.json_util.default, though not the same
        # so we don't modify the structure of the document
        if isinstance(value, dict):
            return self.format_document(value, False)
        elif isinstance(value, list):
            return [self.transform_value(v) for v in value]
        if isinstance(value, RE_TYPES):
            flags = ""
            if value.flags & re.IGNORECASE:
                flags += "i"
            if value.flags & re.LOCALE:
                flags += "l"
            if value.flags & re.MULTILINE:
                flags += "m"
            if value.flags & re.DOTALL:
                flags += "s"
            if value.flags & re.UNICODE:
                flags += "u"
            if value.flags & re.VERBOSE:
                flags += "x"
            pattern = value.pattern
            # quasi-JavaScript notation (may include non-standard flags)
            return '/%s/%s' % (pattern, flags)
        elif isinstance(value, basestring):
            return unicode(value)
        elif isinstance(value, bson.Binary) or (isinstance(value, bytes)):
            # Just include body of binary data without subtype
            return base64.b64encode(value).decode()
        elif isinstance(value, UUID):
            return value.hex
        elif isinstance(value, (int, long, float)):
            if isnan(value):
                raise ValueError("nan")
            elif isinf(value):
                raise ValueError("inf")
            return value
        elif isinstance(value, datetime.datetime):
            return value
        elif value is None:
            return value
        # Default
        return unicode(value)

    def transform_element(self, key, value):
        try:
            new_value = self.transform_value(value)
            yield key, new_value
        except ValueError as e:
            self.logger.warn("Invalid value for key: %s as %s" % (key, str(e)))

    def apply_update(self, doc, update_spec):
        """Apply an update operation to a document."""

        # Helper to cast a key for a list or dict, or raise ValueError
        def _convert_or_raise(container, key):
            if isinstance(container, dict):
                return key
            elif isinstance(container, list):
                return int(key)
            else:
                raise ValueError

        # Helper to retrieve (and/or create)
        # a dot-separated path within a document.
        def _retrieve_path(container, path, create=False):
            looking_at = container
            for part in path:
                if isinstance(looking_at, dict):
                    if create and part not in looking_at:
                        looking_at[part] = {}
                    looking_at = looking_at[part]
                elif isinstance(looking_at, list):
                    index = int(part)
                    # Do we need to create additional space in the array?
                    if create and len(looking_at) <= index:
                        # Fill buckets with None up to the index we need.
                        looking_at.extend(
                            [None] * (index - len(looking_at)))
                        # Bucket we need gets the empty dictionary.
                        looking_at.append({})
                    looking_at = looking_at[index]
                else:
                    raise ValueError
            return looking_at

        # wholesale document replacement
        if "$set" not in update_spec and "$unset" not in update_spec:
            # update spec contains the new document in its entirety
            return update_spec
        else:
            for to_set in update_spec.get("$set", []):
                value = update_spec['$set'][to_set]
                if '.' in to_set:
                    path = to_set.split(".")
                    where = _retrieve_path(doc, path[:-1], create=True)
                    wl = len(where)
                    index = _convert_or_raise(where, path[-1])
                    if isinstance(where, list) and index >= wl:
                        where.extend([None] * (index + 1 - wl))
                    where[index] = value
                else:
                    doc[to_set] = value

            # $unset
            for to_unset in update_spec.get("$unset", []):
                if '.' in to_unset:
                    path = to_unset.split(".")
                    where = _retrieve_path(doc, path[:-1])
                    where.pop(_convert_or_raise(where, path[-1]), None)
                else:
                    doc.pop(to_unset, None)
            return doc

    @abstractmethod
    def format_document(self, document, first_call=True):
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.format_document.__name__]))

    @abstractmethod
    def pop_excluded_fields(self, doc, db_name, collection):
        """IMPORTANT: this is class method, override it with @classmethod!"""
        raise NotImplementedError(
            "Child Class must implement {0} function from super".format([self.pop_excluded_fields.__name__]))
