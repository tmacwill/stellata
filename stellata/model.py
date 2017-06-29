from typing import Union

import contextlib
import datetime
import decimal
import importlib
import json
import msgpack
import re

import stellata.database
import stellata.field
import stellata.index
import stellata.relation
import stellata.query

_models = []
_join_type = None

class ModelType(type):
    """Metaclass for models.

    Attaches a few attributes to field definitions, like the column and table.
    """

    def __new__(cls, name, bases, namespace, **kwargs):
        global _models

        namespace['__fields__'] = []
        namespace['__indexes__'] = []
        namespace['__relations__'] = []

        class_instance = super().__new__(cls, name, bases, namespace)

        for column, field in namespace.items():
            if isinstance(field, stellata.field.Field):
                # store the lvalue of the field definition in the field itself so we can reference it in queries
                # for example, if we have `id = Field()`, then we have `id.column == 'id'`
                field.column = column

                # store the model class so instances can be created from queries
                field.model = class_instance

                # store all fields defined on the model so we can serialize later
                namespace['__fields__'].append(field)

            if isinstance(field, stellata.relation.Relation):
                # store the lvalue of the relation definition in the relation itself so we can reference it in queries
                # for example, if we have `r = Relation()`, then we have `r.column == 'r'`
                field.column = column

                # store the model class so instances can be created from queries
                field.model = class_instance

                # store all relations defined on the model so we can go from model -> relation in joins
                namespace['__relations__'].append(field)

            if isinstance(field, stellata.index.Index):
                # store the lvalue of the relation definition in the relation itself so we can reference it in queries
                # for example, if we have `i = Index()`, then we have `i.column == 'i'`
                field.column = column

                # store the model class so instances can be created from queries
                field.model = class_instance

                # store all relations defined on the model so we can go from model -> relation in joins
                namespace['__indexes__'].append(field)

        _models.append(class_instance)
        return class_instance

class Model(object, metaclass=ModelType):
    """Model definition.

    Each model corresponds to a database table.
    """

    __table__ = None
    __database__ = None

    def __init__(self, *args, **kwargs):
        # set all values given in constructor
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __getattribute__(self, attribute):
        # when accessing fields that haven't been set on instances, return None rather than a meta object
        result = object.__getattribute__(self, attribute)
        if isinstance(result, stellata.field.Field) or isinstance(result, stellata.relation.Relation) \
                or isinstance(result, stellata.index.Index):
            return None

        return result

    def to_dict(self):
        return self.__dict__

    @classmethod
    def begin(cls, database=None):
        cls.execute('begin', database=database)

    @classmethod
    def commit(cls, database=None):
        cls.execute('commit', database=database)

    @classmethod
    def create(cls, data, unique=None):
        if not data:
            return
        return stellata.query.Query(cls).create(data, unique)

    @classmethod
    def execute(cls, sql: str, args: tuple = None, database=None):
        db = database
        if not db:
            db = cls.__database__
        if not db:
            db = stellata.database.pool
        if not db:
            return

        db.execute(sql, args)

    @classmethod
    def find(cls, ids, field=None):
        one = False
        if not isinstance(ids, list):
            ids = [ids]
            one = True

        if not field:
            field = cls.id

        result = cls.where(field << ids).get()
        if one:
            return result[0] if len(result) > 0 else None

        return {getattr(e, field.column): e for e in result}

    @classmethod
    def get(cls):
        return stellata.query.Query(cls).get()

    @classmethod
    def join(cls, relation: 'stellata.relation.Relation'):
        return stellata.query.Query(cls, joins=[stellata.query.JoinExpression(relation)])

    @classmethod
    def join_with(cls, join_type):
        return stellata.query.Query(cls, join_type=join_type)

    @classmethod
    def limit(cls, n):
        return stellata.query.Query(cls, limit=stellata.query.LimitExpression(n))

    def save(self, unique=False):
        return self.__class__.create(self, unique=unique)

    @classmethod
    def on(cls, database):
        return stellata.query.Query(cls, database=database)

    @classmethod
    def order(cls, fields: list, order=None):
        return stellata.query.Query(cls, order=stellata.query.OrderByExpression(fields, order))

    @classmethod
    def truncate(cls, database=None):
        cls.execute('truncate "%s"' % cls.__table__, database=database)

    @classmethod
    def update(cls, data: 'stellata.model.Model'):
        return stellata.query.Query(cls).update(data)

    @classmethod
    def where(cls, expression: 'stellata.query.Expression'):
        return stellata.query.Query(cls, where=expression)

@contextlib.contextmanager
def _join_with(join_type: str):
    global _join_type
    previous = _join_type
    _join_type = join_type
    yield
    _join_type = previous

def registered():
    global _models
    return [e for e in _models if hasattr(e, '__table__') and e.__table__]

def serialize(data, format: str = 'json', pretty: bool = False):
    """Serialize a stellata object to a string format."""
    def encode(obj):
        if isinstance(obj, stellata.model.Model):
            return obj.to_dict()
        elif isinstance(obj, datetime.datetime):
            return int(obj.timestamp())
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif hasattr(obj, 'serialize'):
            return obj.serialize()

        return obj

    if format == 'msgpack':
        return msgpack.packb(data, default=encode)

    if format == 'json':
        if pretty:
            return json.dumps(data, default=encode, indent=4)
        return json.dumps(data, default=encode)

    return data
