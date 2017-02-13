from typing import Union

import datetime
import decimal
import importlib
import json
import re

import stellata.database
import stellata.field
import stellata.index
import stellata.relation
import stellata.query

_models = []

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

class Model(metaclass=ModelType):
    """Model definition.

    Each model corresponds to a database table.
    """

    __table__ = None
    __database__ = None

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def create(cls, data: Union[list, dict], unique=None):
        return stellata.query.Query(cls).create(data, unique)

    def to_dict(self):
        return self.__dict__

    @classmethod
    def join(cls, relation: 'stellata.relation.Relation'):
        return stellata.query.Query(cls, joins=[stellata.query.JoinExpression(relation)])

    @classmethod
    def on(cls, database):
        return stellata.query.Query(cls, database=database)

    @classmethod
    def order(cls, fields: list, order=None):
        return stellata.query.Query(cls, order=stellata.query.OrderByExpression(fields, order))

    @classmethod
    def where(cls, expression: 'stellata.query.Expression'):
        return stellata.query.Query(cls, where=expression)

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
        elif hasattr(obj, 'json_encode'):
            return obj.json_encode()

        return obj

    if format == 'json':
        if pretty:
            return json.dumps(data, default=encode, indent=4)
        return json.dumps(data, default=encode)

    return data
