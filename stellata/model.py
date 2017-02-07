import datetime
import decimal
import importlib
import json
import re

import stellata.database
import stellata.field
import stellata.relation
import stellata.query

class ModelType(type):
    """Metaclass for models.

    Attaches a few attributes to field definitions, like the column and table.
    """

    def __new__(cls, name, bases, namespace, **kwargs):
        namespace['__fields__'] = []
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
                namespace['__fields__'].append(column)

            if isinstance(field, stellata.relation.Relation):
                # store the lvalue of the relation definition in the relation itself so we can reference it in queries
                # for example, if we have `r = Relation()`, then we have `r.column == 'r'`
                field.column = column

                # store the model class so instances can be created from queries
                field.model = class_instance

                # store all relations defined on the model so we can go from model -> relation in joins
                namespace['__relations__'].append(field)

        return class_instance

class Model(metaclass=ModelType):
    """Model definition.

    Each model corresponds to a database table.
    """

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    @classmethod
    def create(cls, data: dict, unique: bool = None):
        """Execute an INSERT query."""

        # accept both a list and single dictionary as an argument
        one = False
        if not isinstance(data, list):
            data = [data]
            one = True

        if len(data) == 0:
            return

        # construct list of field names and placeholders for escaped values
        columns = list(data[0].keys())
        fields = ' (%s)' % ','.join(sorted(columns))
        values = ' values ' + ','.join([
            '(%s)' % ','.join(['%s'] * len(data[0]))
        ] * len(data))
        returning = ' returning id'
        args = [i[1] for j in data for i in sorted(j.items())]

        # handle unique indexes
        unique_string = ''
        if unique:
            # if no columns are given, then update all columns
            unique_columns = unique
            if not isinstance(unique_columns, tuple) and not isinstance(unique_columns, list):
                unique_columns = [unique_columns]

            update_columns = columns
            if isinstance(unique, dict):
                unique_columns = unique['columns']
                update_columns = unique.get('update', [])

            unique_string = ' on conflict (%s) do update set %s' % (
                ','.join(unique_columns),
                ', '.join(['%s = excluded.%s' % (column, column) for column in update_columns])
            )

        # concatenate query parts and execute
        sql = 'insert into "' + cls.__table__ + '"' + fields + values + unique_string + returning
        row_ids = stellata.database.query(sql, args)

        # add the last insert ID so the returned object has an ID
        if one:
            data = data[0]
            data['id'] = row_ids[0][0]
            return cls(**data)

        result = []
        for row, row_id in zip(data, row_ids):
            row['id'] = row_id[0]
            result.append(cls(**row))
        return result

    def to_dict(self):
        return self.__dict__

    @classmethod
    def join(cls, relation):
        return stellata.query.Query(cls, joins=[stellata.query.JoinExpression(relation)])

    @classmethod
    def order(cls, fields, order=None):
        return stellata.query.Query(cls, order=stellata.query.OrderByExpression(fields, order))

    @classmethod
    def where(cls, expression):
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
