from typing import Union

import collections
import random
import string
import stellata.database
import stellata.relations
import stellata.model

class Query:
    """Container class for a SQL query.

    A query is essentially a container for various Expression types, which serialize themselves to SQL.
    """

    def __init__(self, model: type, database=None, joins=None, where=None, order=None, join_type=None):
        if joins is None:
            joins = []

        if joins and not isinstance(joins, list):
            joins = [joins]

        self.model = model
        self.database = database
        self.joins = joins
        self.where_expression = where
        self.order_expression = order
        self.join_type = join_type

    def _delete_query(self):
        query = 'delete from "%s" ' % self.model.__table__
        where_query, where_values = self.where_expression.to_query()
        query += 'where %s' % where_query

        return (query, where_values)

    def _field_aliases(self, model=None, alias=None):
        # use query model by default
        if not model:
            model = self.model

        if not alias:
            alias = model.__table__

        # return a list of fields with aliases that can be used in a SQL query
        return [
            '"%s"."%s" as "%s.%s"' % (alias, field.column, alias, field.column)
            for field in model.__fields__
        ]

    def _get_with_joins(self, one, join_order, join_map):
        result = []

        # each join has a unique string alias to prevent collisions, so create map we can use later
        alias_map = {}
        for join in self.joins:
            alias_map[join.relation.child().model.__table__] = join.alias

        query, values = self._select_query(alias_map)
        rows = self._pool().query(query, values)

        # iterate over joins in order from leaf nodes to root node
        data = {}
        parent_key = None
        for child_model in join_order:
            for join in join_map.get(child_model, []):
                parent_model = join.relation.parent().model
                parent_column = join.relation.parent().column
                parent_table = join.relation.parent().model.__table__
                parent_alias = alias_map.get(parent_table, parent_table)
                parent_key = '%s.id' % parent_alias

                child_model = join.relation.child().model
                child_column = join.relation.child().column
                child_table = join.relation.child().model.__table__
                child_alias = join.alias
                child_key = '%s.id' % child_alias

                many = isinstance(join.relation, stellata.relations.HasMany)
                visited = set()
                for row in rows:
                    # for each join, insert into a table that maps models to their descendents.
                    # the first key is a model name, which maps to each returned model value.
                    # so, for `A1 -> [B1, B2], A2 -> [B3], B1 -> [C1], B2 -> [C2]` we have:
                    # {'b.id': {B1: [C1], B2: [C2], B3: []}, 'a.id': [{A1: [B1.C1, B2.C2], A2: [B3]}]}
                    # the general approach here is that we build up this table backwards, ensuring that all
                    # referenced values are inserted first. then, for each model, we can replace any
                    # fields that are actually references to other models with data already stored in the table.
                    parent_value = row[parent_key]
                    data.setdefault(parent_key, {})
                    data[parent_key].setdefault(parent_value, None)

                    # convert row to model objects, since that's what we'll ultimately return
                    child_row = self._row_to_object(child_model, row, child_alias)
                    parent_row = self._row_to_object(parent_model, row, parent_alias)
                    if data.get(parent_key, {}).get(parent_value):
                        parent_row = data[parent_key][parent_value]

                    # get values stored for this row and alias
                    v = [] if many else None
                    if (
                        hasattr(data[parent_key][parent_value], join.relation.column) and \
                        not isinstance(
                            getattr(data[parent_key][parent_value], join.relation.column),
                            stellata.relation.Relation
                        )
                    ):
                        v = getattr(data[parent_key][parent_value], join.relation.column)

                    # since joins are left joins, skip rows that are empty for the current join
                    if child_row:
                        child_row_id = child_row.id

                        # if data for foreign key already exists, then use that
                        if child_key in data:
                            if many:
                                if child_row_id not in visited:
                                    v.append(data.get(child_key, {}).get(child_row_id, []))
                                    visited.add(child_row_id)
                            else:
                                v = data.get(child_key, {}).get(child_row_id, [])

                        # if no data exists, then we must be at a leaf node, so use the row value
                        else:
                            if many:
                                if child_row_id not in visited:
                                    v.append(child_row)
                                    visited.add(child_row_id)
                            else:
                                v = child_row

                    # insert data into table
                    if parent_row:
                        setattr(parent_row, join.relation.column, v)
                        data[parent_key][parent_value] = parent_row

        # if no rows are returned, then return an empty list
        if not parent_key or parent_key not in data:
            return []

        # final result is stored at the key representing the root model
        result = list(data[parent_key].values())
        if one:
            result = result[0]

        return result

    def _get_with_queries(self, one, join_order, join_map):
        result = {}
        data = {}
        query, values = self._select_query(use_joins=False)
        rows = self._pool().query(query, values)

        # instantiate model objects from rows in initial query
        for row in rows:
            row_object = self._row_to_object(self.model, row)
            data.setdefault(self.model, {})
            data[self.model][row_object.id] = row_object

        # for each join, store all rows in memory, then fetch all child rows for the join
        # we need to store both the actual data as well as the map from parents -> children
        for child_model in reversed(join_order):
            for join in join_map.get(child_model, []):
                belongs_to = isinstance(join.relation, stellata.relations.BelongsTo)
                related_ids = []
                related_field = None
                if belongs_to:
                    related_field = join.relation.child()
                    related_ids = [
                        getattr(row, join.relation.foreign_key().column)
                        for row in data[join.relation.parent().model].values()
                    ]
                else:
                    related_field = join.relation.foreign_key()
                    related_ids = list(data.get(join.relation.parent().model, {}).keys())

                rows = join.relation.child().model.where(related_field << related_ids).get()
                row_ids = []
                for row in rows:
                    data.setdefault(join.relation.child().model, {})
                    data[join.relation.child().model][row.id] = row
                    row_ids.append(row.id)

        # now that all rows are in memory, associate children with their parents by aggregating children
        # by the foreign key and then setting attributes on the parents
        for child_model in join_order[:-1]:
            for join in join_map.get(child_model, []):
                many = isinstance(join.relation, stellata.relations.HasMany)
                belongs_to = isinstance(join.relation, stellata.relations.BelongsTo)

                # for belongs to, index into the child and set the value on the parent
                if belongs_to:
                    for parent in data[join.relation.parent().model].values():
                        setattr(
                            parent,
                            join.relation.column,
                            data[join.relation.child().model].get(getattr(parent, join.relation.foreign_key().column))
                        )

                # for has many/one, aggregate children by their ID, then attach that to the parent
                else:
                    children_by_parent_id = {}
                    for child in data.get(join.relation.child().model, {}).values():
                        parent_id = getattr(child, join.relation.foreign_key().column)
                        if many:
                            children_by_parent_id.setdefault(parent_id, [])
                            children_by_parent_id[parent_id].append(child)
                        else:
                            children_by_parent_id[parent_id] = child

                    for parent_id in data.get(join.relation.parent().model, {}).keys():
                        parent = data[join.relation.parent().model][parent_id]
                        children = children_by_parent_id.get(parent_id, [] if many else None)
                        setattr(parent, join.relation.column, children)

        return list(data.get(join_order[-1], {}).values())

    def _insert_query(self, objects: list, unique=None, one=False):
        # construct list of field names and placeholders for escaped values
        data = [e.to_dict() for e in objects]
        columns = list(data[0].keys())
        fields = ' (%s)' % ','.join(sorted(columns))
        values = ' values ' + ','.join([
            '(%s)' % ','.join(['%s'] * len(data[0]))
        ] * len(data))
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
                ','.join([e.column for e in unique_columns]),
                ', '.join(['%s = excluded.%s' % (column, column) for column in update_columns])
            )

        # concatenate query parts and execute
        returning = ' returning %s' % ','.join(self._field_aliases())
        sql = 'insert into "' + self.model.__table__ + '"' + fields + values + unique_string + returning
        return (sql, args)

    def _pool(self):
        if self.database:
            return self.database

        return stellata.database.pool

    def _row_to_object(self, model: 'stellata.model.Model', row, alias=None):
        empty = True
        data = {}
        if not alias:
            alias = model.__table__

        # iterate over fields defined in the model and extract them from the row dict
        for field in model.__fields__:
            value = row['%s.%s' % (alias, field.column)]
            data[field.column] = value
            if value is not None:
                empty = False

        # don't return a dictionary where every column is null
        if empty:
            return None

        return model(**data)

    def _select_query(self, alias_map=None, use_joins=True):
        alias_map = alias_map or {}
        columns = self._field_aliases()

        # add each join to the list of columns to select
        if use_joins:
            for join in self.joins:
                child = join.relation.child()
                columns += self._field_aliases(child.model, join.alias)

        # base select query
        query = 'select %s from "%s" ' % (','.join(columns), self.model.__table__)

        # add each join clause
        if self.joins and use_joins:
            query += '%s ' % ' '.join(e.to_query(alias_map) for e in self.joins)

        # add where clause
        where_values = tuple()
        if self.where_expression:
            where_query, where_values = self.where_expression.to_query(alias_map)
            query += 'where %s' % where_query

        if self.order_expression:
            query += ' %s' % self.order_expression.to_query(alias_map)

        return (query, where_values)

    def _update_query(self, data: 'stellata.model.Model'):
        values = []
        query = 'update "%s" ' % self.model.__table__

        update = ','.join([e + ' = %s' for e in data.to_dict().keys()])
        query += 'set %s ' % update
        values += list(data.to_dict().values())

        where_query, where_values = self.where_expression.to_query()
        query += 'where %s returning ' % where_query
        values += where_values

        # return all data from updated objects, so caller can determine which rows where changed
        query += ','.join(self._field_aliases())
        return (query, values)

    def create(self, data: Union['stellata.model.Model', list], unique=None):
        # accept both a list and single dictionary as an argument
        one = False
        if not isinstance(data, list):
            data = [data]
            one = True

        if len(data) == 0:
            return

        # run insert query and get result, which will have any defaults added as well
        query, values = self._insert_query(data, unique, one)
        result = [self._row_to_object(self.model, row) for row in self._pool().query(query, values)]

        if one and len(result) > 0:
            return result[0]
        return result

    def delete(self):
        query, values = self._delete_query()
        self._pool().execute(query, values)

    def get(self, one=False):
        # if we don't have any joins, then just grab rows and we're done
        if not self.joins:
            query, values = self._select_query()
            rows = self._pool().query(query, values)
            result = [self._row_to_object(self.model, row) for row in rows]
            if one and len(result) > 0:
                result = result[0]

            return result

        # build directed graph and reversed directed graph of joins so we can identify leaves and roots
        join_order = []
        adjacency_list = {}
        reversed_adjacency_list = {}
        join_map = {}
        for join in self.joins:
            parent = join.relation.parent().model
            child = join.relation.child().model

            adjacency_list.setdefault(parent, [])
            adjacency_list[parent].append(child)
            reversed_adjacency_list.setdefault(child, [])
            reversed_adjacency_list[child].append(parent)
            join_map.setdefault(child, [])
            join_map[child].append(join)

        # perform a DFS on the reversed directed graph to identify the root node
        root = None
        stack = collections.deque([list(reversed_adjacency_list.keys())[0]])
        explored = set()
        while len(stack) > 0:
            root = stack.pop()
            if root in explored:
                continue

            explored.add(root)
            if root not in reversed_adjacency_list:
                break

            for child in reversed_adjacency_list[root]:
                stack.append(child)

        # do a post-order DFS on the directed graph to order joins from leaf to root
        def _build_join_order(adjacency_list, root, join_order, explored):
            if root in adjacency_list and root not in explored:
                for child in adjacency_list[root]:
                    _build_join_order(adjacency_list, child, join_order, explored)

            if root not in explored:
                join_order.append(root)
            explored.add(root)

        explored = set()
        _build_join_order(adjacency_list, root, join_order, explored)

        if self.join_type == 'join' or (not self.join_type and stellata.model._join_type == 'join'):
            return self._get_with_joins(one, join_order, join_map)

        return self._get_with_queries(one, join_order, join_map)

    def get_one(self):
        return self.get(one=True)

    def join(self, relation: 'stellata.relation.Relation'):
        self.joins.append(JoinExpression(relation))
        return self

    def join_with(self, join_type):
        self.join_type = join_type
        return self

    def on(self, database: 'stellata.database.Pool'):
        self.database = database
        return self

    def order(self, fields: list, order=None):
        self.order_expression = OrderByExpression(fields, order)
        return self

    def update(self, data: 'stellata.model.Model'):
        query, values = self._update_query(data)
        rows = self._pool().query(query, values)
        return [self._row_to_object(self.model, row) for row in rows]

    def where(self, expression: 'Expression'):
        self.where_expression = expression
        return self

class Expression:
    """Expression base class.

    Each expression subclass is responsible for serializing itself into a query string.
    """

    def to_query(self):
        """Serialize expression to a SQL query string."""
        raise NotImplementedError()

class JoinExpression(Expression):
    """Expression containing a single join with another table.

    These expressions are chained via .join calls, so have no overloaded operators.
    """

    def __init__(self, relation: 'stellata.relation.Relation'):
        self.relation = relation
        self.alias = ''.join(random.choices(string.ascii_uppercase + string.ascii_lowercase, k=5))

    def to_query(self, alias_map=None):
        alias_map = alias_map or {}
        parent = self.relation.parent()
        child = self.relation.child()

        parent_table = alias_map.get(parent.model.__table__, parent.model.__table__)
        child_table = alias_map.get(child.model.__table__, child.model.__table__)

        return 'left join "%s" as "%s" on "%s"."%s" = "%s"."%s"' % (
            child.model.__table__,
            self.alias,
            parent_table,
            parent.column,
            self.alias,
            child.column
        )

class OrderByExpression(Expression):
    """Expression containing a list of ORDER BY clauses."""

    def __init__(self, fields: Union[list, 'stellata.field.Field'], order: str):
        if not isinstance(fields, list):
            fields = [fields]

        self.fields = fields
        self.order = order or 'asc'

    def to_query(self, alias_map=None):
        alias_map = alias_map or {}
        return 'order by %s %s' % (','.join([
            '"%s"."%s"' % (alias_map.get(field.model.__table__, field.model.__table__), field.column)
            for field in self.fields
        ]), self.order)

class SingleColumnExpression(Expression):
    """Expression containing a single column and value.

    Example SingleColumnExpressions are `id = 5` or `count > 3`.
    OR-ing or AND-ing two SingleColumnExpressions produces a MultiColumnExpression.
    """

    def __init__(self, model: 'stellata.model.Model', column: str, comparison: str, value: Union[int, str, bool]):
        if value is None:
            comparison = 'is' if comparison == '=' else 'is not'

        self.model = model
        self.column = column
        self.comparison = comparison
        self.value = value

    def __or__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'or')

    def __and__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'and')

    def to_query(self, alias_map=None):
        alias_map = alias_map or {}
        table = alias_map.get(self.model.__table__, self.model.__table__)
        if self.comparison == 'in':
            return (
                '"%s"."%s" %s (%s)' % (
                    table,
                    self.column,
                    self.comparison,
                    ','.join(['%s' for e in self.value])
                ),
                self.value
            )

        if self.value == None:
            return ('"%s"."%s" %s null' % (table, self.column, self.comparison), [])

        return ('"%s"."%s" %s %%s' % (table, self.column, self.comparison), [self.value])

class MultiColumnExpression(Expression):
    """Expression containing a two SingleColumnExpressions.

    Example SingleColumnExpressions are `id = 5 OR count > 3`.
    OR-ing or AND-ing two MultiColumnExpressions produces a MultiColumnExpression.
    """

    def __init__(self, model: 'stellata.model.Model', left: Expression, right: Expression, operator: str):
        self.model = model
        self.left = left
        self.right = right
        self.operator = operator

    def __or__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'or')

    def __and__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'and')

    def to_query(self, alias_map=None):
        alias_map = alias_map or {}
        left_query, left_values = self.left.to_query(alias_map)
        right_query, right_values = self.right.to_query(alias_map)
        return (' (%s %s %s) ' % (left_query, self.operator, right_query), left_values + right_values)
