import collections
import stellata.database
import stellata.relations
import stellata.model

class Query:
    """Container class for a SQL query.

    A query is essentially a container for various Expression types, which serialize themselves to SQL.
    """

    def __init__(self, model, joins=None, where=None, order=None):
        if joins is None:
            joins = []

        if joins and not isinstance(joins, list):
            joins = [joins]

        self.model = model
        self.joins = joins
        self.where_expression = where
        self.order_expression = order

    def _delete_query(self):
        query = 'delete from "%s" ' % self.model.__table__
        where_query, where_values = self.where_expression.to_query()
        query += 'where %s' % where_query

        return (query, where_values)

    def _row_to_object(self, model, row):
        empty = True
        data = {}

        # iterate over fields defined in the model and extract them from the row dict
        for column in model.__fields__:
            value = row['%s.%s' % (model.__table__, column)]
            data[column] = value
            if value is not None:
                empty = False

        # don't return a dictionary where every column is null
        if empty:
            return None

        return model(**data)

    def _select_query(self):
        # get list of columns to be selected from database
        columns = [
            '"%s"."%s" as "%s.%s"' % (self.model.__table__, column, self.model.__table__, column)
            for column in self.model.__fields__
        ]

        # add each join to the list of columns to select
        for join in self.joins:
            child = join.child()
            columns += [
                '"%s"."%s" as "%s.%s"' % (child.model.__table__, column, child.model.__table__, column)
                for column in child.model.__fields__
            ]

        # base select query
        query = 'select %s from "%s" ' % (','.join(columns), self.model.__table__)

        # add each join clause
        if self.joins:
            query += '%s ' % ' '.join(e.to_query() for e in self.joins)

        # add where clause
        where_values = tuple()
        if self.where_expression:
            where_query, where_values = self.where_expression.to_query()
            query += 'where %s' % where_query

        if self.order_expression:
            query += ' %s' % self.order_expression.to_query()

        return (query, where_values)

    def _update_query(self, set_values):
        values = []
        query = 'update "%s" ' % self.model.__table__

        update = ','.join([e + ' = %s' for e in set_values.keys()])
        query += 'set %s ' % update
        values += list(set_values.values())

        where_query, where_values = self.where_expression.to_query()
        query += 'where %s returning ' % where_query
        values += where_values

        # return all data from updated objects, so caller can determine which rows where changed
        returning = [
            '"%s"."%s" as "%s.%s"' % (self.model.__table__, column, self.model.__table__, column)
            for column in self.model.__fields__
        ]

        query += ','.join(returning)
        return (query, values)

    def delete(self):
        query, values = self._delete_query()
        stellata.database.execute(query, values)

    def get(self):
        result = []
        created = {}
        query, values = self._select_query()
        rows = stellata.database.query(query, values)

        # if we don't have any joins, then we're done
        if not self.joins:
            return [self._row_to_object(self.model, row) for row in rows]

        # build directed graph and reversed directed graph of joins so we can identify leaves and roots
        adjacency_list = {}
        reversed_adjacency_list = {}
        join_map = {}
        for join in self.joins:
            parent = join.parent()
            child = join.child()

            adjacency_list.setdefault(parent.model, [])
            adjacency_list[parent.model].append(child.model)
            reversed_adjacency_list.setdefault(child.model, [])
            reversed_adjacency_list[child.model].append(parent.model)
            join_map.setdefault(child.model, [])
            join_map[child.model].append(join.relation)

        # perform a DFS on the reversed directed graph to identify the root node
        root = None
        stack = collections.deque([list(reversed_adjacency_list.keys())[0]])
        explored = set()
        while len(stack) > 0:
            root = stack.pop()
            explored.add(root)
            if root not in reversed_adjacency_list:
                break

            for child in reversed_adjacency_list[root]:
                stack.append(child)

        # do a post-order DFS on the directed graph to order joins from leaf to root
        join_order = []
        explored = set()
        def _build_join_order(adjacency_list, root, join_order, explored):
            if root in adjacency_list and root not in explored:
                for child in adjacency_list[root]:
                    _build_join_order(adjacency_list, child, join_order, explored)

            explored.add(root)
            join_order.append(root)

        _build_join_order(adjacency_list, root, join_order, explored)

        # iterate over joins in order from leaf nodes to root node
        data = {}
        aliases = {}
        parent_key = None
        for child_model in join_order:
            for relation in join_map.get(child_model, []):
                parent_model = relation.parent().model
                parent_column = relation.parent().column
                child_model = relation.child().model
                child_column = relation.child().column

                # map table names to attribute names so both parent/child models can use the same key
                many = isinstance(relation, stellata.relations.HasMany)
                aliases[child_model.__table__] = relation.column
                visited = set()
                for row in rows:
                    # for each join, insert into a table that maps models to their descendents.
                    # the first key is a model name, which maps to each returned model value.
                    # so, for `A1 -> [B1, B2], A2 -> [B3], B1 -> [C1], B2 -> [C2]` we have:
                    # {'b.id': {B1: [C1], B2: [C2], B3: []}, 'a.id': [{A1: [B1.C1, B2.C2], A2: [B3]}]}
                    # the general approach here is that we build up this table backwards, ensuring that all
                    # referenced values are inserted first. then, for each model, we can replace any
                    # fields that are actually references to other models with data already stored in the table.
                    parent_key = '%s.%s' % (relation.id_field().model.__table__, relation.id_field().column)
                    parent_value = row[parent_key]

                    data.setdefault(parent_key, {})
                    data[parent_key].setdefault(parent_value, {})

                    # convert row to model objects, since that's what we'll ultimately return
                    parent_row = self._row_to_object(parent_model, row)
                    child_row = self._row_to_object(child_model, row)
                    alias = aliases[child_model.__table__]

                    # get values stored for this row and alias
                    v = [] if many else None
                    if hasattr(data[parent_key][parent_value], alias):
                        v = getattr(data[parent_key][parent_value], alias)

                    # since joins are left joins, skip rows that are empty for the current join
                    found = False
                    if child_row:
                        # for each column in the row, check if it's a reference to another model
                        for row_key, row_value in child_row.__dict__.items():
                            key = '%s.%s' % (child_model.__table__, row_key)
                            if key in data:
                                found = True
                                # grab the referenced object from the table and add it to this object
                                if row_value not in visited:
                                    if many:
                                        v.append(data.get(key, {}).get(row_value, []))
                                    else:
                                        v = data.get(key, {}).get(row_value, [])

                                # de-duplicate rows, since parent data will be returned multiple times
                                if not isinstance(relation, stellata.relations.BelongsTo):
                                    visited.add(row_value)

                        # for leaf node joins, just insert the child row since there are no references yet
                        if not found:
                            if many:
                                v.append(child_row)
                            else:
                                v = child_row

                    # insert data into table
                    setattr(parent_row, alias, v)
                    data[parent_key][parent_value] = parent_row

        # if no rows are returned, then return an empty list
        if not parent_key:
            return []

        # final result is stored at the key representing the root model
        return list(data[parent_key].values())

    def join(self, relation):
        self.joins.append(JoinExpression(relation))
        return self

    def order(self, fields, order=None):
        self.order_expression = OrderByExpression(fields, order)
        return self

    def update(self, set_values):
        query, values = self._update_query(set_values)
        rows = stellata.database.query(query, values)
        return [self._row_to_object(self.model, row) for row in rows]

    def where(self, expression):
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

    def __init__(self, relation):
        self.relation = relation

    def child(self):
        return self.relation.child()

    def parent(self):
        return self.relation.parent()

    def to_query(self):
        parent = self.parent()
        child = self.child()

        return 'left join "%s" on "%s"."%s" = "%s"."%s"' % (
            child.model.__table__,
            parent.model.__table__,
            parent.column,
            child.model.__table__,
            child.column
        )

class OrderByExpression(Expression):
    """Expression containing a list of ORDER BY clauses."""

    def __init__(self, fields, order):
        if not isinstance(fields, list):
            fields = [fields]

        self.fields = fields
        self.order = order or 'asc'

    def to_query(self):
        return 'order by %s %s' % (','.join([
            '"%s"."%s"' % (field.model.__table__, field.column)
            for field in self.fields
        ]), self.order)

class SingleColumnExpression(Expression):
    """Expression containing a single column and value.

    Example SingleColumnExpressions are `id = 5` or `count > 3`.
    OR-ing or AND-ing two SingleColumnExpressions produces a MultiColumnExpression.
    """

    def __init__(self, model, column: str, comparison: str, value):
        self.model = model
        self.column = column
        self.comparison = comparison
        self.value = value

    def __or__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'or')

    def __and__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'and')

    def to_query(self):
        if self.comparison == 'in':
            return (
                '"%s"."%s" %s (%s)' % (
                    self.model.__table__,
                    self.column,
                    self.comparison,
                    ','.join(['%s' for e in self.value])
                ),
                self.value
            )

        return ('"%s"."%s" %s %%s' % (self.model.__table__, self.column, self.comparison), [self.value])

class MultiColumnExpression(Expression):
    """Expression containing a two SingleColumnExpressions.

    Example SingleColumnExpressions are `id = 5 OR count > 3`.
    OR-ing or AND-ing two MultiColumnExpressions produces a MultiColumnExpression.
    """

    def __init__(self, model, left: Expression, right: Expression, operator: str):
        self.model = model
        self.left = left
        self.right = right
        self.operator = operator

    def __or__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'or')

    def __and__(self, value: Expression):
        return MultiColumnExpression(self.model, self, value, 'and')

    def to_query(self):
        left_query, left_values = self.left.to_query()
        right_query, right_values = self.right.to_query()
        return (' (%s %s %s) ' % (left_query, self.operator, right_query), left_values + right_values)