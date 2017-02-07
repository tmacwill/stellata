from typing import Union

import stellata.query

class Field:
    """Base class for an object representing a single field on a model.

    Fields correspond to database columns.
    """

    def __init__(self, column=None, model=None):
        self.column = column
        self.model = model

    def __eq__(self, value: Union[int, str, float]):
        return stellata.query.SingleColumnExpression(self.model, self.column, '=', value)

    def __lt__(self, value: Union[int, str, float]):
        return stellata.query.SingleColumnExpression(self.model, self.column, '<', value)

    def __gt__(self, value: Union[int, str, float]):
        return stellata.query.SingleColumnExpression(self.model, self.column, '>', value)

    def __lshift__(self, value: list):
        return stellata.query.SingleColumnExpression(self.model, self.column, 'in', value)

    # in case people forget which way the arrows go, lol
    def __rshift__(self, value: list):
        return self.__lshift__(value)
