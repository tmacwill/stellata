import stellata.field
import stellata.model

class Relation:
    """Base class for an object representing a single relation.

    A relation is defined by two tables, where one of those tables has a foreign key into the other.
    Table args are given within a lambda to avoid issues where classes aren't defined yet.
    """

    def __init__(self, foreign_key, parent=None):
        self.foreign_key_lambda = foreign_key
        self.parent_lambda = parent or (lambda: self.model.id)

    def _parent(self):
        parent = self.parent_lambda()
        if isinstance(parent, stellata.model.ModelType):
            parent = parent.id

        return parent

    def foreign_key(self):
        return self.foreign_key_lambda()

    def child(self):
        raise NotImplementedError()

    def parent(self):
        raise NotImplementedError()
