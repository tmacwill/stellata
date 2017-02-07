import stellata.field
import stellata.model

class Relation:
    """Base class for an object representing a single relation.

    A relation is defined by two tables, where one of those tables has a foreign key into the other.
    Table args are given within a lambda to avoid issues where classes aren't defined yet.
    """

    def __init__(self, model_lambda, column=None, model=None):
        self.model_lambda = model_lambda
        self.column = column
        self.model = model

    def child(self):
        models = self.model_lambda()
        if isinstance(models, stellata.field.Field):
            return models

        return models[1] if models[0].model == self.model else models[0]

    def parent(self):
        models = self.model_lambda()
        if isinstance(models, stellata.field.Field):
            return self.model.id

        return models[0] if models[0].model == self.model else models[1]

    def id_field(self):
        models = self.model_lambda()
        if isinstance(models, stellata.field.Field):
            return self.model.id
        if len(models) > 2:
            return models[2]

        return self.model.id
