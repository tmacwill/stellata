class Index:
    """Base class for an object representing a database index."""

    def __init__(self, fields_lambda, primary_key=False, unique=False):
        self.fields_lambda = fields_lambda
        self.unique = unique

    def fields(self):
        result = self.fields_lambda()
        if not isinstance(result, tuple):
            result = (result,)

        return result

class PrimaryKey(Index):
    def __init__(self, fields_lambda, primary_key=True, unique=False):
        return super().__init__(fields_lambda, primary_key, unique)
