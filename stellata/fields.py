import stellata.field

class BigInteger(stellata.field.Field):
    """BIGINT column type."""

    column_type = 'bigint'

class Boolean(stellata.field.Field):
    """TINYINT column type."""

    column_type = 'tinyint'

class DateTime(stellata.field.Field):
    """DATETIME column type."""

    column_type = 'datetime'

class Integer(stellata.field.Field):
    """INTEGER column type."""

    column_type = 'integer'

class Text(stellata.field.Field):
    """TEXT column type."""

    column_type = 'text'

class UUID(stellata.field.Field):
    """UUID column type."""

    column_type = 'uuid'

    def __init__(self, length=None, null=True, default=None):
        if default is None:
            default = 'uuid_generate_v1mc()'

        super().__init__(length, null, default)

class Varchar(stellata.field.Field):
    """CHARACTER VARYING column type."""

    column_type = 'character varying'
