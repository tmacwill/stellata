import stellata.field

class BigInteger(stellata.field.Field):
    """BIGINT column type."""

    column_type = 'bigint'

class Boolean(stellata.field.Field):
    """BOOLEAN column type."""

    column_type = 'boolean'

class Integer(stellata.field.Field):
    """INTEGER column type."""

    column_type = 'integer'

class Numeric(stellata.field.Field):
    """Numeric column type."""

    column_type = 'numeric'

class Text(stellata.field.Field):
    """TEXT column type."""

    column_type = 'text'

class Timestamp(stellata.field.Field):
    """TIMESTAMP column type."""

    column_type = 'timestamp without time zone'

    def __init__(self, length=None, null=True, default=''):
        if default == '':
            default = 'now()'

        super().__init__(length, null, default)

class UUID(stellata.field.Field):
    """UUID column type."""

    column_type = 'uuid'

    def __init__(self, length=None, null=True, default=''):
        if default == '':
            default = 'uuid_generate_v1mc()'

        super().__init__(length, null, default)

class Varchar(stellata.field.Field):
    """CHARACTER VARYING column type."""

    column_type = 'character varying'
