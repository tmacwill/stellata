import stellata.database
import stellata.model

def _alter_table_string(table, field, primary_key=False, create=False):
    result = []

    if create:
        if field.length:
            result.append(
                'alter table "%s" add column "%s" %s (%s) ;' %
                (table, field.column, field.column_type, field.length)
            )
        else:
            result.append('alter table "%s" add column "%s" %s ;' % (table, field.column, field.column_type))
    else:
        if field.length:
            result.append(
                'alter table "%s" alter column "%s" type %s (%s) ;' %
                (table, field.column, field.column_type, field.length)
            )
        else:
            result.append('alter table "%s" alter column "%s" type %s ;' % (table, field.column, field.column_type))

    if field.null:
        result.append('alter table "%s" alter column "%s" drop not null ;' % (table, field.column))
    else:
        result.append('alter table "%s" alter column "%s" set not null ;' % (table, field.column))

    if field.default:
        result.append('alter table "%s" alter column "%s" set default %s ;' % (table, field.column, field.default))
    else:
        result.append('alter table "%s" alter column "%s" drop default ;' % (table, field.column))

    return result

def _handle(database, statements, execute, quiet):
    if not statements:
        return

    for statement in statements:
        if not quiet:
            print(statement)

        if execute:
            database.execute(statement)

    return statements

def migrate(database=None, models=None, execute=False, quiet=False):
    if not database:
        database = stellata.database.pool

    if not models:
        models = stellata.model._models

    result = []
    for model in models:
        if not hasattr(model, '__table__') or not model.__table__:
            continue

        sql = '''
            select
                column_name,
                data_type,
                character_maximum_length,
                column_default,
                is_nullable
            from information_schema.columns where
                table_name = '%s';
        ''' % model.__table__

        # if table doesn't exist in schema, then it needs to be created
        schema = database.query(sql)
        if len(schema) == 0:
            result.append(_handle(database, 'create table "%s" () ;' % model.__table__, execute, quiet))

        existing_columns = set()
        defined_columns = set()
        defined_fields = []
        for field in model.__fields__:
            defined_fields.append(field)
            defined_columns.add(field.column)

        # for convenience when using the psql CLI, add the ID column first
        for i, field in enumerate(defined_fields):
            if field.column == 'id':
                defined_fields.pop(i)
                defined_fields.insert(0, field)
                break

        # for convenience when using the psql CLI, add the dt column second
        for i, field in enumerate(defined_fields):
            if field.column.endswith('dt'):
                defined_fields.pop(i)
                defined_fields.insert(1, field)
                break

        # make sure column types match
        for existing_column in schema:
            column_name, column_type, length, default, null = existing_column
            existing_columns.add(column_name)

            for field in defined_fields:
                if field.column == column_name:
                    if column_type != field.column_type or \
                            length != field.length or \
                            default != field.default or \
                            (null == 'YES' and not field.null) or \
                            (null == 'NO' and field.null):
                        result += _handle(
                            database,
                            _alter_table_string(model.__table__, field),
                            execute,
                            quiet
                        )

        # drop columns that are no longer needed
        unused_columns = existing_columns - defined_columns
        for unused_column in unused_columns:
            result += _handle(
                database,
                ['alter table "%s" drop column "%s" ;' % (model.__table__, unused_column)],
                execute,
                quiet
            )

        # add columns that are missing, preserving the order determined earlier
        missing_columns = defined_columns - existing_columns
        for field in defined_fields:
            if field.column in missing_columns:
                result += _handle(
                    database,
                    _alter_table_string(model.__table__, field, create=True),
                    execute,
                    quiet
                )

    return result
