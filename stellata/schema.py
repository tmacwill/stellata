import re
import stellata.database
import stellata.model

def _alter_table_string(field, primary_key=False, create=False):
    result = []

    alter = 'alter table "%s"' % field.model.__table__
    if create:
        prefix = '%s add column "%s"' % (alter, field.column)
        if field.length:
            result.append('%s %s (%s) ;' % (prefix, field.column_type, field.length))
        else:
            result.append('%s %s ;' % (prefix, field.column_type))
    else:
        prefix = '%s alter column "%s" type %s' % (alter, field.column, field.column_type)
        if field.length:
            result.append('%s (%s) ;' % (prefix, field.length))
        else:
            result.append('%s ;' % prefix)

    alter += ' alter column "%s"' % field.column
    if field.null:
        result.append('%s drop not null ;' % alter)
    else:
        result.append('%s set not null ;' % alter)

    if field.default:
        result.append('%s set default %s ;' % (alter, field.default))
    else:
        result.append('%s drop default ;' % alter)

    return result

def _handle(database, statements, execute):
    if not statements:
        return

    if not isinstance(statements, list):
        statements = [statements]

    if execute:
        for statement in statements:
            database.execute(statement)

    return statements

def _index_name(index):
    return '%s__%s' % (index.model.__table__, index.column)

def _index_string(index):
    sql = 'create '
    if index.unique:
        sql += 'unique '

    sql += 'index "%s" on "%s" using btree (%s)' % (
        _index_name(index),
        index.model.__table__,
        ', '.join([e.column for e in index.fields()])
    )

    return sql

def _migrate_indexes(database, models, execute):
    result = []
    for model in models:
        # get the primary key for the table
        sql = '''
            select
                tablename,
                indexname,
                indexdef
            from pg_indexes where
                tablename = '%s' and
                indexname like '%%_pkey'
        ''' % model.__table__
        schema = database.query(sql)

        # handle primary keys first
        primary_keys_match = False
        model_has_primary_key = False
        database_has_primary_key = len(schema) > 0
        index_columns = None
        for index in model.__indexes__:
            if isinstance(index, stellata.index.PrimaryKey):
                model_has_primary_key = True

                # check if columns on primary key match columns defined on index
                index_columns = ','.join([e.column for e in index.fields()])
                if len(schema) > 0:
                    table, index, definition = schema[0]
                    if re.search('\(%s\)' % index_columns, definition):
                        primary_keys_match = True

        if database_has_primary_key and not model_has_primary_key:
            result.append('alter table %s drop constraint if exists %s_pkey ;' % (model.__table__, model.__table__))
        if not database_has_primary_key and model_has_primary_key:
            result.append(
                'alter table %s add primary key (%s) ;' %
                (model.__table__, index_columns)
            )
        elif not primary_keys_match and index_columns:
            result.append('alter table %s drop constraint if exists %s_pkey ;' % (model.__table__, model.__table__))
            result.append(
                'alter table %s add primary key (%s) ;' %
                (model.__table__, index_columns)
            )

        # get indexes that currently exist for table, excluding primary key
        sql = '''
            select
                tablename,
                indexname,
                indexdef
            from pg_indexes where
                tablename = '%s' and
                indexname not like '%%_pkey'
        ''' % model.__table__
        schema = database.query(sql)

        # get all indexes that should exist in database, skipping primary keys
        defined_indexes = []
        defined_index_names = set()
        for index in model.__indexes__:
            if isinstance(index, stellata.index.PrimaryKey):
                continue

            defined_indexes.append(index)
            defined_index_names.add(_index_name(index))

        # make sure all existing indexes match the types defined by models
        existing_indexes = set()
        for existing_index in schema:
            table, index_name, definition = existing_index
            existing_indexes.add(index_name)

            for defined_index in defined_indexes:
                if _index_name(defined_index) == index_name:
                    index_string = _index_string(defined_index)

                    # if definitions don't match, then drop index and re-create
                    if index_string.lower().replace('"', '') != definition.lower().replace('"', ''):
                        result.append('drop index "%s" ;' % index_name)
                        result.append('%s ;' % index_string)

        # drop indexes that are no longer needed
        unused_indexes = existing_indexes - defined_index_names
        for unused_index in unused_indexes:
            result.append('drop index "%s" ;' % unused_index)

        # add indexes that are missing
        missing_indexes = defined_index_names - existing_indexes
        for missing_index in missing_indexes:
            for defined_index in defined_indexes:
                if missing_index == _index_name(defined_index):
                    result.append('%s ;' % _index_string(defined_index))

    return result

def _migrate_tables(database, models, execute):
    result = []
    for model in models:
        if not hasattr(model, '__table__') or not model.__table__:
            continue

        # get columns that currently exist for table
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
        schema = database.query(sql)

        # if table doesn't exist in schema, then it needs to be created
        if len(schema) == 0:
            result.append('create table "%s" () ;' % model.__table__)

        # get all columns that should exist in the database
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
            if field.column == 'dt':
                defined_fields.pop(i)
                defined_fields.insert(1, field)
                break

        # for each column that exists in the database, make sure its metadata matches models
        existing_columns = set()
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
                        result.extend(_alter_table_string(field))

        # drop columns that are no longer needed
        unused_columns = existing_columns - defined_columns
        for unused_column in unused_columns:
            result.append('alter table "%s" drop column "%s" ;' % (model.__table__, unused_column))

        # add columns that are missing, preserving the order determined earlier
        missing_columns = defined_columns - existing_columns
        for field in defined_fields:
            if field.column in missing_columns:
                result.extend(_alter_table_string(field, create=True))

    return result

def drop_tables_and_lose_all_data(database, execute=False):
    sql = '''
        select
            tablename
        from pg_tables
        where schemaname = 'public'
    '''
    tables = database.query(sql)

    statements = ['drop table %s' % table['tablename'] for table in tables]
    _handle(database, statements, execute)
    return statements

def migrate(database=None, models=None, execute=False):
    if not database:
        database = stellata.database.pool

    if not models:
        models = stellata.model._models

    result = []
    result.extend(_migrate_tables(database, models, execute))
    result.extend(_migrate_indexes(database, models, execute))
    _handle(database, result, execute)
    return result
