import stellata.database
import stellata.fields
import stellata.schema
import stellata.tests.base

db = stellata.tests.base.db

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID(null=False)
    foo = stellata.fields.Text()

class B(stellata.model.Model):
    __table__ = 'b'

    id = stellata.fields.UUID(null=False)
    bar = stellata.fields.Integer()
    baz = stellata.fields.Varchar(length=255)

class Base(stellata.tests.base.Base):
    down = '''
    drop table if exists a;
    drop table if exists b;
    '''

    def test(self):
        return stellata.schema.migrate(db, models=[A, B], quiet=True)

class TestEmpty(Base):
    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'create table "a" () ;',
            'alter table "a" add column "id" uuid ;',
            'alter table "a" alter column "id" set not null ;',
            'alter table "a" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "a" add column "foo" text ;',
            'alter table "a" alter column "foo" drop not null ;',
            'alter table "a" alter column "foo" drop default ;',
            'create table "b" () ;',
            'alter table "b" add column "id" uuid ;',
            'alter table "b" alter column "id" set not null ;',
            'alter table "b" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "b" add column "bar" integer ;',
            'alter table "b" alter column "bar" drop not null ;',
            'alter table "b" alter column "bar" drop default ;',
            'alter table "b" add column "baz" character varying (255) ;',
            'alter table "b" alter column "baz" drop not null ;',
            'alter table "b" alter column "baz" drop default ;'
        ])

class TestExtraColumns(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null,
        foo text,
        foobar integer
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255),
        barfoo text
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" drop column "foobar" ;',
            'alter table "b" drop column "barfoo" ;'
        ])

class TestMissingColumns(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" add column "foo" text ;',
            'alter table "a" alter column "foo" drop not null ;',
            'alter table "a" alter column "foo" drop default ;',
            'alter table "b" add column "bar" integer ;',
            'alter table "b" alter column "bar" drop not null ;',
            'alter table "b" alter column "bar" drop default ;',
            'alter table "b" add column "baz" character varying (255) ;',
            'alter table "b" alter column "baz" drop not null ;',
            'alter table "b" alter column "baz" drop default ;'
        ])

class TestMissingTable(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null,
        foo text
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'create table "b" () ;',
            'alter table "b" add column "id" uuid ;',
            'alter table "b" alter column "id" set not null ;',
            'alter table "b" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "b" add column "bar" integer ;',
            'alter table "b" alter column "bar" drop not null ;',
            'alter table "b" alter column "bar" drop default ;',
            'alter table "b" add column "baz" character varying (255) ;',
            'alter table "b" alter column "baz" drop not null ;',
            'alter table "b" alter column "baz" drop default ;'
        ])

class TestModifyColumnDefault(Base):
    up = '''
    create table if not exists a (
        id uuid not null,
        foo text
    );

    create table if not exists b (
        id uuid not null,
        bar integer,
        baz character varying (255)
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" alter column "id" type uuid ;',
            'alter table "a" alter column "id" set not null ;',
            'alter table "a" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "b" alter column "id" type uuid ;',
            'alter table "b" alter column "id" set not null ;',
            'alter table "b" alter column "id" set default uuid_generate_v1mc() ;'
        ])

class TestModifyColumnLength(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (5)
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "b" alter column "baz" type character varying (255) ;',
            'alter table "b" alter column "baz" drop not null ;',
            'alter table "b" alter column "baz" drop default ;'
        ])

class TestModifyColumnNull(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc(),
        foo text not null
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc(),
        bar integer not null,
        baz character varying (255)
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" alter column "id" type uuid ;',
            'alter table "a" alter column "id" set not null ;',
            'alter table "a" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "a" alter column "foo" type text ;',
            'alter table "a" alter column "foo" drop not null ;',
            'alter table "a" alter column "foo" drop default ;',
            'alter table "b" alter column "id" type uuid ;',
            'alter table "b" alter column "id" set not null ;',
            'alter table "b" alter column "id" set default uuid_generate_v1mc() ;',
            'alter table "b" alter column "bar" type integer ;',
            'alter table "b" alter column "bar" drop not null ;',
            'alter table "b" alter column "bar" drop default ;'
        ])

class TestModifyColumnType(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null,
        foo integer
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar text,
        baz character varying (255)
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" alter column "foo" type text ;',
            'alter table "a" alter column "foo" drop not null ;',
            'alter table "a" alter column "foo" drop default ;',
            'alter table "b" alter column "bar" type integer ;',
            'alter table "b" alter column "bar" drop not null ;',
            'alter table "b" alter column "bar" drop default ;'
        ])

class TestNoop(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255)
    );
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [])
