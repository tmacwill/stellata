import stellata.database
import stellata.fields
import stellata.index
import stellata.schema
import stellata.tests.base

db = stellata.tests.base.db

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID(null=False)
    foo = stellata.fields.Text()

    foo_index = stellata.index.Index(lambda: A.foo)
    primary_key = stellata.index.PrimaryKey(lambda: A.id)

class B(stellata.model.Model):
    __table__ = 'b'

    id = stellata.fields.UUID(null=False)
    bar = stellata.fields.Integer()
    baz = stellata.fields.Varchar(length=255)

    bar_baz_index = stellata.index.Index(lambda: (B.bar, B.baz), unique=True)

class Base(stellata.tests.base.Base):
    down = '''
    drop table if exists a;
    drop table if exists b;
    drop index if exists a__foo_index;
    drop index if exists b__bar_baz_index;
    '''

    def test(self):
        result = stellata.schema.migrate(db, models=[A, B], quiet=True)
        return result

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
            'alter table "b" alter column "baz" drop default ;',
            'alter table a add primary key (id) ;',
            'create index "a__foo_index" on "a" using btree (foo) ;',
            'create unique index "b__bar_baz_index" on "b" using btree (bar, baz) ;'
        ])

class TestExtraColumns(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text,
        foobar integer
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255),
        barfoo text
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table "a" drop column "foobar" ;',
            'alter table "b" drop column "barfoo" ;'
        ])

class TestExtraIndex(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    create index if not exists "b__id_bar_baz_index" on "b" using btree (id, bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, ['drop index "b__id_bar_baz_index" ;'])

class TestExtraPrimaryKey(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null primary key,
        bar integer,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table b drop constraint if exists b_pkey ;',
        ])

class TestMissingColumns(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key
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
            'alter table "b" alter column "baz" drop default ;',
            'create index "a__foo_index" on "a" using btree (foo) ;',
            'create unique index "b__bar_baz_index" on "b" using btree (bar, baz) ;'
        ])

class TestMissingIndex(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
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
        self.assertEqual(result, [
            'create index "a__foo_index" on "a" using btree (foo) ;',
            'create unique index "b__bar_baz_index" on "b" using btree (bar, baz) ;'
        ])

class TestMissingPrimaryKey(Base):
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

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'alter table a add primary key (id) ;'
        ])

class TestMissingTable(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
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
            'alter table "b" alter column "baz" drop default ;',
            'create unique index "b__bar_baz_index" on "b" using btree (bar, baz) ;'
        ])

class TestModifyColumnDefault(Base):
    up = '''
    create table if not exists a (
        id uuid not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid not null,
        bar integer,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
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
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (5)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
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
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text not null
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc(),
        bar integer not null,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
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
        id uuid default uuid_generate_v1mc() not null primary key,
        foo integer
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar text,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
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

class TestModifyIndex(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (id, foo) ;
    create index if not exists "b__bar_baz_index" on "b" using btree (id, bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [
            'drop index "a__foo_index" ;',
            'create index "a__foo_index" on "a" using btree (foo) ;',
            'drop index "b__bar_baz_index" ;',
            'create unique index "b__bar_baz_index" on "b" using btree (bar, baz) ;'
        ])

class TestNoop(Base):
    up = '''
    create table if not exists a (
        id uuid default uuid_generate_v1mc() not null primary key,
        foo text
    );

    create table if not exists b (
        id uuid default uuid_generate_v1mc() not null,
        bar integer,
        baz character varying (255)
    );

    create index if not exists "a__foo_index" on "a" using btree (foo) ;
    create unique index if not exists "b__bar_baz_index" on "b" using btree (bar, baz) ;
    '''

    def test(self):
        result = super().test()
        self.assertEqual(result, [])
