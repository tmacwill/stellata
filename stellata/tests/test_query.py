import stellata.database
import stellata.fields
import stellata.model
import stellata.relations
import stellata.tests.base

db = stellata.tests.base.db

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID()
    foo = stellata.fields.Text()

    b = stellata.relations.HasMany(lambda: (A.id, B.a_id))

class B(stellata.model.Model):
    __table__ = 'b'

    id = stellata.fields.UUID()
    a_id = stellata.fields.UUID()

    a = stellata.relations.BelongsTo(lambda: (A.id, B.a_id))
    c = stellata.relations.HasMany(lambda: C.b_id)

class C(stellata.model.Model):
    __table__ = 'c'

    id = stellata.fields.UUID()
    b_id = stellata.fields.UUID()

    b = stellata.relations.BelongsTo(lambda: (B.id, C.b_id, C.id))
    d = stellata.relations.HasOne(lambda: (C.id, D.c_id))

class D(stellata.model.Model):
    __table__ = 'd'

    id = stellata.fields.UUID()
    c_id = stellata.fields.UUID()

class TestCreateQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_single(self, query):
        A.create({A.id.column: 5, A.foo.column: 'foo'})
        query.assert_called_with(
            'insert into "a" (foo,id) values (%s,%s) returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            ['foo', 5]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.create([{
            A.id.column: 1,
            A.foo.column: 'foo'
        }, {
            A.id.column: 2,
            A.foo.column: 'bar'
        }])

        query.assert_called_with(
            'insert into "a" (foo,id) values (%s,%s),(%s,%s) returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            ['foo', 1, 'bar', 2]
        )

class TestDeleteQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_execute()
    def test_where(self, execute):
        A.where(A.id == 1).delete()
        execute.assert_called_with('delete from "a" where "a"."id" = %s', [1])

class TestGetQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_where(self, query):
        A.where(A.id == 1).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo" from "a" where "a"."id" = %s',
            [1]
        )

    @stellata.tests.base.mock_query()
    def test_or(self, query):
        A.where((A.id == 1) | (A.id == 2)).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo" from "a" where  ("a"."id" = %s or "a"."id" = %s) ',
            [1, 2]
        )

    @stellata.tests.base.mock_query()
    def test_and(self, query):
        A.where((A.id == 1) & (A.id == 2)).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo" from "a" where  ("a"."id" = %s and "a"."id" = %s) ',
            [1, 2]
        )

    @stellata.tests.base.mock_query()
    def test_parens(self, query):
        A.where((A.id == 1) | ((A.id == 2) & (A.id == 3))).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo" from "a" where  '
            '("a"."id" = %s or  ("a"."id" = %s and "a"."id" = %s) ) ',
            [1, 2, 3]
        )

    @stellata.tests.base.mock_query()
    def test_in(self, query):
        A.where(A.id << [1, 2, 3]).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo" from "a" where "a"."id" in (%s,%s,%s)',
            [1, 2, 3]
        )

class TestJoinQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_single(self, query):
        A.join(A.b).where(A.id == 1).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo","b"."id" as "b.id","b"."a_id" as "b.a_id" '
            'from "a" left join "b" on "a"."id" = "b"."a_id" where "a"."id" = %s',
            [1]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.join(A.b).join(B.c).where(A.id == 1).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo","b"."id" as "b.id","b"."a_id" as "b.a_id",'
            '"c"."id" as "c.id","c"."b_id" as "c.b_id" from "a" left join "b" on "a"."id" = "b"."a_id" '
            'left join "c" on "b"."id" = "c"."b_id" where "a"."id" = %s',
            [1]
        )

class TestUpdateQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_single(self, query):
        A.where(A.id == 1).update({A.id.column: 2})
        query.assert_called_with(
            'update "a" set id = %s where "a"."id" = %s returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            [2, 1]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.where(A.id == 1).update({A.id.column: 2, A.foo.column: 'foo'})
        query.assert_called_with(
            'update "a" set id = %s,foo = %s where "a"."id" = %s returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            [2, 'foo', 1]
        )

class DatabaseTest(stellata.tests.base.Base):
    up = '''
    create table if not exists a (
        id uuid not null default uuid_generate_v1mc(),
        foo text not null
    );

    create table if not exists b (
        id uuid not null default uuid_generate_v1mc(),
        a_id uuid not null
    );

    create table if not exists c (
        id uuid not null default uuid_generate_v1mc(),
        b_id uuid not null
    );

    create table if not exists d (
        id uuid not null default uuid_generate_v1mc(),
        c_id uuid not null
    );
    '''

    down = '''
    drop table a;
    drop table b;
    drop table c;
    drop table d;
    '''

    def setUp(self):
        super().setUp()
        db.execute('''
            insert into a
                (id, foo)
            values
                ('2a12f545-c587-4b99-8fd2-57e79f7c8bca', 'baz'),
                ('31be0c81-f5ee-49b9-a624-356402427f76', 'bar')
            ;

            insert into b
                (id, a_id)
            values
                ('3b33518d-a8b5-4a06-ad32-a5bfe0893a4a', '31be0c81-f5ee-49b9-a624-356402427f76'),
                ('f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3', '31be0c81-f5ee-49b9-a624-356402427f76'),
                ('b0825a6c-36bf-4415-abd7-0d0e5ee3e1c9', '2a12f545-c587-4b99-8fd2-57e79f7c8bca')
            ;

            insert into c
                (id, b_id)
            values
                ('8fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56', '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a'),
                ('9fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56', '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
            ;

            insert into d
                (id, c_id)
            values
                ('5e0954fc-2f2f-4a63-9665-3fdf033f5ef5', '8fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56')
            ;
        ''')

class TestDelete(DatabaseTest):
    def test_where(self):
        A.where(A.foo == 'bar').delete()
        result = db.query('''select * from a where foo = 'bar' ''')
        self.assertEqual(len(result), 0)

class TestGet(DatabaseTest):
    def test_in(self):
        result = A.order(A.id).where(A.foo << ['bar', 'baz']).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].foo, 'baz')
        self.assertEqual(result[1].foo, 'bar')

    def test_or(self):
        result = A.order(A.id).where((A.foo == 'bar') | (A.foo == 'baz')).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].foo, 'baz')
        self.assertEqual(result[1].foo, 'bar')

    def test_order(self):
        result = A.order(A.foo, 'asc').get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].foo, 'bar')
        self.assertEqual(result[1].foo, 'baz')

        result = A.order(A.foo, 'desc').get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].foo, 'baz')
        self.assertEqual(result[1].foo, 'bar')

    def test_where(self):
        result = A.where(A.foo == 'bar').get()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].foo, 'bar')

        result = A.where(A.foo == 'no').get()
        self.assertEqual(len(result), 0)

class TestUpdate(DatabaseTest):
    def test_where(self):
        result = A.where(A.foo == 'bar').update({'foo': 'qux'})
        self.assertEqual(result[0].id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[0].foo, 'qux')

        result = db.query('''select * from a where foo = 'bar' ''')
        self.assertEqual(len(result), 0)

        result = db.query('''select * from a where foo = 'qux' ''')
        self.assertEqual(len(result), 1)

class TestJoin(DatabaseTest):
    def test_belongs_to_a(self):
        result = B.order(B.id).join(B.a).get()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].a.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[1].a.id, '2a12f545-c587-4b99-8fd2-57e79f7c8bca')
        self.assertEqual(result[2].a.id, '31be0c81-f5ee-49b9-a624-356402427f76')

    def test_belongs_to_b(self):
        result = C.order(C.id).join(C.b).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')

    def test_belongs_to_chain(self):
        result = C.join(C.b).join(B.a).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[0].b.a.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[1].b.a.id, '31be0c81-f5ee-49b9-a624-356402427f76')

    def test_has_many(self):
        result = A.order(A.id).join(A.b).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].b), 1)
        self.assertEqual(len(result[1].b), 2)
        self.assertEqual(result[0].b[0].id, 'b0825a6c-36bf-4415-abd7-0d0e5ee3e1c9')
        self.assertEqual(result[1].b[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')

    def test_has_many_chain(self):
        result = A.order(A.id).join(A.b).join(B.c).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].b), 1)
        self.assertEqual(len(result[1].b), 2)
        self.assertEqual(len(result[0].b[0].c), 0)
        self.assertEqual(len(result[1].b[0].c), 2)
        self.assertEqual(len(result[1].b[1].c), 0)
        self.assertEqual(result[1].b[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')
        self.assertEqual(result[1].b[0].c[0].id, '8fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56')
        self.assertEqual(result[1].b[0].c[1].id, '9fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56')
        self.assertEqual(result[0].b[0].id, 'b0825a6c-36bf-4415-abd7-0d0e5ee3e1c9')

    def test_has_one(self):
        result = C.order(C.id).join(C.d).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].d.id, '5e0954fc-2f2f-4a63-9665-3fdf033f5ef5')
        self.assertEqual(result[1].d, None)

    def test_where(self):
        result = A.join(A.b).where(A.id == '31be0c81-f5ee-49b9-a624-356402427f76').get()
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0].b), 2)
        self.assertEqual(result[0].b[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[0].b[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')
