import stellata.database
import stellata.fields
import stellata.index
import stellata.model
import stellata.relations
import stellata.tests.base

db = stellata.tests.base.db

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID()
    foo = stellata.fields.Text()

    # note: some attribute names intentionally mismatch tables to test that
    b_has_many = stellata.relations.HasMany(lambda: B.a_id, lambda: A)

    index_id = stellata.index.Index(lambda: A.id, unique=True)

class B(stellata.model.Model):
    __table__ = 'b'

    id = stellata.fields.UUID()
    a_id = stellata.fields.UUID()

    a_belongs_to = stellata.relations.BelongsTo(lambda: B.a_id, lambda: A)
    c_has_many = stellata.relations.HasMany(lambda: C.b_id)

class C(stellata.model.Model):
    __table__ = 'c'

    id = stellata.fields.UUID()
    b_id = stellata.fields.UUID()

    b = stellata.relations.BelongsTo(lambda: C.b_id, lambda: B)
    d = stellata.relations.HasOne(lambda: D.c_id)

class D(stellata.model.Model):
    __table__ = 'd'

    id = stellata.fields.UUID()
    b_id = stellata.fields.UUID()
    c_id = stellata.fields.UUID()

    b_belongs_to = stellata.relations.BelongsTo(lambda: D.b_id, lambda: B)

class E(stellata.model.Model):
    __table__ = 'e'

    id = stellata.fields.UUID()
    d1_id = stellata.fields.UUID()
    d2_id = stellata.fields.UUID()

    d1 = stellata.relations.BelongsTo(lambda: E.d1_id, lambda: D)
    d2 = stellata.relations.BelongsTo(lambda: E.d2_id, lambda: D)

class TestCreateQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_conflict(self, query):
        A.create(A(id=1, foo='foo'), unique=(A.id, A.foo))
        query.assert_called_with(
            'insert into "a" (foo,id) values (%s,%s) on conflict (id,foo) do update set '
            'id = excluded.id, foo = excluded.foo returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            ['foo', 1]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.create([A(id=1, foo='foo'), A(id=2, foo='bar')])
        query.assert_called_with(
            'insert into "a" (foo,id) values (%s,%s),(%s,%s) returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            ['foo', 1, 'bar', 2]
        )

    @stellata.tests.base.mock_query()
    def test_single(self, query):
        A.create(A(id=5, foo='foo'))
        query.assert_called_with(
            'insert into "a" (foo,id) values (%s,%s) returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            ['foo', 5]
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
        A.join_with('join').join(A.b_has_many).where(A.id == 1).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo","VAqPT"."id" as '
            '"VAqPT.id","VAqPT"."a_id" as "VAqPT.a_id" from "a" left join "b" as '
            '"VAqPT" on "a"."id" = "VAqPT"."a_id" where "a"."id" = %s',
            [1]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.join_with('join').join(A.b_has_many).join(B.c_has_many).where(A.id == 1).get()
        query.assert_called_with(
            'select "a"."id" as "a.id","a"."foo" as "a.foo","VAqPT"."id" as '
            '"VAqPT.id","VAqPT"."a_id" as "VAqPT.a_id","KdIGW"."id" as '
            '"KdIGW.id","KdIGW"."b_id" as "KdIGW.b_id" from "a" left join "b" as '
            '"VAqPT" on "a"."id" = "VAqPT"."a_id" left join "c" as "KdIGW" on '
            '"VAqPT"."id" = "KdIGW"."b_id" where "a"."id" = %s',
            [1]
        )

class TestUpdateQuery(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_single(self, query):
        A.where(A.id == 1).update(A(id=2))
        query.assert_called_with(
            'update "a" set id = %s where "a"."id" = %s returning "a"."id" as "a.id","a"."foo" as "a.foo"',
            [2, 1]
        )

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        A.where(A.id == 1).update(A(id=2, foo='foo'))
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
        b_id uuid not null,
        c_id uuid not null
    );

    create table if not exists e (
        id uuid not null default uuid_generate_v1mc(),
        d1_id uuid not null,
        d2_id uuid not null
    );
    '''

    down = '''
    drop table if exists a;
    drop table if exists b;
    drop table if exists c;
    drop table if exists d;
    drop table if exists e;
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
                (id, b_id, c_id)
            values
                ('5e0954fc-2f2f-4a63-9665-3fdf033f5ef5', '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a',
                    '8fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56'),
                ('0dd535d3-2c0e-4dfc-aa3a-9f57c1c2a4c6', '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a',
                    'cf28adf1-bdaa-4a37-9175-a0676ff9d6c5'),
                ('e3066a4a-744e-4164-b29b-de1125fa8db9', 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3',
                    'cf28adf1-bdaa-4a37-9175-a0676ff9d6c5')
            ;

            insert into e
                (id, d1_id, d2_id)
            values
                ('9eb774ca-62ac-4455-b489-2b0b585f5dd5', '5e0954fc-2f2f-4a63-9665-3fdf033f5ef5',
                    '0dd535d3-2c0e-4dfc-aa3a-9f57c1c2a4c6')
            ;

            create unique index "a__index_id" on "a" using btree (id);
        ''')

class TestCreate(DatabaseTest):
    def test_conflict(self):
        result = db.query('''select * from a where foo = 'foobar' ''')
        self.assertEqual(len(result), 0)

        A.create(A(id='2a12f545-c587-4b99-8fd2-57e79f7c8bca', foo='foobar'), unique=A.id)
        result = db.query('''select * from a where foo = 'foobar' ''')
        self.assertEqual(len(result), 1)

    def test_multi(self):
        db.execute('''truncate a''')

        A.create([A(foo='foo'), A(foo='bar')])
        result = db.query('''select * from a''')
        self.assertEqual(len(result), 2)

    def test_single(self):
        db.execute('''truncate a''')
        A.create(A(foo='foo'))

        result = db.query('''select * from a''')
        self.assertEqual(len(result), 1)

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

    def test_limit(self):
        result = A.order(A.id).where(A.foo << ['bar', 'baz']).get()
        self.assertEqual(len(result), 2)
        result = A.order(A.id).limit(1).where(A.foo << ['bar', 'baz']).get()
        self.assertEqual(len(result), 1)

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
        result = A.where(A.foo == 'bar').update(A(foo='qux'))
        self.assertEqual(result[0].id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[0].foo, 'qux')

        result = db.query('''select * from a where foo = 'bar' ''')
        self.assertEqual(len(result), 0)

        result = db.query('''select * from a where foo = 'qux' ''')
        self.assertEqual(len(result), 1)

class BaseTestJoin(DatabaseTest):
    def test_belongs_to_a(self):
        result = B.order(B.id).join(B.a_belongs_to).get()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[1].a_belongs_to.id, '2a12f545-c587-4b99-8fd2-57e79f7c8bca')
        self.assertEqual(result[2].a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')

    def test_belongs_to_b(self):
        result = C.order(C.id).join(C.b).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')

    def test_belongs_to_chain(self):
        result = C.join(C.b).join(B.a_belongs_to).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[0].b.a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[1].b.a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')

    def test_belongs_to_multi(self):
        result = E.join(E.d1).join(E.d2).get()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].d1.id, '5e0954fc-2f2f-4a63-9665-3fdf033f5ef5')
        self.assertEqual(result[0].d2.id, '0dd535d3-2c0e-4dfc-aa3a-9f57c1c2a4c6')

    def test_has_many(self):
        result = A.order(A.id).join(A.b_has_many).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].b_has_many), 1)
        self.assertEqual(len(result[1].b_has_many), 2)
        self.assertEqual(result[0].b_has_many[0].id, 'b0825a6c-36bf-4415-abd7-0d0e5ee3e1c9')
        self.assertEqual(result[1].b_has_many[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b_has_many[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')

    def test_has_many_chain(self):
        result = A.order(A.id).join(A.b_has_many).join(B.c_has_many).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].b_has_many), 1)
        self.assertEqual(len(result[1].b_has_many), 2)
        self.assertEqual(len(result[0].b_has_many[0].c_has_many), 0)
        self.assertEqual(len(result[1].b_has_many[0].c_has_many), 2)
        self.assertEqual(len(result[1].b_has_many[1].c_has_many), 0)
        self.assertEqual(result[1].b_has_many[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b_has_many[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')
        self.assertEqual(result[1].b_has_many[0].c_has_many[0].id, '8fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56')
        self.assertEqual(result[1].b_has_many[0].c_has_many[1].id, '9fd3bec1-e8b9-4e8c-a7e8-3d47152d4e56')
        self.assertEqual(result[0].b_has_many[0].id, 'b0825a6c-36bf-4415-abd7-0d0e5ee3e1c9')

    def test_has_one(self):
        result = C.order(C.id).join(C.d).get()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].d.id, '5e0954fc-2f2f-4a63-9665-3fdf033f5ef5')
        self.assertEqual(result[1].d, None)

    def test_multi_from_different_models(self):
        result = D.order(D.id).join(D.b_belongs_to).join(B.c_has_many).get()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].id, '0dd535d3-2c0e-4dfc-aa3a-9f57c1c2a4c6')
        self.assertEqual(result[1].id, '5e0954fc-2f2f-4a63-9665-3fdf033f5ef5')
        self.assertEqual(result[2].id, 'e3066a4a-744e-4164-b29b-de1125fa8db9')
        self.assertEqual(result[0].b_belongs_to.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[1].b_belongs_to.id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[2].b_belongs_to.id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')
        self.assertEqual(len(result[0].b_belongs_to.c_has_many), 2)
        self.assertEqual(len(result[1].b_belongs_to.c_has_many), 2)
        self.assertEqual(len(result[2].b_belongs_to.c_has_many), 0)

    def test_multi_from_same_model(self):
        result = B.order(B.id).join(B.a_belongs_to).join(B.c_has_many).get()
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(result[1].a_belongs_to.id, '2a12f545-c587-4b99-8fd2-57e79f7c8bca')
        self.assertEqual(result[2].a_belongs_to.id, '31be0c81-f5ee-49b9-a624-356402427f76')
        self.assertEqual(len(result[0].c_has_many), 2)
        self.assertEqual(len(result[1].c_has_many), 0)
        self.assertEqual(len(result[2].c_has_many), 0)

    def test_where(self):
        result = A.join(A.b_has_many).where(A.id == '31be0c81-f5ee-49b9-a624-356402427f76').get()
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[0].b_has_many), 2)
        self.assertEqual(result[0].b_has_many[0].id, '3b33518d-a8b5-4a06-ad32-a5bfe0893a4a')
        self.assertEqual(result[0].b_has_many[1].id, 'f6f85647-ad4c-4fd7-9d87-09c1e4f7a9d3')

    # def test_where_in_association(self):
    #     result = A.join(A.b_has_many).where(B.a_id == '31be0c81-f5ee-49b9-a624-356402427f76').get()
    #     self.assertEqual(len(result), 1)
    #     self.assertEqual(result[0].id, '31be0c81-f5ee-49b9-a624-356402427f76')

class TestJoinSingleQuery(BaseTestJoin):
    def setUp(self):
        super().setUp()
        stellata.model._join_type = 'join'

class TestJoinSeparateQueries(BaseTestJoin):
    def setUp(self):
        super().setUp()
        stellata.model._join_type = 'in'
