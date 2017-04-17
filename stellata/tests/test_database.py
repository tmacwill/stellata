import stellata.database
import stellata.fields
import stellata.tests.base

db = stellata.tests.base.db
db2 = stellata.tests.base.db2

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID()
    foo = stellata.fields.Text()

class TestPool(stellata.tests.base.Base):
    up = '''
    create table if not exists a (
        id uuid not null default uuid_generate_v1mc(),
        foo text not null
    );
    '''

    down = '''
    drop table if exists a;
    '''

    def setUp(self):
        db.execute(self.up)
        db2.execute(self.up)

    def tearDown(self):
        db.execute(self.down)
        db2.execute(self.down)

    def test_instance(self):
        db.execute("insert into a (foo) values ('bar')")
        self.assertEqual(len(db.query("select * from a")), 1)
        self.assertEqual(len(db2.query("select * from a")), 0)

    def test_model(self):
        A.create(A(foo='bar'))
        self.assertEqual(len(A.where(A.foo == 'bar').get()), 1)
        self.assertEqual(len(A.on(db).where(A.foo == 'bar').get()), 1)
        self.assertEqual(len(A.where(A.foo == 'bar').on(db).get()), 1)
        self.assertEqual(len(A.on(db2).where(A.foo == 'bar').get()), 0)

