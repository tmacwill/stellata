import stellata.database
import stellata.model
import stellata.fields
import stellata.tests.base

db = stellata.tests.base.db

class A(stellata.model.Model):
    __table__ = 'a'

    id = stellata.fields.UUID()
    foo = stellata.fields.Text()

class TestSerialize(stellata.tests.base.Base):
    def test_json_multi(self):
        one = A(id=1, foo='bar')
        two = A(id=2, foo='baz')
        self.assertEqual(
            stellata.model.serialize({'a': [one, two]}),
            '''{"a": [{"id": 1, "foo": "bar"}, {"id": 2, "foo": "baz"}]}'''
        )

    def test_json_single(self):
        a = A(id=1, foo='bar')
        self.assertEqual(stellata.model.serialize(a), '''{"id": 1, "foo": "bar"}''')

    def test_msgpack_single(self):
        a = A(id=1, foo='bar')
        self.assertEqual(
            stellata.model.serialize(a, format='msgpack'),
            b'\x82\xa2id\x01\xa3foo\xa3bar'
        )

    def test_msgpack_multi(self):
        one = A(id=1, foo='bar')
        two = A(id=2, foo='baz')
        self.assertEqual(
            stellata.model.serialize({'a': [one, two]}, format='msgpack'),
            b'\x81\xa1a\x92\x82\xa2id\x01\xa3foo\xa3bar\x82\xa2id\x02\xa3foo\xa3baz'
        )
