import stellata.database
import stellata.model
import stellata.fields
import stellata.tests.base

import unittest
import unittest.mock

class M(stellata.model.Model):
    __table__ = 't'
    id = stellata.fields.UUID()
    foo = stellata.fields.Text()

class TestCreate(stellata.tests.base.Base):
    @stellata.tests.base.mock_query()
    def test_single(self, query):
        M.create({M.id.column: 5, M.foo.column: 'foo'})
        query.assert_called_with('insert into "t" (foo,id) values (%s,%s) returning id', ['foo', 5])

    @stellata.tests.base.mock_query()
    def test_multi(self, query):
        M.create([{
            M.id.column: 1,
            M.foo.column: 'foo'
        }, {
            M.id.column: 2,
            M.foo.column: 'bar'
        }])

        query.assert_called_with('insert into "t" (foo,id) values (%s,%s),(%s,%s) returning id', ['foo', 1, 'bar', 2])
