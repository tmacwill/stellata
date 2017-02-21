import stellata.database

import random
import unittest
import unittest.mock

db = stellata.database.initialize(
    name='stellata_test',
    user='stellata_test',
    password='stellata_test'
)

db2 = stellata.database.Pool(
    name='stellata_test2',
    user='stellata_test2',
    password='stellata_test2'
)

def mock_execute():
    return unittest.mock.patch('stellata.database.Pool.execute')

def mock_query():
    return unittest.mock.patch('stellata.database.Pool.query')

class Base(unittest.TestCase):
    up = None
    down = None

    def setUp(self):
        random.seed(12345)
        if self.up:
            db.execute(self.up)

    def tearDown(self):
        if self.down:
            db.execute(self.down)
