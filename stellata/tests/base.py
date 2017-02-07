import stellata.database

import unittest
import unittest.mock

def mock_execute():
    return unittest.mock.patch('stellata.database.execute')

def mock_query():
    return unittest.mock.patch('stellata.database.query')

class Base(unittest.TestCase):
    up = None
    down = None

    def setUp(self):
        if self.up:
            stellata.database.initialize({
                'name': 'stellata_test',
                'user': 'stellata_test',
                'password': 'stellata_test',
            })

            stellata.database.execute(self.up)

    def tearDown(self):
        if self.down:
            stellata.database.execute(self.down)
