import unittest
from parse import parse

class TestParse(unittest.TestCase):
    def test_parse_empty_query(self):
        query = ''
        expected_columns = []
        self.assertEqual(parse(query), expected_columns)

    def test_parse_single_column_query(self):
        query = 'SELECT name FROM users'
        expected_columns = ['name']
        self.assertEqual(parse(query), expected_columns)

    def test_parse_multiple_columns_query(self):
        query = 'SELECT name, age, email FROM users'
        expected_columns = ['name', 'age', 'email']
        self.assertEqual(parse(query), expected_columns)

    def test_parse_query_with_conditions(self):
        query = 'SELECT name FROM users WHERE age > 18'
        expected_columns = ['name']
        self.assertEqual(parse(query), expected_columns)

    '''def test_parse_query_with_join(self):
        query = 'SELECT u.name, a.address FROM users u JOIN addresses a ON u.id = a.user_id'
        expected_columns = ['u.name', 'a.address']
        self.assertEqual(parse(query), expected_columns)'''

if __name__ == '__main__':
    unittest.main()