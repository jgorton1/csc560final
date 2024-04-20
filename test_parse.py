import unittest
from parse import get_output_columns, get_where_columns

class TestParse(unittest.TestCase):
    def test_parse_empty_query(self):
        query = ''
        expected_columns = []
        self.assertEqual(get_output_columns(query), expected_columns)

    def test_parse_single_column_query(self):
        query = 'SELECT name FROM users'
        expected_columns = ['name']
        self.assertEqual(get_output_columns(query), expected_columns)

    def test_parse_multiple_columns_query(self):
        query = 'SELECT name, age, email FROM users'
        expected_columns = ['name', 'age', 'email']
        self.assertEqual(get_output_columns(query), expected_columns)

    def test_parse_query_with_conditions(self):
        query = 'SELECT name FROM users WHERE age > 18'
        expected_columns = ['name']
        self.assertEqual(get_output_columns(query), expected_columns)

    '''def test_parse_query_with_join(self):
        query = 'SELECT u.name, a.address FROM users u JOIN addresses a ON u.id = a.user_id'
        expected_columns = ['u.name', 'a.address']
        self.assertEqual(parse(query), expected_columns)'''
    def test_get_where_columns_no_conditions(self):
        query = 'SELECT name FROM users'
        expected_columns = []
        self.assertEqual(get_where_columns(query), expected_columns)

    def test_get_where_columns_single_condition(self):
        query = 'SELECT name FROM users WHERE age > 18'
        expected_columns = ['age']
        self.assertEqual(get_where_columns(query), expected_columns)

    def test_get_where_columns_multiple_conditions(self):
        query = 'SELECT name FROM users WHERE age > 18 AND city = "New York"'
        expected_columns = ['age', 'city']
        self.assertEqual(get_where_columns(query), expected_columns)

    def test_get_where_columns_with_join(self):
        query = 'SELECT u.name, a.address FROM users u JOIN addresses a ON u.id = a.user_id WHERE u.age > 18 AND a.city = "New York"'
        expected_columns = ['u.age', 'a.city']
        self.assertEqual(get_where_columns(query), expected_columns)

if __name__ == '__main__':
    unittest.main()