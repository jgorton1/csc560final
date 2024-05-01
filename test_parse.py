
import unittest
from parse import get_output_columns, get_where_columns, get_where_predicates, add_count_star, get_aggregate_operation

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

    def test_example_query(self):
        query = "SELECT AVG(l_tax) FROM lineitem WHERE l_returnflag = 'A' and l_extendedprice < 20000 and l_extendedprice > 10000"
        expected_output_columns = ['l_tax']
        expected_where_columns = ['l_returnflag', 'l_extendedprice']
        self.assertEqual(get_output_columns(query), expected_output_columns)
        self.assertEqual(get_where_columns(query), expected_where_columns)
        print(get_where_predicates(query))

    def test_add_count_star_no_count_star(self):
        query = 'SELECT name, age FROM users'
        expected_query = 'SELECT name, age , COUNT(*) FROM users'
        self.assertEqual(add_count_star(query), expected_query)

    def test_add_count_star_with_count_star(self):
        query = 'SELECT COUNT(*) FROM users'
        expected_query = 'SELECT COUNT(*) FROM users'
        self.assertEqual(add_count_star(query), expected_query)

    def test_add_count_star_multiple_select_statements(self):
        query = 'SELECT name FROM users'
        expected_query = 'SELECT name , COUNT(*) FROM users'
        self.assertEqual(add_count_star(query), expected_query)

    def test_get_aggregate_operation_with_count_star(self):
        query = 'SELECT COUNT(*) FROM users'
        expected_operation = 'COUNT'
        self.assertEqual(get_aggregate_operation(query), expected_operation)

    def test_get_aggregate_operation_with_avg(self):
        query = 'SELECT AVG(salary) FROM employees'
        expected_operation = 'AVG'
        self.assertEqual(get_aggregate_operation(query), expected_operation)

    def test_get_aggregate_operation_with_sum(self):
        query = 'SELECT SUM(quantity) FROM orders'
        expected_operation = 'SUM'
        self.assertEqual(get_aggregate_operation(query), expected_operation)

    def test_get_aggregate_operation_with_min(self):
        query = 'SELECT MIN(price) FROM products'
        expected_operation = 'MIN'
        self.assertEqual(get_aggregate_operation(query), expected_operation)

    def test_get_aggregate_operation_with_max(self):
        query = 'SELECT MAX(price) FROM products'
        expected_operation = 'MAX'
        self.assertEqual(get_aggregate_operation(query), expected_operation)


if __name__ == '__main__':
    unittest.main()
