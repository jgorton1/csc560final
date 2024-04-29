import sqlparse
query = "SELECT SUM(l_tax) FROM lineitem WHERE RETURNFLAG = 'R' and l_extendedprice < 1000"
print(sqlparse.parse(query)[0].tokens)
# print where clause tokens
where_clause = None
parsed = sqlparse.parse(query)
for statement in parsed:
    for item in statement.tokens:
        if isinstance(item, sqlparse.sql.Where):
            where_clause = item
            break

# print comparators in where clause
if where_clause:
    for token in where_clause.tokens:
        if isinstance(token, sqlparse.sql.Comparison):
            print(token)
