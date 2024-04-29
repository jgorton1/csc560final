'''
SQL Parser for simple queries
does not allow nested queries or joins
collects and return all of the columns mentioned in the query
so that we can make a sample using them
'''

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where, Comparison

def get_output_columns(query):
    parsed = sqlparse.parse(query)
    columns = []

    for stmt in parsed:
        # Check if the statement is a SELECT statement
        if stmt.get_type() != 'SELECT':
            continue

        # Find the SELECT clause
        select_clause = next((token for token in stmt.tokens if token.ttype is sqlparse.tokens.Keyword.DML and token.value.upper() == 'SELECT'), None)
        if select_clause is None:
            continue

        # Find the FROM clause
        from_clause = next((token for token in stmt.tokens if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM'), None)

        # Get the tokens between SELECT and FROM
        if from_clause is not None:
            tokens_between = stmt.tokens[stmt.token_index(select_clause)+1:stmt.token_index(from_clause)]
        else:
            tokens_between = stmt.tokens[stmt.token_index(select_clause)+1:]

        # Extract the column names from these tokens
        for token in tokens_between:
            if isinstance(token, Identifier):
                columns.append(token.get_real_name())
            elif isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    columns.append(identifier.get_real_name())

    return columns    

def get_where_columns(query):
    ''' Get columns from WHERE clause of a query'''
    parsed = sqlparse.parse(query)
    where_clause = None

    # Find WHERE clause
    for statement in parsed:
        for item in statement.tokens:
            if isinstance(item, Where):
                where_clause = item
                break

    # Extract columns from WHERE clause
    if where_clause:
        columns = []
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                columns.append(str(token.left))
        return columns

    return []

def get_where_predicates(query):
    ''' Get predicates on each column from WHERE clause of a query'''
    parsed = sqlparse.parse(query)
    where_clause = None
    # Find WHERE clause
    for statement in parsed:
        for item in statement.tokens:
            if isinstance(item, Where):
                where_clause = item
                break
    # Extract predicates from WHERE clause