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
            # function calls
            elif isinstance(token, sqlparse.sql.Function):
                # get the column name from inside the function
                # print(token.get_parameters())
                columns.append(token.get_parameters()[0].get_real_name())

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
                if str(token.left) not in columns:
                    columns.append(str(token.left))
        return columns

    return []
def get_comparator(comparison):
    for sub_token in comparison.tokens:
        if isinstance(sub_token, sqlparse.sql.Token) and sub_token.value.strip() in ('<', '<=', '=', '!=', '>', '>='):
            return sub_token.value.strip()

def get_where_predicates(query):
    ''' Get predicates on each column from WHERE clause of a query'''
    parsed = sqlparse.parse(query)
    where_clause = None
    # Find WHERE clause
    where_columns = get_where_columns(query) # should be in order - not a robust assumption but here we are
    for statement in parsed:
        for item in statement.tokens:
            if isinstance(item, Where):
                where_clause = item
                break
    # Extract predicates from WHERE clause
    where_predicates = [None for i in range(len(where_columns))]
    if where_clause:
        for token in where_clause.tokens:
            if isinstance(token, sqlparse.sql.Comparison):
                # predicate can be in any order
                # find which where column the predicate corresponds to
                print(token)
                if str(token.left) in where_columns:
                    comparand = token.right.value
                    i = where_columns.index(str(token.left))
                elif str(token.right) in where_columns:
                    comparand = token.left.value
                    i = where_columns.index(str(token.right))

                comparator = get_comparator(token)
                print(comparator)
                # remove quotes
                if comparand[0] == comparand[-1] and comparand[0] in ('"', "'"):
                    comparand = comparand[1:-1]
                if comparand.isalpha():
                    where_predicates[i] =comparand
                elif comparand.isnumeric():
                    # here we modify a range
                    if where_predicates[i] is None:
                        print("here")
                        if comparator in ('<', '<='):
                            where_predicates[i] = [float('-inf'), float(comparand)]
                        elif comparator in ('>', '>='):
                            where_predicates[i] = [float(comparand), float('inf')]
                    else:
                        if comparator in ('<', '<='):
                            where_predicates[i][1] = float(comparand)
                        elif comparator in ('>', '>='):
                            where_predicates[i][0] = float(comparand)

                else:
                    # value error
                    print(comparand, type(comparand))
                    raise ValueError("Predicate value not a string or number")
    return where_predicates

def add_operation(query, operation):
    # add the specified operation to the query
    parsed = sqlparse.parse(query)
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

        # Check if the operation already exists
        if any(token.value.upper() == operation.upper() for token in tokens_between):
            return query

        # Add the operation
        operation_token = sqlparse.sql.Token(None, f', {operation} ')
        stmt.tokens.insert(stmt.token_index(from_clause), operation_token)
        return str(stmt)
def add_count_star(query):
    return add_operation(query, 'COUNT(*)')
def add_var_samp(query, column_name):
    return add_operation(query, f'VAR_SAMP({column_name})')

def get_aggregate_operation(query):
    # assuming only one aggregate operation
    parsed = sqlparse.parse(query)
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

        # Check if there is a COUNT(*) already
        for token in tokens_between:
            if isinstance(token, sqlparse.sql.Function):
                return token.get_real_name()
        return None



