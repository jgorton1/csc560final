import time
import sqlparse
import pyarrow.parquet as pq
import parse
import sample_finder as sf
import pyarrow.dataset as ds
import duckdb as db
import pandasql as ps

def approx_query(query):
    # find the correct sample
    where_cols = parse.get_where_columns(query)
    output_cols = parse.get_output_columns(query)
    where_predicates = parse.get_where_predicates(query)
    op = parse.get_aggregate_operation(query)
    query = parse.add_count_star(query)
    query = parse.add_var_samp(query, output_cols[0])
    names, metadatas = sf.sample_finder("partitions.pkl", where_cols, output_cols, where_predicates)
    results = [None for i in range(len(names))]

    con = db.connect()
    for i, name in enumerate(names):
        lineitem = ds.dataset('samples/' + name + '.parquet', format='parquet')
        #lineitem = ds.('samples/' + name + '.parquet')
        print(metadatas[i])
        results[i] = con.execute(query).arrow()
        print(results[i])
    
    if op == "SUM":
        # calculate the weighted sum and confidence intervals
        weighted_sum = 0
        variance = 0
        for i, result in enumerate(results):
            strat_pop, strat_sample = metadatas[i]
            weight = float(strat_pop) / strat_sample
            outputcol = output_cols[0]
            sum = float(result[f"sum({outputcol})"][0].as_py())
            weighted_sum += sum * weight
            # calculate the variance
            strat_var = result[f"var_samp({outputcol})"][0].as_py()
            weighted_var = (1 - 1/weight) * strat_pop ** 2 / strat_sample * strat_var
            variance += weighted_var
        # calculate the 95% confidence interval
        error = 1.96 * variance ** 0.5

        ci = (weighted_sum - error, weighted_sum + error)


        return weighted_sum, ci

    elif op == "AVG":
        # calculate the weighted average and confidence intervals
        weighted_avg = 0
        variance = 0
        total_count = 0
        total_weight = 0
        for i, result in enumerate(results):
            strat_pop, strat_sample = metadatas[i]
            weight = float(strat_pop) / strat_sample
            total_weight += weight
            outputcol = output_cols[0]
            avg = float(result[f"avg({outputcol})"][0].as_py())
            weighted_avg += avg * weight
            total_count += strat_pop
            # calculate the variance
            strat_var = result[f"var_samp({outputcol})"][0].as_py()
            weighted_var = (1 - 1/weight) * strat_pop ** 2 / strat_sample * strat_var
            variance += weighted_var
        weighted_avg /= total_weight
        variance /= total_count ** 2
        # calculate the 95% confidence interval
        error = 1.96 * variance ** 0.5

        ci = (weighted_avg - error, weighted_avg + error)

        return weighted_avg, ci

    elif op == "COUNT":
        # calculate weighted count and confidence intervals
        weighted_count = 0
        variance = 0
        total_count = 0
        for i, result in enumerate(results):
            strat_pop, strat_sample = metadatas[i]
            print(strat_pop, strat_sample)
            weight = float(strat_pop) / strat_sample
            count = int(result[f"count_star()"][0].as_py())
            weighted_count += count * weight
            print(count, weight, weighted_count)
            total_count += strat_pop
            # calculate the variance
            proportion = count / strat_sample
            strat_var = strat_sample / (strat_sample -1) * proportion * (1 - proportion)
            weighted_var = (1 - 1/weight) * strat_pop ** 2 / strat_sample * strat_var
            variance += weighted_var
        error = 1.96 * variance ** 0.5
        ci = (weighted_count - error, weighted_count + error)
        return weighted_count, ci
    else:
        raise ValueError("Unsupported operation")


    # execute the query on the sample
    # return the result with error bars

def exact_query(query):
    con = db.connect()
    lineitem = ds.dataset('db_parq/lineitem.parquet', format='parquet')
    result = con.execute(query).arrow()
    print(result)

def main():
    query = "SELECT COUNT(l_tax) FROM lineitem WHERE l_returnflag= 'A' and l_extendedprice < 20000 and l_extendedprice > 10000"# and l_extendedprice > 10000"
    #query = "SELECT AVG(l_extendedprice) FROM lineitem WHERE l_returnflag = 'R' and l_shipmode = 'MAIL'"
    exact_query(query)
    start_time = time.time()
    print(approx_query(query))
    end_time = time.time()
    print("Execution time: ", end_time - start_time, "seconds")
    start_time = time.time()
    print(exact_query(query))
    end_time = time.time()
    print("Execution time: ", end_time - start_time, "seconds")
    #start_time = time.time()
    #print(exact_query_pandas(query))
    #end_time = time.time()
    #print("Execution time: ", end_time - start_time, "seconds")

    
if __name__ == '__main__':
    main()