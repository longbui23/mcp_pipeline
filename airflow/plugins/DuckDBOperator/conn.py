import duckdb


def connect():
    '''connect to duckdb'''
    conn_str = f"md:?motherduck_token=<{access_token}>"
    con = duckdb.connect(conn_str)

    return con
