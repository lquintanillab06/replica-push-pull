
def resolver_pk(database_name,cursor,table):
    sql_pk = """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE (TABLE_SCHEMA = %(db)s)
            AND (TABLE_NAME = %(table)s)
            AND (COLUMN_KEY = 'PRI')
        """
    cursor.execute(sql_pk,{'db': database_name, 'table': table})
    pk =  cursor.fetchone()["COLUMN_NAME"]
    return pk


def resolver_columnas(database_name,cursor,table,action):
    sql_columns = """"""
    if action == 'update':
        sql_columns = """
             SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE (TABLE_SCHEMA = %(db)s)
             AND (TABLE_NAME = %(table)s)
             AND (COLUMN_KEY <> 'PRI')
        """

    if action == 'insert': 
        sql_columns = """
             SELECT COLUMN_NAME FROM information_schema.COLUMNS
             WHERE (TABLE_SCHEMA = %(db)s)
             AND (TABLE_NAME = %(table)s)
        """
    cursor.execute(sql_columns,{'db': database_name, 'table': table})
    columns = cursor.fetchall()
    return columns

def resolver_update_query(database_name,cursor,table):
    pk = resolver_pk(database_name,cursor,table)
    columns = resolver_columnas(database_name,cursor,table, 'update')
    res = f"UPDATE {table} SET "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            res = res + f"{column['COLUMN_NAME']} = %({column['COLUMN_NAME']})s," 
        if(idx == (len(columns)-1) ):
            res = res + f"{column['COLUMN_NAME']} = %({column['COLUMN_NAME']})s" 
    res+= f" WHERE {pk} = %({pk})s"
    return res

def resolver_insert_query(database_name,cursor,table):
    pk = resolver_pk(database_name,cursor,table)
    columns = resolver_columnas(database_name,cursor,table, 'insert')
    columnas = ''
    valores = ''
    res = f"INSERT INTO {table} "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            columnas = columnas+f"{column['COLUMN_NAME']},"
            valores = valores+f"%({column['COLUMN_NAME']})s,"
        if(idx == (len(columns)-1) ):
            columnas = columnas+f"{column['COLUMN_NAME']}"
            valores = valores+f"%({column['COLUMN_NAME']})s"
    res = res+ f"({columnas}) VALUES ({valores})"       
    return res
   

def resolver_insert_query_sin_id(database_name,cursor,table):
    pk = resolver_pk(database_name,cursor,table)
    columns = resolver_columnas(database_name,cursor,table, 'update')
    columnas = ''
    valores = ''
    res = f"INSERT INTO {table} "
    for idx,column in enumerate(columns):
        if(idx <= (len(columns)-2) ):
            columnas = columnas+f"{column['COLUMN_NAME']},"
            valores = valores+f"%({column['COLUMN_NAME']})s,"
        if(idx == (len(columns)-1) ):
            columnas = columnas+f"{column['COLUMN_NAME']}"
            valores = valores+f"%({column['COLUMN_NAME']})s"
    res = res+ f"({columnas}) VALUES ({valores})"       
    return res

    