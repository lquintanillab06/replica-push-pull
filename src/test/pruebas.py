from src.database import get_local_connection,get_local_pool_connection
from mysql.connector import errors


def prueba_query_chofer():
    localDB= get_local_connection()
    sql = f"select * from chofer"
    localDB.cursor.execute(sql)
    audits = localDB.cursor.fetchall()
    print("*" * 100)
    print(audits)

def prueba_pool():
    localPoolDB = get_local_pool_connection()
    cnx1 = localPoolDB.get_conexion()
    cursor = cnx1.cursor()
    sql = "select * from sucursal"
    cursor.execute(sql)
    sucs = cursor.fetchall()
    print(sucs)
    cnx1.close()

def prueba_query_sucs_pool():
    localPoolDB = get_local_pool_connection()
    cnx1 = localPoolDB.conexion.get_connection()
    cnx2 = localPoolDB.conexion.get_connection()
    cnx3 = localPoolDB.conexion.get_connection()
    try:
        # fetch the fourth connection from the pool
        cnx4 = localPoolDB.conexion.get_connection()

    except errors.PoolError as e:
        # connection pool exhausted, so we can't fetch 4th connection
        print(e)
    
    print('Closing cnx3 connection ')
    cnx3.close()
    cnx4 = localPoolDB.conexion.get_connection()
    cursor = cnx4.cursor()
    sql = "select * from sucursal"
    cursor.execute(sql)
    sucs = cursor.fetchall()
    print(sucs)
    cnx1.close()
    cnx2.close()
    cnx4.close()
    
    
