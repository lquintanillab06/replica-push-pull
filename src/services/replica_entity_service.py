from .replica_configuration_service import resolver_insert_query,resolver_pk,resolver_update_query
from  mysql.connector.errors import IntegrityError


def get_entity(database_name,cursor,table,entity_id):
    pk = resolver_pk(database_name,cursor,table)
    query_entity = f"select * from {table} where {pk} = %(id)s "
    cursor.execute(query_entity,{'id':entity_id})
    row = cursor.fetchone()
    return row


def insert_entity(database_name,cursor,table,row):
    insert_query = resolver_insert_query(database_name,cursor,table)
    cursor.execute(insert_query,row)

def update_entity(database_name,cursor,table,row):
    update_query = resolver_update_query(database_name,cursor,table)
    cursor.execute(update_query,row)

def delete_entity(conectionDB,table,entity_id):
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        pk = resolver_pk(conectionDB.database,cursor,table)
        query_delete = f"delete from {table} where {pk} = %(id)s "
        cursor.execute(query_delete,{'id':entity_id})
        cnx.commit()
        cnx.close()
        return True, False
    except Exception as e:
        print(e)
        cnx.close()
        return False, True
    



def push_entity(local,remote,entity_name,entity_id,action):
    
    try:
        local_cnx = local.get_conexion()
        local_cursor = local_cnx.cursor(dictionary=True, buffered=True)
        row = get_entity(local.database,local_cursor,entity_name,entity_id)
    except Exception as e:
        print(e)
        local_cnx.close()
        return False, True

    try:
        row = get_entity(local.database,local_cursor,entity_name,entity_id)
    except Exception as e:
        print(e)
        local_cnx.close()
        return False, True
    
    try:
        remote_cnx = remote.get_conexion()
        remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)
        remote_cnx.close()
        return False, True
    
    if row or action == 'DELETE':
        try:
            if action == 'INSERT':
                print("Realizando el push-insert")
                insert_entity(remote.database,remote_cursor,entity_name,row)
                remote_cnx.commit()
            if action == 'UPDATE':
                print("Realizando el push-update")
                update_entity(remote.database,remote_cursor,entity_name,row)
                remote_cnx.commit()
            if action == 'DELETE':
                print("Realizando el push-delete")
                delete_entity(remote.database,remote_cursor,entity_name,entity_id)
                remote_cnx.commit()
            local_cnx.close()
            remote_cnx.close()
            return True, False
        except Exception as e:
            print(e)
            local_cnx.close()
            remote_cnx.close()
            return False, True 
    
    return False, True 

   

def pull_entity(local,remote,entity_name,entity_id,action):
    
    try: 
        remote_cnx = remote.get_conexion()
        remote_cursor = remote_cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
         print(e)
         return False, True
    
    try: 
        row = get_entity(remote.database,remote_cursor,entity_name,entity_id)
    except Exception as e:
         print(e)
         remote_cnx.close()
         return False, True

    try:
        local_cnx = local.get_conexion()
        local_cursor = local_cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
         print(e)
         return False, True
    
    if row or action == 'DELETE':
        try:
            if action == 'INSERT':
                print("Realizando el pull-insert")
                print(row)
                insert_entity(local.database,local_cursor,entity_name,row)
                local_cnx.commit()
            if action == 'UPDATE':
                print(row)
                print("Realizando el pull-update")
                valorVersion = validUpdate(local_cursor,remote_cursor,entity_name,row, 'PULL')
                if valorVersion:
                    # print(" <<<<        ACTUALIZANDO    >>> ")
                    update_entity(local.database,local_cursor,entity_name,row)
                    local_cnx.commit()                    
            if action == 'DELETE':
                print("Realizando el pull-delete")
                #delete_entity(local.database,local_cursor,entity_name,entity_id)
                delete_entity(local, entity_name,entity_id)
                local_cnx.commit()
            local_cnx.close()
            remote_cnx.close()
            return True, False
        except Exception as e:
            print(e)
            local_cnx.close()
            remote_cnx.close()
            return False, True

def get_replica_entity(conectionDB, table, entity_id):
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        entity = get_entity(conectionDB.database,cursor,table,entity_id)
        cnx.close()
        return entity
    except Exception as e:
        print(e)
        return None
    
def get_replica_entity_by_field(conectionDB, table, field, field_id):

    query_entity = f"select * from {table} where {field} = %(id)s "
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        cursor.execute(query_entity,{'id':field_id})
        row = cursor.fetchone()
        cnx.close()
        return row
    except Exception as e:
        print(e)
        return None
  
def insert_replica_entity(conectionDB, table, entity):
    print(f"     INSERTANDO  {table}")
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        pass
        print(e)

    try:
        insert_entity(conectionDB.database,cursor,table,entity)
        cnx.commit()
        cnx.close()
        return True, None

    except IntegrityError as e:
        print(e)
        cnx.close()
        return False, e.errno
    except Exception as e:
        print(e)
        cnx.close()
        return False, None

def update_replica_entity(conectionDB, table, entity):  
    print(" ▀▀▀▀▀▀▀  ACTUALIZANDO   ▀▀▀▀▀▀▀ ")
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
    except Exception as e:
        print(e)

    try:
        update_entity(conectionDB.database,cursor,table,entity)
        cnx.commit()
        cnx.close()
        return True, None
    except Exception as e:
        print(e)
        cnx.close()
        return False, None 

def insert_or_update_entity(conectionDB, table, entity):
    _,error = insert_replica_entity(conectionDB,table,entity)
    print(error)
    if error == 1062:
        update_replica_entity(conectionDB,table,entity)
    
def get_replica_entities(conectionDB, table, field, field_id):

    query_entity = f"select * from {table} where {field} = %(id)s "
    try:
        cnx = conectionDB.get_conexion()
        cursor = cnx.cursor(dictionary=True, buffered=True)
        cursor.execute(query_entity,{'id':field_id})
        row = cursor.fetchall()
        cnx.close()
        return row
    except Exception as e:
        print(e)
        return None

def deleteCobros():
    print("delete")

def validUpdate(local_cursor,remote_cursor,table,entity_id,action):
    """ verificar si las versiones de los distintos 
    registros sean diferentes para actualizarse y de lo contrario no actualizar """
    # print(table)
    # print(entity_id)
 

    query_entity = f"select id,version from {table} where id = %(id)s "

    local_cursor.execute(query_entity,{'id':entity_id['id']})
    rowL = local_cursor.fetchone()

    remote_cursor.execute(query_entity,{'id':entity_id['id']})
    rowR = remote_cursor.fetchone()
    
    print(f"LA INFORMACION DE LA SUCURSAL ES:   {rowL}")
    print(f"LA INFORMACION DE LA NUBE ES:   {rowR}")

    if rowL['version'] == rowR['version']:
        print('LAS VERSIONES SON IGUALES ')
        print("no hay nada que actualizar")
        return False
    elif rowR['version'] > rowL['version'] and action == 'PULL':
        print("La nube tiene una version mas reciente debe actualizarse ")
        return True
    elif rowL['version'] > rowR['version'] and action == 'PUSH':
        print("La SUCURSAL tiene una version mas reciente debe actualizarse EN LA NUBE ")
        return True
    else:
        print("NO CUMPLE CON LAS REGLAS NO ACTUALIZARA NADA")
        return False
    # return rowR