from configparser import ConfigParser
import psycopg2
import psycopg2.extras as extras
import chargeback_rpt.vm_data_utility as vm_util

from dotenv import dotenv_values
def get_postgres_conn():
 try:

  env_path=r'D:\ChargeBackApp\.env'
  config = dotenv_values(dotenv_path=env_path)
  conn = psycopg2.connect(
         database=config['DATABASES_NAME'], user=config['DATABASES_USER'],
      password=config['DATABASES_PASSWORD'], host=config['DATABASES_HOST'], port=config['DATABASES_PORT']
     )
  return conn

 except Exception as error:
  str_error=f'.env file at {env_path} error : {str(error)}'
  vm_util.add_error_to_file(str_error)
  raise error

def get_one_sql(conn,sql_cmd, params):

    row = None
    item=None
    try:
        cur = conn.cursor()
        if params is not None:
          cur.execute(sql_cmd, params)
        else:
          cur.execute(sql_cmd)

        #print("The number of cost: ", cur.rowcount)

        fields = [col[0] for col in cur.description]
        if cur.rowcount >0:
         item = dict(zip(fields, cur.fetchone()))

        #row = cur.fetchone()
        #         while row is not None:
        #             print(row)
        #             row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()
    #return dict

    return item


def get_list_sql(conn,sql_cmd, params):
    itemList=None
    try:
        cur = conn.cursor()

        if params is not None:
            cur.execute(sql_cmd, params)
        else:
            cur.execute(sql_cmd)

        #print("The number of cost: ", cur.rowcount)
        fields = [col[0] for col in cur.description]
        if cur.rowcount > 0:
         itemList=[dict(zip(fields, rw)) for rw in cur.fetchall()]

        # for row in rows:
        #     print(row)

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()
    # return list(dict)
    return itemList

#create ,update,delete
def add_multiple_data_sql(conn,sql_cmd,params):
 return  None

def add_one_data_sql(conn, sql_cmd, params):
    new_id = None
    try:
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql_cmd, params)
        # get the generated id back
        new_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()

    return new_id

def add_data_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    #return query,tuples
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        raise error
        return 0
    print("importing data succeeded")
    return 1
    cursor.close()


def update_data_sql(conn, sql, params):
    updated_rows = 0
    try:
        # create a new cursor
        cur = conn.cursor()

        # execute the UPDATE  statement
        cur.execute(sql, params)
        # get the number of updated rows
        updated_rows = cur.rowcount
        # Commit the changes to the database
        conn.commit()
        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()

    return updated_rows


def delete_data_sql(conn, sql, params):
    rows_deleted = 0
    try:

        # create a new cursor
        cur = conn.cursor()

        # execute the UPDATE  statement
        cur.execute(sql, params)

        # get the number of updated rows
        rows_deleted = cur.rowcount

        # Commit the changes to the database
        conn.commit()

        # Close communication with the PostgreSQL database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if conn is not None:
            conn.close()

    return rows_deleted



# config_file='chargeback_rpt/database.ini'
#config_file='database.ini'


# def config(filename=config_file, section='postgresql'):
#     # create a parser
#     parser = ConfigParser()
#     # read config file
#     parser.read(filename)
#
#     # get section, default to postgresql
#     db = {}
#     if parser.has_section(section):
#         params = parser.items(section)
#         for param in params:
#             db[param[0]] = param[1]
#     else:
#         raise Exception('Section {0} not found in the {1} file'.format(section, filename))
#
#     return db


# def get_postgres_conn():
#  try:
#   connStr_params = config()
#   conn = psycopg2.connect(**connStr_params)
#   return conn
#  except Exception as error:
#   raise error

