import os
import pymysql


def save(table_name, columns, values):
    connection = _get_connection()

    if len(columns) != len(values):
        raise Exception(f'Num columns [{len(columns)}] not equal to num of values [{len(values)}]')

    columns_string = ','.join(columns)
    format_strings = ','.join(['%s'] * len(values))
    with connection.cursor() as cursor:

        try:
            sql = f"INSERT INTO {table_name} ({columns_string}) " \
                  f"VALUES ({format_strings})"
            cursor.execute(sql, values)
            connection.commit()
            return cursor.lastrowid
        finally:
            connection.close()


def update(table_name, columns, values, where_condition_string):
    connection = _get_connection()

    if len(columns) != len(values):
        raise Exception(f'Num columns [{len(columns)}] not equal to num of values [{len(values)}]')

    assignment_string = ",".join([f"{c}=%s" for c in columns])

    with connection.cursor() as cursor:

        try:
            sql = f"UPDATE {table_name} " \
                  f"SET {assignment_string} " \
                  f"WHERE {where_condition_string}"
            cursor.execute(sql, values)
            connection.commit()
            return cursor.lastrowid
        finally:
            connection.close()


def get(table_name, columns, where_condition_string):
    connection = _get_connection()
    columns_string = ','.join(columns)

    with connection.cursor() as cursor:
        try:
            sql = f"SELECT {columns_string} from {table_name} WHERE {where_condition_string}"
            cursor.execute(sql)
            entries = cursor.fetchall()
            return entries
        finally:
            connection.close()


def exists(table_name, where_condition_string):
    connection = _get_connection()
    with connection.cursor() as cursor:
        try:
            sql = f"SELECT * from {table_name} WHERE {where_condition_string}"
            cursor.execute(sql)
            existing_entry = cursor.fetchall()
            return len(existing_entry) > 0
        finally:
            connection.close()


def _get_connection():
    return pymysql.connect(host='fjv3-cluster.cluster-cdeoqg4injkb.eu-west-1.rds.amazonaws.com',
                           port=3306,
                           user=os.environ["MYSQL_USER"],
                           passwd=os.environ["MYSQL_PW"],
                           db='mannsi')
