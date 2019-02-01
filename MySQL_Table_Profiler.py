import mysql.connector
import pandas as pd

conn = mysql.connector.connect(host='localhost',  database='', user='', password='')

def get_field_list(mysql_table_name):
    df = pd.read_sql("desc %s" % mysql_table_name, conn)
    field_list = df['Field'].values.tolist()
    # print(field_list[3:7])
    return field_list[3:7]


def profile_data(mysql_table_name):
    field_list = get_field_list(mysql_table_name)
    cursor = conn.cursor()
    cursor.execute("drop table if exists work_table_1")
    cursor.execute("create table work_table_1 (field_name varchar(20), most_common_value varchar(50), n_occurrences int)")

    for i in field_list:
        sql_script_1 = """
        insert into work_table_1
        select "%s" as field_name, field as most_common_value, n as n_occurrences
        from (select %s as field, count(*) as n from %s group by field) b
        order by n_occurrences desc limit 1 ;
        """ % (i, i, mysql_table_name)

        print("SQL Script: " + sql_script_1)
        cursor.execute(sql_script_1)
        conn.commit()

profile_data('usertbl')
