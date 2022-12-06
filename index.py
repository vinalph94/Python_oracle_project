import cx_Oracle
from config import connection_data

class sql_objects:
    def connect(self):
        cx_Oracle.init_oracle_client(lib_dir=connection_data['local_path'])
        dsn_tns = cx_Oracle.makedsn(connection_data['server_name'], '1523', service_name = 'pcse1p.data.uta.edu')
        conn = cx_Oracle.connect(user=connection_data['user_name'], password=connection_data['password'], dsn=dsn_tns)
        c = conn.cursor()
        print('connected')
        return c

    def close_connection(self, conn):
        print('closing connection')
        conn.close()

    def get_table_names(self, conn):
        conn.execute("SELECT TABLE_NAME FROM all_tables WHERE OWNER = 'AXP2009' ORDER BY TABLE_NAME")
        print("\n")
        for row in conn:
            print (row[0])

    def show_books(self, conn):
        conn.execute("SELECT * FROM AXP2009.FALL22_S001_13_BOOKS")
        print("\n")
        for row in conn:
            print(row[3],end=" - ")
            print(row[4])
            print("Category:"+ row[5])
            print(row[2])  
            print("\n")


p1 = sql_objects()
conn = p1.connect()
while True:
    print("\nMAIN MENU")  
    print("1. SHOW RELATIONS")  
    print("2. SHOW ALL BOOKS IN THE LIBRARY")
    print("3. RUN QUERY")  
    print("4. MODIFY RELATIONS")  
    choice1 = int(input("Enter the Choice:"))

    if choice1 == 1:
        p1.get_table_names(conn)
    elif choice1 == 2:
        p1.show_books(conn)
    else:
        p1.close_connection(conn)
        break

