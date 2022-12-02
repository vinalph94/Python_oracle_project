import cx_Oracle
from config import connection_data
from tabulate import tabulate

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

    def getMostBorrowedBooks(self,conn):
        #RETURN the Name, ID and Genre OF the book which was borrowed n or more times IN the LAST quarter OF 2022
        try:
            value = input("\nEnter the value for number of times borrowed :")
            conn.execute("SELECT Name, fsb.BOOK_ID AS BOOK_ID, Genre FROM axp2009.FALL22_S001_13_BORROW_RECORD fsbr, AXP2009.FALL22_S001_13_BOOKS fsb WHERE FSBR.BOOK_ID = fsb.BOOK_ID AND fsb.BOOK_ID = ANY (SELECT BOOK_ID FROM axp2009.FALL22_S001_13_BORROW_RECORD br WHERE DATE_OF_ISSUE  >= to_timestamp('2022-10-01', 'YYYY-MM-DD') and DATE_OF_ISSUE  < to_timestamp('2022-12-31', 'YYYY-MM-DD') GROUP BY BOOK_ID HAVING count(BOOK_ID)>= :var) GROUP BY Name, fsb.BOOK_ID, Genre", {'var': value})
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['NAME', 'BOOK', 'GENRE'], tablefmt='psql'))
           
        except cx_Oracle.DatabaseError as er:
            print('There is error in the Oracle database:', er)
 
        except Exception as er:
            print('Error:', er)

    def getTotalFee(self,conn):
        #Returns the name AND ID OF students who have a totat late fee OF MORE than N
        try:
            fee = input("\nEnter the value for total fee :")
            conn.execute("SELECT Fname, Lname, fss.student_ID, sum(LATE_FEE) AS Total_Penalty FROM AXP2009.FALL22_S001_13_STUDENTS fss, AXP2009.FALL22_S001_13_BORROW_RECORD fsbr WHERE fss.student_ID = fsbr.STUDENT_ID  GROUP BY Fname, Lname, fss.student_ID HAVING sum(LATE_FEE) >= :amount ORDER BY sum(LATE_FEE) DESC", {'amount': fee})
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['FNAME', 'LNAME', 'STUDENT_ID', 'TOTAL_PENALTY'], tablefmt='psql'))
           
        except cx_Oracle.DatabaseError as er:
            print('There is error in the Oracle database:', er)
 
        except Exception as er:
            print('Error:', er)

    def getMostBorrowedPublisher(self,conn):
        #Returns the name of the publisher who's books were most borrowed 
        try:
            conn.execute("select * from (SELECT Name, COUNT(Name) AS times FROM AXP2009.FALL22_S001_13_PUBLISHER fsp, AXP2009.FALL22_S001_13_PUBLISHED_BY fspb, AXP2009.FALL22_S001_13_BORROW_RECORD fsbr WHERE fsp.PUBLISHER_ID = fspb.PUBLISHER_ID AND fspb.BOOK_ID = fsbr.BOOK_ID GROUP BY NAME) where times=(select Max(times) from (SELECT Name, COUNT(Name) AS times FROM AXP2009.FALL22_S001_13_PUBLISHER fsp, AXP2009.FALL22_S001_13_PUBLISHED_BY fspb, AXP2009.FALL22_S001_13_BORROW_RECORD fsbr WHERE fsp.PUBLISHER_ID = fspb.PUBLISHER_ID AND fspb.BOOK_ID = fsbr.BOOK_ID GROUP BY NAME))")
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['NAME', 'TIMES'], tablefmt='psql'))

    def pl.getAverageLateFee(self,conn):

        try:
            conn.execute("SELECT DATE_OF_ISSUE, round(AVG(late_fee) OVER (ORDER BY DATE_OF_ISSUE ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),2) as AVG_LATE_FEE from AXP2009.FALL22_S001_13_BORROW_RECORD fsbr") 
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['DATE_OF_ISSUE'],tablefmt='psql'))

    def pl.getTotalLateFee(self,conn):

        try:
            conn.execute("SELECT fsbr.BOOK_ID, fsbr.DATE_OF_ISSUE, SUM(fsbr.LATE_FEE) FROM FALL22_S001_13_BORROW_RECORD fsbr INNER JOIN FALL22_S001_13_BOOKS fsb ON fsbr.BOOK_ID = fsb.BOOK_ID GROUP BY fsbr.BOOK_ID, ROLLUP(fsbr.DATE_OF_ISSUE)") 
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['BOOK_ID','DATE_OF_ISSUE','LATE_FEE'],tablefmt='psql'))  

    def pl.getCountForQuarter(self,conn):

        try:
            conn.execute("SELECT fss.FNAME, TO_CHAR(fsb.REGISTERED_BOOK_ON, 'Q') AS "QUARTER", fsb.GENRE, COUNT(*) FROM FALL22_S001_13_BOOKS fsb INNER JOIN FALL22_S001_13_STAFF fss ON fss.STAFF_ID = fsb.STAFF_ID GROUP BY TO_CHAR(fsb.REGISTERED_BOOK_ON, 'Q'), fss.FNAME, ROLLUP(fsb.GENRE) FETCH NEXT 10 ROWS ONLY") 
            rows = conn.fetchall()
            print(tabulate(rows, showindex=False, headers=['FNAME','GENRE'],tablefmt='psql'))                          
           
        except cx_Oracle.DatabaseError as er:
            print('There is error in the Oracle database:', er)
 
        except Exception as er:
            print('Error:', er)


p1 = sql_objects()
conn = p1.connect()
while True:
    print("\nMAIN MENU")  
    print("1. SHOW RELATIONS")  
    print("2. MODIFY RELATIONS")  
    print("3. RUN QUERY")  
    choice1 = int(input("Enter the Choice:"))

    if choice1 == 1:
        p1.get_table_names(conn)

    elif choice1 == 3:
        print("\MENU FOR BUSINESS GOALS")  
        print("1. Show the Name, ID and Genre OF the book which was borrowed N or more times IN the LAST quarter OF 2022")  
        print("2. Show the name AND ID OF students who have a totat late fee OF MORE than N")  
        print("3. Show the name of the publisher who's books were most borrowed")  
        print("4. Show average over three rows (PRECEDING, CURRENT, FOLLOWING) for the late fee of books borrowed rounded upto 2 decimals ordered by DATE_OF_ISSUE")  
        print("5. Show the total late fee collected for every book that is borrowed for all dates along with its total cost per book")  
        print("6. Show the count of books registered into the system by staff for every quarter of the year and by genre")  
        choice2 = int(input("\nPlease Enter the Choice for the business goals:"))

        if choice2 == 1:
            p1.getMostBorrowedBooks(conn)

        elif choice2 == 2:
            p1.getTotalFee(conn)

        elif choice2 == 3:
            p1.getMostBorrowedPublisher(conn)

        elif choice2 == 4:
            pl.getAverageLateFee(conn) 

        elif choice2 == 5:
            pl.getTotalLateFee(conn)
            
        elif choice2 == 6:
            pl.getCountForQuarter(conn)    
            
        else:
            p1.close_connection(conn)
            break
 
    else:
        p1.close_connection(conn)
        break

