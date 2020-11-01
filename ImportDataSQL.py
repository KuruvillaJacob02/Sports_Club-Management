import pickle
import mysql.connector as ms
print('''This program will import all data from the binary files and
store it in sql databases.Only run this program if no database for the sports club has been created''')
ch=input("Do you want to create the database and import data from the Binary files? Y/N")
if ch=='Y':
    host=input("Enter host name")
    passwd=input("Enter Sql password")
    user=input("Enter SQL username")
    myq=ms.connect(host=host,user=user,passwd=passwd)
    if myq.is_connected():
        print("Success")
    cur=myq.cursor(buffered=True) # Create Cursor
    print(cur.execute(" create database SportsClub "))
    cur.execute("use SportsClub")

    #Create Table Member
    cur.execute('''create table Member (Code char(9) Primary Key, 
    Name varchar(60) not null, 
    Date date, 
    Address char(100), 
    Ph int(9),  
    Fc1 char(2),
    Fc2 char(2),
    Fc3 char(2))''')
    with open("Member File.dat",'rb') as f:
        try:
            while True:
                rec=pickle.load(f)
                print(rec)
                query="insert into Member values('{0}','{1}','{2}','{3}',{4},'{5}','{6}','{7}')".format(rec[0],rec[1],rec[2].strftime("%Y-%m-%d"),rec[3],rec[4],rec[5],rec[6],rec[7])
                cur.execute(query)
                myq.commit()
        except:
            print("Member data imported")
    with open("Member File.dat",'rb') as f:
        list1=[]
        try:
            while True:
                rec=pickle.load(f)
                list1.append(rec)
        except:
            print("Member data imported")
            for rec in list1:
                query="insert into member values('{0}','{1}','{2}','{3}',{4},'{5}','{6}','{7}')".format(rec[0],rec[1],rec[2].strftime("%Y-%m-%d"),rec[3],rec[4],rec[5],rec[6],rec[7])
                cur.execute(query)
                myq.commit()

    # Create Fees table
    cur.execute('''create table Fees (Code char(9) Primary Key, 
    Paid_Date date, 
    Due_Date date, 
    Amount_Paid int(9))''')

    with open('Fees File.dat','rb')as fees:
        list1=[]
        try:
            while True:
                rec=pickle.load(fees)
                list1.append(rec)
        except:
            for rec in list1:
                query="insert into Fees values('{0}','{1}','{2}',{3})".format(rec[0],rec[1].strftime("%Y-%m-%d"),rec[2].strftime("%Y-%m-%d"),rec[3])
                cur.execute(query)
                myq.commit()
            print("Fees Data Imported")
    
    #Create Facility Table
    cur.execute('''create table Facility (Code char(9) Primary Key, 
    Facility char(20))''')

    with open("Facility File.dat","rb")as fac:
        list1=[]
        try:
            while True:
                rec=pickle.load(fac)
                list1.append(rec)
        except:
            for rec in list1:
                query="insert into Facility values('{0}','{1}')".format(rec[0],rec[1])
                cur.execute(query)
                myq.commit()
            print("Facility Data Imported")