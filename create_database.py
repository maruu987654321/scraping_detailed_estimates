import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="www777#A",
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE all_stocks")