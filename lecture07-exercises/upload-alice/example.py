import pymysql

# IP address of the MySQL database server, that is already started with datahub


Host = "mysql"
# Username of the database server (root is required to enable manipulation of the database)
User = "root"
# Password for the database user
Password = "datahub"
# database you want to use
database = "datahub"
conn = pymysql.connect(host=Host, user=User, password=Password, database=database)
cur = conn.cursor()

# Your task is to replace the following code to ingest the individual words of Alice in Wonderland
# First you need to read the file (the file is loaded into the root of the container at ./alice-in-wonderland.txt)
file = './alice-in-wonderland.txt'
'''hdfsclient = InsecureClient('http://localhost:9096', user='superuser')
text = hdfsclient.read(file)'''
words = []
with open(file) as f:
    text = f.read()
    words = text.split()


# Then you must split the content of the file into individual words


# Then create a table called alice and insert the individual words as (id, word) into the table

# Example of interacting with the database
query = f"CREATE TABLE IF NOT EXISTS alice (id int NOT NULL, word varchar(20))"
cur.execute(query)

for j in range(len(words)):
    query = f"INSERT INTO alice (id, word) VALUES ({j}, '{words[j]}')"
cur.execute(query)
print(f"{cur.rowcount} details inserted")
conn.commit()
conn.close()
