import dataset
import os
import getpass
import re
import pymysql
pymysql.install_as_MySQLdb()


def connect_to_db(table):
    host = 'localhost'
    port = 3306
    user = 'root'
    password = 'the-secret-pw'
    path = 'mysql://'+ user +':'+ password + '@'+ host +':' + str(port) + '/' + table

    if 'MYSQL_ROOT_PASSWORD' in os.environ:
        host = os.getenv('MYSQL_HOST')
        port = os.getenv('MYSQL_PORT')
        user = "root"
        password = os.getenv('MYSQL_ROOT_PASSWORD')
        path = 'mysql://'+ user +':'+ password + '@'+ host +':' + str(port) + '/' + table
        print('db path: ', path)
    return dataset.connect(path)

def create_user(username, password):

    db = connect_to_db("teams")
    table = db.create_table("registrations")
    exists = table.find_one(username=username)
    if exists:
        print(exists)
        print( "credentials with name %s already exist" % username)
        return
    table.insert(dict(username=username, password=password))
    print("User was created successfully")
    return

if __name__ == '__main__':
    print("Creating credentials to have access to the benchmark backend. ")

    print('Curret users in DB:')
    for userData in connect_to_db('teams')['registrations']:
        print(userData)

    print("INFO! Username should contain @ character")


    username = input('Provide username: ').strip(" ")
    username = re.sub(r"[,;\"\'\n\t\s]*", "",username)

    if "@" not in username:
        print("Username should contain @ character")
    while(True):
        password1 = getpass.getpass(prompt='Provide password: ')
        password2 = getpass.getpass(prompt='Type the password again: ')
        if password1 != password2:
            print("Passwords do not match. try again")
        else:
            create_user(username, password1)
            exit(0)
