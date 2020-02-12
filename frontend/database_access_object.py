import pymysql
pymysql.install_as_MySQLdb()
import dataset
import os
import sys
import datetime
import subprocess

# scheduler service name for restarting upon new entry
SCHEDULER ="scheduler"
def restart_scheduler(container_name):
    subprocess.check_output(['docker', 'restart', container_name])

# generic root connection. To be used separately elsewhere
def connect_to_db(table, access='user'):

    if 'MYSQL_ROOT_PASSWORD' in os.environ:
        host = os.getenv('MYSQL_HOST')
        port = os.getenv('MYSQL_PORT')
        if access == 'root':
            user = "root"
            password = os.getenv('MYSQL_ROOT_PASSWORD')
        else:
            user = os.getenv('MYSQL_USER')
            password = os.getenv('MYSQL_PASSWORD')
        path = 'mysql://'+ user +':'+ password + '@'+ host +':' + str(port) + '/' + table
        # print(path)
        return dataset.connect(path)
    else:
        print(" MYSQL ENVs is not ")

class Database:
    def __init__(self, table_name):
        self.table_name = table_name
        self.connect_to_db(self.table_name)

    def connect_to_db(self, table):
        host = 'localhost'
        port = 3306
        user = 'dbuser'
        password = 'dbpassword'
        dbase = table
        path = 'mysql://'+ user +':'+ password + '@'+ host +':' + str(port) + '/' + dbase

        if 'MYSQL_ROOT_PASSWORD' in os.environ:
            host = os.getenv('MYSQL_HOST')
            port = os.getenv('MYSQL_PORT')
            user = os.getenv('MYSQL_USER')
            password = os.getenv('MYSQL_PASSWORD')
            dbase = os.getenv('MYSQL_DATABASE')
            # print(host, port, user, password, dbase)

            path = 'mysql://'+ user +':'+ password + '@'+ host +':' + str(port) + '/' + dbase
            # print(path)
        self.db = dataset.connect(path)

    def add_team(self, team, image, status):
        # Note! upsert wont work with 'name' field
        table = self.db[self.table_name]
        row = table.find_one(name=team)
        if row:
            # print("Found entry", row)
            table.update(dict(name=team, team_image_name=image, updated=status), ['name'])
        else:
            # print("Entry is new")
            table.insert(dict(name=team, team_image_name=image, updated=status))
            restart_scheduler(SCHEDULER)
        sys.stdout.flush()

    def update_image(self, image_name, timestamp):
        table = self.db[self.table_name]
        table.update(dict(team_image_name=image_name,  time_tag=timestamp, updated='True'), ['team_image_name'])

    def update_result(self, result):
            '''
            team_image_name
            runtime
            accuracy
            recall
            precision
            computed_scenes
            tag
            '''
            table = self.db[self.table_name]
            table.update(dict(team_image_name=result['team_image_name'],
                accuracy = result['accuracy'],
                recall = result['recall'],
                precision = result['precision'],
                runtime = result['runtime'],
                computed_scenes=result['computed_scenes'],
                tag=result['tag'],
                last_run=result['last_run'],
                updated='False',
            ), ['team_image_name'])
            # print("result updated for image ", result['team_image_name'])


    def find_images(self):
        table = self.db[self.table_name]
        images = {}
        for t in table.all():
                # print("entry ", t)
                if t['team_image_name']:
                    try:
                        docker_hub_link = t['team_image_name'].split('/')
                        if t['updated'] == 'True':
                            # print("Found updated entry: ", t)
                            # images.append(str(t['team_image_name']))
                            images[t['team_image_name']] = 'updated'
                        else:
                            images[t['team_image_name']] = 'old'
                    except IndexError:
                        print('Incorrectly specified image encountered. Format is {team_repo/team_image}')
                        continue
                else:
                    print('Team did not submitted image yet')
        sys.stdout.flush()
        return images

    def get_ranking(self):
        table = self.db[self.table_name].all()

        if not len(list(table)):
            print("ERROR! It this is the first run make sure that DB is initialized")
            return [], "", 0
        table = self.db[self.table_name].all()
        for team in table:
            try:
                team['accuracy']
            except KeyError:
                return self.db[self.table_name].all(), "", 0

        #precision problem in MySQL. reserver word
        # query = '''SELECT name, team_image_name,
        #             accuracy, recall,
        #             scenes, runtime,updated FROM %s ORDER BY accuracy DESC'''% self.table_name
        query = '''SELECT * FROM %s ORDER BY accuracy DESC'''% self.table_name
        ranking = self.db.query(query)
        query2 = '''SELECT last_run FROM %s WHERE last_run = (SELECT MAX(last_run) FROM %s)'''% (self.table_name,self.table_name)
        try:
            last_experiment_time = 0
            for row in self.db.query(query2):
                last_experiment_time = row['last_run']
            query3 = '''SELECT runtime FROM %s WHERE runtime = (SELECT MAX(runtime) FROM %s)'''% (self.table_name,self.table_name)
            max_runtime = 0
            for row in self.db.query(query3):
                max_runtime = row['runtime']

            return ranking, last_experiment_time, max_runtime

        except (pymysql.ProgrammingError, pymysql.err.ProgrammingError):
            print("If this is the first run make sure that DB is initialized")
            return [], "", 0
        except pymysql.InternalError as e:
            print(e)
            return ranking, datetime.datetime.utcnow(), 0
