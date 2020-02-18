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
        raise ValueError('MySQL Environment Variables not set!')

class Teams:
    def __init__(self, table):
        self.table = table
        self.connect_to_db(self.table)

    def connect_to_db(self, table):
      self.db = connect_to_db(table) 

    def add_team(self, name, image, status):
        # Note! upsert wont work with 'name' field
        table = self.db[self.table]
        row = table.find_one(name=name)
        if row:
            # print("Found entry", row)
            table.update(dict(name=name, team_image_name=image, updated=status), ['name'])
        else:
            # print("Entry is new")
            table.insert(dict(name=name, team_image_name=image, updated=status))
            restart_scheduler(SCHEDULER)
        sys.stdout.flush()

    def update_image(self, image_name, timestamp):
        table = self.db[self.table]
        table.update(dict(team_image_name=image_name,  time_tag=timestamp, updated='True'), ['team_image_name'])

    def update_result(self, result):
            '''
            team_image_name
            total_runtime
            latency
            accuracy
            timeliness
            tag
            last_run
            '''
            table = self.db[self.table]
            table.update(dict(team_image_name=result['team_image_name'],
                total_runtime = result['accuracy'],
                latency = result['recall'],
                accuracy = result['precision'],
                timeliness = result['runtime'],
                tag=result['tag'],
                last_run=result['last_run'],
                updated='False',
            ), ['team_image_name'])
            print("Result updated for image ", result['team_image_name'])


    def find_images(self):
        table = self.db[self.table]
        images = {}
        for t in table.all():
                # print("entry ", t)
                if t['team_image_name']:
                    try:
                        docker_hub_link = t['team_image_name'].split('/')
                        if t['updated'] == 'True':
                            images[t['team_image_name']] = 'updated'
                        else:
                            images[t['team_image_name']] = 'old'
                    except IndexError:
                        print('Incorrectly specified image encountered. Format is {team_repo/team_image}')
                        continue
                else:
                    print('Team has not submitted image yet')
        sys.stdout.flush()
        return images

    def get_ranking(self):
        table = self.db[self.table].all()

        if not len(list(table)):
            print("ERROR! It this is the first run make sure that DB is initialized")
            return [], "", 0
        table = self.db[self.table].all()
        for team in table:
            try:
                team['accuracy']
            except KeyError:
                return self.db[self.table].all(), "", 0

        #precision problem in MySQL. reserver word
        # query = '''SELECT name, team_image_name,
        #             accuracy, recall,
        #             scenes, runtime,updated FROM %s ORDER BY accuracy DESC'''% self.table_name
        query = '''SELECT * FROM %s ORDER BY accuracy DESC'''% self.table
        ranking = self.db.query(query)
        query2 = '''SELECT last_run FROM %s WHERE last_run = (SELECT MAX(last_run) FROM %s)'''% (self.table,self.table)
        try:
            last_experiment_time = 0
            for row in self.db.query(query2):
                last_experiment_time = row['last_run']
            query3 = '''SELECT runtime FROM %s WHERE runtime = (SELECT MAX(runtime) FROM %s)'''% (self.table,self.table)
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
