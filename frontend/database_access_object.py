import pymysql
pymysql.install_as_MySQLdb()
import dataset
import os
import sys
import datetime
import subprocess

# scheduler service name for restarting upon new entry
SCHEDULER_SERVICE_NAME = "scheduler"

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
        table = self.db[self.table]
        row = table.find_one(name=name)
        if row:
            print('Updating entry for team %s' % name)
            table.update(dict(name=name, image=image, updated=status), ['name'])
        else:
            print('Inserting new entry for team %s' % name)
            table.insert(dict(name=name, image=image, updated=status))
            restart_scheduler(SCHEDULER_SERVICE_NAME)

    def update_image(self, image_name, timestamp):
        table = self.db[self.table]
        table.update(dict(image=image_name,  time_tag=timestamp, updated='True'), ['image'])

    def update_result(self, result):
            '''
            image
            total_runtime
            latency
            accuracy
            timeliness
            tag
            last_run
            '''
            table = self.db[self.table]
            table.update(dict(image=result['image'],
                total_runtime = result['total_runtime'],
                latency = result['latency'],
                accuracy = result['accuracy'],
                timeliness = result['timeliness'],
                tag=result['tag'],
                last_run=result['last_run'],
                updated=str(False),
            ), ['image'])
            print("Result updated for image ", result['image'])

    def get_image_statuses(self):
        '''Return dictionary of image -> status (updated/old) for all images in DB
        '''
        table = self.db[self.table]
        images = {}
        for row in table.all():
            image = row['image']
            if image:
                # Verify image format
                try:
                    image.split('/')
                except IndexError:
                    print('Igoring image with incorrect format: "%s". Format is {team_repo/team_image}.' % image)
                    continue
                # Populate dict
                if row['updated'] == 'True':
                    images[image] = 'updated'
                else:
                    images[image] = 'old'
            else:
                print('Ignoring team "%s": no image specified' % row['name'])
        return images

    def get_team_data(self, image, columns):
        table = self.db[self.table] 
        teamData = table.find_one(image=image)
        if not teamData:
            return {}
        return {k: v for (k, v) in teamData.items() if k in columns} 

    def get_ranking(self):
        '''Return a tuple (table, last_experiment_time, waiting_time), 
        where "table" is the team entries sorted on their score
        '''
        #TODO: Reimplement
        return self.db[self.table].all(), "", 0
        table = self.db[self.table].all()
        if not list(table):
            print("Failed to retrieve rankings! It this is the first run make sure that DB is initialized")
            return [], "", 0
        table = self.db[self.table].all()
        for team in table:
            #FIXME
            try:
                team['total_runtime']
            except KeyError:
                return self.db[self.table].all(), "", 0

        #precision problem in MySQL. reserver word
        # query = '''SELECT name, image,
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
