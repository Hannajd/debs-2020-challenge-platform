import pymysql
pymysql.install_as_MySQLdb()
import dataset
import os
import sys
import datetime
import subprocess
import logging

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
            logging.info('Updating entry for team %s' % name)
            table.update(dict(name=name, image=image, updated=status), ['name'])
        else:
            logging.info('Inserting new entry for team %s' % name)
            table.insert(dict(name=name, image=image, updated=status))
            restart_scheduler(SCHEDULER_SERVICE_NAME)

    def update_image(self, image_name, timestamp):
        table = self.db[self.table]
        table.update(dict(image=image_name,  time_tag=timestamp, updated='True'), ['image'])

    def update_result(self, result):
            '''
            image
            --- Score ---
            total_runtime
            latency
            accuracy
            timeliness
            --- Metadata ---
            benchmark_runtime
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
                benchmark_runtime=result['benchmark_runtime'],
                updated=str(False),
            ), ['image'])
            logging.info("Result updated for image %s", result['image'])

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
                    logging.warn('Igoring image with incorrect format: "%s". Format is {team_repo/team_image}.' % image)
                    continue
                # Populate dict
                if row['updated'] == 'True':
                    images[image] = 'updated'
                else:
                    images[image] = 'old'
            else:
                logging.info('Ignoring team "%s": no image specified', row['name'])
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
        teams_unranked = self.db[self.table].all()
        failover_ranking = (teams_unranked, "", 0)
        if not self.verify_schema(teams_unranked, 'total_runtime'):
            logging.error("Database schema verification failed! It this is the first run make sure that DB is initialized")
            return failover_ranking

        ranking_query = '''SELECT *, (R.rank_total_runtime+R.rank_latency+R.rank_timeliness+R.rank_accuracy) as total_rank FROM 
            %s AS T INNER JOIN 
                (SELECT id, 
                dense_rank() OVER (ORDER BY IFNULL(total_runtime, 1E10) ASC) AS rank_total_runtime,
                dense_rank() OVER (ORDER BY IFNULL(latency, 1E10) ASC) AS rank_latency,
                dense_rank() OVER (ORDER BY IFNULL(timeliness, 0) DESC) AS rank_timeliness,
                dense_rank() OVER (ORDER BY IFNULL(latency, 1E10) ASC) AS rank_accuracy
                FROM %s) AS R
                ON T.id = R.id
            ORDER BY total_rank ASC
                ''' % (self.table, self.table)
        last_experiment_time_query = 'SELECT MAX(last_run) AS result FROM %s' % self.table
        max_runtime_query = 'SELECT MAX(benchmark_runtime) AS result FROM %s' % self.table
        try:
            ranking = self.db.query(ranking_query)
        except Exception as e:
            logging.error("Failed to retrieve rankings. If this is the first run make sure that DB is initialized: %s", e)
            return failover_ranking
        last_experiment_time = self.single_result_or_default(last_experiment_time_query, 'result', 0)
        max_runtime = self.single_result_or_default(max_runtime_query, 'result', 0)
        return (ranking, last_experiment_time, max_runtime)

    def verify_schema(self, table, test_column):
        if not list(table):
            return False
        for team in table:
            try:
                team[test_column]
            except KeyError:
                return False
        return True

    def single_result_or_default(self, query, column, default, *_clauses, **kwargs):
        result = default
        try:
            rows = self.db.query(query, *_clauses, **kwargs)
            for i, row in enumerate(rows):
                result = row[column]
            if i > 1:
                raise ValueError('query "%s" returned multiple results!')
            return result
        except Exception as e:
            logging.error("Failed to execute '%s' metadata: %s", query, e)
            return default
