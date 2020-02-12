import logging
import os
import json
import datetime
from crawler import DockerCrawler
import time
import requests

#HOST = "http://127.0.0.1:8080"
PATH = '/schedule'

LOG_FOLDER_NAME = "scheduler_logs"
if not os.path.exists(LOG_FOLDER_NAME):
    os.makedirs(LOG_FOLDER_NAME)
filename = 'scheduler.log'
logging.basicConfig(
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s',
                    handlers=[
                     logging.FileHandler("%s/%s" % (LOG_FOLDER_NAME, filename)),
                     logging.StreamHandler()
                    ])
# logger = logging.getLogger()

endpoint = os.getenv("FRONTEND_SERVER")
if endpoint is None:
    logging.error("please specify front-end server address!")
    #exit(1)
    #endpoint = HOST + PATH
else:
    endpoint = "http://" + endpoint + PATH


class Scheduler:

    def __init__(self):

        json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime.datetime) else None)
        self.schedule = {}
        try:
            self.schedule, _ = self.reguest_all_images()
        except requests.exceptions.ConnectionError as e:
            logging.error("Make sure that server you are trying to connect is up. %s" % e)
            logging.error("Please restart Scheduler when the frontend server is up!")
            logging.error("Hint! You can use 'docker restart scheduler' command.")
            exit(1)
        self.last_updated_images = {} #snapshot
        self.crawler = DockerCrawler()

    # requests only at startup
    # then contains in memory shapshot of all teams and last image runs
    def reguest_all_images(self):
        response = requests.get(endpoint)
        logging.info(response.status_code)
        schedule = response.json()
        logging.info("Requested image list is: %s " % schedule)
        return schedule, response.status_code

    def run(self):
        self.updated_status = False
        # logging.info(self.schedule)
        for image, status in self.schedule.items():
                #logging.info('updated: %s' % image)
                old_timestamp = self.last_updated_images.get(image)
                new_timestamp = self.crawler.run(image)
                if old_timestamp == new_timestamp:
                    logging.debug('same: %s' % image)
                    # self.updated_status = False
                    if self.schedule.get(image) != 'old' and not None:
                        self.schedule[image] = 'old'
                elif old_timestamp is None and status == 'old':
                    # do nothing, only save timestamp as current one
                    logging.info("all images are same")
                    self.last_updated_images[image] = new_timestamp
                    self.updated_status = True
                else:
                    logging.info('New tag for image %s detected at %s' % (image, new_timestamp))
                    self.last_updated_images[image] = new_timestamp
                    self.updated_status = True
                    self.schedule[image] = 'updated'
        logging.info("all teams checked")


def send_schedule(payload):
    headers = {'Content-type': 'application/json'}
    try:
        response = requests.post(endpoint, json = payload, headers=headers)

        logging.info('Response status is: %s' % response.status_code)
        if (response.status_code == 201):
            return {'status': 'success', 'message': 'updated'}
        if (response.status_code == 404):
            return {'message': 'Something went wrong. No scene exist. Check if the path is correct'}
    except requests.exceptions.ConnectionError as e:
        logging.error("please specify front-end server address! %s" % e)
        exit(1)
    return response.status_code


if __name__ == '__main__':
    logging.info("Waiting for DB server to start")
    logging.info("Waiting for the backend server to start")
    backoff = int(os.getenv("SCHEDULER_STARTUP_BACKOFF", default=30))
    frontend_backoff = int(os.getenv("FRONTEND_STARTUP_BACKOFF", default=0))
    if backoff <= frontend_backoff:
        logging.debug("Sheduler should start after the frontend server. Adding small backoff")
        backoff = frontend_backoff + 15
    time.sleep(backoff)

    scheduler = Scheduler()
    # make crawling request window non-uniform
    wait_seconds = int(os.getenv("SCHEDULER_SLEEP_TIME", default=60))
    # wait_upper_bound = wait_seconds*10
    # wait_lower_bound = wait_seconds

    while(True):
        scheduler.run()
        updated_images = {}
        # logging.info("Scheduler items are: %s" % scheduler.schedule)
        if scheduler.updated_status:
            for image, status in scheduler.schedule.items():
                    # logging.info("Status: %s " % status)
                    if str(status) == 'updated':
                        updated_images[image] = scheduler.last_updated_images[image]
            # wait_seconds = wait_lower_bound

        if updated_images:
            logging.info("Scheduler sends updated images: " % updated_images)
            send_schedule(updated_images)
            scheduler.updated_status = False
        else:
            logging.info("Images weren't updated yet. Idling...")
            # wait_seconds += 10
            # if wait_seconds > wait_upper_bound:
            #     wait_seconds = wait_lower_bound

        time.sleep(wait_seconds)


# requests.exceptions.SSLError:
# HTTPSConnectionPool(host='hub.docker.com', port=443):
# Max retries exceeded with url: /v2/repositories/olehbodunov/sleepy_client/tags/
# (Caused by SSLError(SSLEOFError(8, u'EOF occurred in violation of protocol (_ssl.c:590)'),))
