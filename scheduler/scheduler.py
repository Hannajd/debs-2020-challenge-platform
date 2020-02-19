import logging
import os
import json
import datetime
from crawler import DockerCrawler
import time
import requests

SCHEDULE_ENDPOINT = '/schedule'
SCHEDULING_FREQUENCY_SECONDS = int(os.getenv("SCHEDULER_SLEEP_TIME", default=60))
LOG_FOLDER = "scheduler_logs"
LOG_FILE = 'scheduler.log'
os.makedirs(LOG_FOLDER, exist_ok=True)
logging.basicConfig(
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s',
                    handlers=[
                     logging.FileHandler("%s/%s" % (LOG_FOLDER, LOG_FILE)),
                     logging.StreamHandler()
                    ])

FRONTEND_ENDPOINT = os.getenv("FRONTEND_SERVER")
if not FRONTEND_ENDPOINT:
    raise ValueError("Please specify FRONTEND_SERVER environment variable!")
else:
    FRONTEND_ENDPOINT = "http://" + FRONTEND_ENDPOINT + SCHEDULE_ENDPOINT


class Scheduler:

    def __init__(self):
        '''Initialize and retrieve all images from frontend server (controller)
        After that, the cached images are used for scheduling. 
        In case a new image is added, the scheduler needs to be restarted by the controller!
        '''
        json.JSONEncoder.default = lambda self,obj: (obj.isoformat() if isinstance(obj, datetime.datetime) else None)
        self.schedule = {}
        try:
            self.schedule, _ = self.reguest_all_images()
        except requests.exceptions.ConnectionError as e:
            logging.error("Make sure that server you are trying to connect is up. %s" % e)
            logging.error("Please start the scheduler again when the frontend server is up!")
            logging.error("You can do this by executing 'docker restart scheduler'.")
            exit(1)
        self.last_updated_images = {} #snapshot
        self.crawler = DockerCrawler()

    def reguest_all_images(self):
        '''Request all the images from the frontend server
        '''
        response = requests.get(FRONTEND_ENDPOINT)
        logging.info(response.status_code)
        schedule = response.json()
        logging.info("Requested image list is: %s " % schedule)
        return schedule, response.status_code

    def run(self):
        self.updated_status = False
        for image, status in self.schedule.items():
                old_timestamp = self.last_updated_images.get(image)
                new_timestamp = self.crawler.get_last_update_timestamp(image)
                if old_timestamp == new_timestamp:
                    # Image not updated
                    logging.debug('Image has not been updated: %s', image)
                    self.schedule[image] = 'old'
                elif old_timestamp is None and status == 'old':
                    # old timestamp missing
                    # do nothing, only save timestamp as current one
                    logging.info("all images are same")
                    self.last_updated_images[image] = new_timestamp
                    self.updated_status = True
                else:
                    # Image updated
                    logging.info('New tag for image %s detected at %s', image, new_timestamp)
                    self.last_updated_images[image] = new_timestamp
                    self.updated_status = True
                    self.schedule[image] = 'updated'
        logging.info("All team images checked")


def post_schedule(payload):
    headers = {'Content-type': 'application/json'}
    try:
        response = requests.post(FRONTEND_ENDPOINT, json = payload, headers=headers)
        logging.info('Finished sending image schedule. Response: %s' % response.status_code)
        if (response.status_code == 201):
            return {'status': 'success', 'message': 'updated'}
        if (response.status_code == 404):
            return {'message': 'Something went wrong!'}
    except requests.exceptions.ConnectionError as e:
        logging.error("Please specify Frontend server address! %s", e)
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
    while(True):
        scheduler.run()
        updated_images = {}
        if scheduler.updated_status:
            for image, status in scheduler.schedule.items():
                    if str(status) == 'updated':
                        updated_images[image] = scheduler.last_updated_images[image]

        if updated_images:
            logging.info("Scheduler sending updated images: %s", updated_images)
            post_schedule(updated_images)
            scheduler.updated_status = False
        else:
            logging.info("Images weren't updated yet. Idling...")

        time.sleep(SCHEDULING_FREQUENCY_SECONDS)
