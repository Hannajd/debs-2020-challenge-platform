import requests
import datetime
import logging


DOCKER_REGISTRY_V2 = 'https://hub.docker.com/v2/repositories'


class DockerCrawler:

    def get_last_update_timestamp(self, image):
        docker_hub_link = image.split('/')
        url = DOCKER_REGISTRY_V2 + '/%s/%s/tags/' % (docker_hub_link[0], docker_hub_link[1])
        logging.info('Retrieving image data: %s', url)
        try:
            data = requests.get(url)
            logging.info('Status Code: %d', data.status_code)
            logging.debug('Image data: %s', data.json())
            if data.status_code != 200:
                raise Exception('Invalid status code! Is the image public?')
            last_updated = data.json()['results'][0]['last_updated']
            return self.convert_time(last_updated)
        except:
            logging.error('Failed to access image!')
            return

    def convert_time(self, s):
        return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0).replace(second=0)
