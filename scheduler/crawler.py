import requests
import datetime

DOCKER_REGISTRY_V2 = 'https://hub.docker.com/v2/repositories'


class DockerCrawler:

    def __init__(self):
        self.table = []

    def convert_time(self, s):
        #myfmt = "%Y-%m-%d %H:%M"
        return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0).replace(second=0)

    def run(self, image):
        docker_hub_link = image.split('/')

        url = DOCKER_REGISTRY_V2 + '/%s/%s/tags/'%(docker_hub_link[0], docker_hub_link[1])
        #print("requesting: ", url)
        data = requests.get(url)
        if data.status_code >= 400:
            print("Can't access image. This image may be marked as private. Status: ", data.status_code)
            return
        last_updated = data.json()['results'][0]['last_updated']
        return self.convert_time(last_updated)
