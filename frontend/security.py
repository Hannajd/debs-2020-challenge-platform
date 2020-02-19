from database_access_object import Teams, connect_to_db
from werkzeug.security import safe_str_cmp
import subprocess
import json

REGISTRATIONS = connect_to_db("teams")['registrations']


def authenticate(username, password):
    user = REGISTRATIONS.find_one(username=username)
    if user and safe_str_cmp(user['password'].encode('utf-8'), password.encode('utf-8')):
        return user


def identity(payload):
    user_id = payload['identity']
    return REGISTRATIONS.find_one(username=user_id)


def restart_scheduler(container_name):
    subprocess.check_output(['docker', 'restart', container_name])


def find_container_ip_addr(container_name):
    info = subprocess.check_output(['docker', 'inspect', container_name])
    # parsing nested json from docker inspect
    ip = list(json.loads(info.decode('utf-8'))[0]["NetworkSettings"]["Networks"].values())[0]["IPAddress"]
    print("%s container ip is: %s", container_name, ip)
    return ip
