from database_access_object import Database, connect_to_db
import subprocess
import json

db = connect_to_db("teams")
table = db['registrations']


def authenticate(username, password):
    user = db['registrations'].find_one(username=username, password=password)
    return user

def restart_scheduler(container_name):
    subprocess.check_output(['docker', 'restart', container_name])

def find_container_ip_addr(container_name):
    info = subprocess.check_output(['docker', 'inspect', container_name])
    # parsing nested json from docker inspect
    ip = list(json.loads(info.decode('utf-8'))[0]["NetworkSettings"]["Networks"].values())[0]["IPAddress"]
    print("%s container ip is: %s" % (container_name, ip))
    return ip

    # user = username_mapping.get(username, None)
    # if user and safe_str_cmp(user.password, password):
    #     return user
    # else:
    #     return None
def identity(payload):
    user_id = payload['identity']
    return db['registrations'].find_one(username=user_id)
