import os
import logging
import json
from flask import (
        Flask, jsonify,
        render_template, request, redirect, url_for, session, abort
        )
import time
import sys
#from textwrap import dedent
import datetime
from flask_jwt_extended import JWTManager
from flask_jwt_extended import (
            create_access_token, create_refresh_token, get_jwt_identity, jwt_required, decode_token
)

from security import authenticate, find_container_ip_addr
from database_access_object import Database

# --- APP ---
app = Flask(__name__)
# app.secret_key = 'super-secret'
app.config['SECRET_KEY'] = os.getenv("SECRET_APP_KEY")
# app.config['JWT_TOKEN_LOCATION'] = ['json']
jwt = JWTManager(app)
# ------

LOG_FOLDER_NAME = "frontend_logs"
if not os.path.exists(LOG_FOLDER_NAME):
    os.makedirs(LOG_FOLDER_NAME)
filename = 'controller.log'
logger = logging.getLogger()
logging.basicConfig(
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s',
                    handlers=[
                     logging.FileHandler("%s/%s" % (LOG_FOLDER_NAME, filename)),
                     #logging.StreamHandler()
                    ])
formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# --- constants ---
DELTA = datetime.timedelta(minutes=10) # average waiting time initial
CYCLE_TIME = datetime.timedelta(minutes=10)
UPDATE_TIME = datetime.datetime.utcnow()

skip_columns = ['team_image_name']
# --- Allowed HOSTS (scheduler container will be detected at runtime)
remote_manager = os.getenv("REMOTE_MANAGER_SERVER").split(",")
allowed_hosts = remote_manager
# --- find scheduler ---
scheduler_ip = find_container_ip_addr(os.getenv("SCHEDULER_IP"))
allowed_hosts.append(scheduler_ip)
logging.debug("Allowed hosts are: %s" % allowed_hosts)

team_status = {}
# --- helper functions ---
def update_waiting_time(seconds):
    global DELTA
    if seconds >= 60:  # minimum waiting time in minutes
        DELTA = datetime.timedelta(minutes=seconds/60)


def filter(row):
    # specify in @skip_columns which columns not to be shown
    new_row = {}
    for k, v in row.items():
        if k in skip_columns:
            continue
        elif k == 'last_run':
            if v:
                new_row[k] = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
        else:
            new_row[k] = v
    # print("new row: ", new_row)
    return new_row


def generate_ranking_table(result, last_run, time_to_wait):
    global DELTA, CYCLE_TIME, UPDATE_TIME
    ranking = {}
    queue = []
    time = 0
    update_waiting_time(time_to_wait)
    if not result:
        return ranking, queue
    if last_run:
        last_run = datetime.datetime.strptime(last_run, '%Y-%m-%dT%H:%M:%S')
        last_run = max(last_run, UPDATE_TIME)
        print("Max date is %s" % last_run)
        time = last_run + DELTA + CYCLE_TIME
    else:
        time = UPDATE_TIME + DELTA + CYCLE_TIME
    marked_to_run = 0
    time_to_wait = 0
    for ix, row in enumerate(result):
        # print("ROW: ", row)
        ranking[ix+1] = filter(row)
        team_name = row.get('team_image_name', None)
        current_status = ""
        if team_name:
            current_status = team_status.get(team_name, "")
        if row.get('updated', None)  == "True":
            if time:
               queue.append({row['name']: {
                    "eta": unconvert_time(time + DELTA*(marked_to_run)),
                    "status": current_status
               }})
               marked_to_run +=1
    sys.stdout.flush()
    #print(ranking)
    return ranking, queue


def round_time(tm):
    return tm - datetime.timedelta(minutes=tm.minute % 10,
                             seconds=tm.second,
                             microseconds=tm.microsecond)

def unconvert_time(s):
    return s.strftime("%Y-%m-%d %H:%M:%S")

# --- ROUTES ----
@app.route('/result', methods=['POST'])
def post_result():
    if (request.remote_addr in allowed_hosts) or request.remote_addr.startswith( '172', 0, 4 ):
        data = request.json
        team = data.get('team_image_name')
        team_in_schedule = team_status.get(team, None)
        if team_in_schedule:
            team_status[team_in_schedule] = ""

        logging.info("received new result: %s" % data)
        sys.stdout.flush()
        accuracy = data.get('accuracy')
        if not accuracy:
            return jsonify({"message":"Bad request"}), 400
        global CYCLE_TIME
        loop_time = data.get('piggybacked_manager_timeout', CYCLE_TIME)
        CYCLE_TIME = datetime.timedelta(seconds=loop_time)
        # update database
        # set all to False
        db.update_result(data)
        return json.dumps(request.json), 200
    else:
        logging.warning(" %s is allowed NOT to post results" % request.remote_addr)
        return {"message":"Host not allowed"}, 403


@app.route('/', methods=['GET'])
def index():
    logging.debug("INDEX route requested by IP address: %s " % request.remote_addr)

    query, last_experiment_time, waiting_time = db.get_ranking()
    # logging.debug("Query: %s, last_experiment_time: %s, wait: %s" % (query, last_experiment_time, waiting_time))
    ranking, queue = generate_ranking_table(query, last_experiment_time, waiting_time)
    # logging.debug("Rankng: %s, Queue: %s" % (ranking,queue))
    return render_template('table.html', post=ranking, team=queue)

@app.route('/status_update', methods=['GET', 'POST'])
def status():
    STATUS_FIELD = 'status_update'
    if (request.remote_addr in allowed_hosts) or request.remote_addr.startswith( '172', 0, 4):
        if request.method == 'GET':
            # testing
            return jsonify(team_status), 200
        if request.method == 'POST':
            data = request.json
            for team, status in data.items():
                team_status[team] = status
            return jsonify(team_status), 200
            # status = data.get(STATUS_FIELD)
            # team = data.get('team_image_name')
            # logging.info("got %s %s" % (team, status))
            # if team:
            #     team_status[team] = status
            #     return jsonify(team_status), 200
            # else:
            #     return {"message":"Bad request"}, 400

    else:
        logging.warning(" %s is NOT allowed to post schedule" % request.remote_addr)
        return abort(403)


@app.route('/add_team', methods=['GET', 'POST'])
#@jwt_required
def add_teams():
    global UPDATE_TIME
    # print(request.json)
    if session.get('access_token'):
        current_user = get_jwt_identity()
        decoded = decode_token(session['access_token'])
        # can additionally decode token to verify
        # if not identity(decoded):
        #     return render_template('404.html'), 404
        if request.method == 'GET':
            return render_template('team_form.html')
        if request.method == 'POST':
            team = request.values.get('name', None) # Your form's
            image = request.values.get('team_image_name', None) # input names
            updated = request.values.get('updated')
            logging.info("Requested to add team %s with image %s and status: %s" % (team, image, updated))
            if not updated:
                updated = "False"
            else:
                updated = "True" # default form value is 'on'
            try:
                if not image:
                    return {"message": "Please provide image name"}, 500
                image.split('/')[1]
            except IndexError:
                return {"message": "Image name specified incorrectly"}, 500
            #session['access_token'] = ""
            db.add_team(team, image, updated)
            logging.info("Added team %s with image %s and status: %s" % (team, image, updated))
            UPDATE_TIME = datetime.datetime.utcnow()
            #response.json = jsonify({"Updated": {team:image +" "+ str(updated)}})
            return render_template('success.html'), 200
    else:
        return render_template('404.html'), 404


@app.route('/login', methods=['GET', "POST"])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    elif request.method == 'POST':
        username = request.values.get('username', None) # Your form's
        password = request.values.get('password', None) # input names
        #print("U %s P %s " % (username, password))
        user = authenticate(username, password)
        sys.stdout.flush()
        if not user:
            return render_template('404.html'), 404
        access_token = create_access_token(identity=username, fresh=False)
        response = redirect(url_for('add_teams'))
        #print("TOKEN:", access_token)
        session['access_token'] = access_token
        headers = {
        'Authorization': 'Bearer {}'.format(access_token),
        "Content-Type": 'application/json'
        }
        response.headers['Authorization'] = 'Bearer {}'.format(access_token)
        response.method = 'GET'
        response.json = jsonify(data={"access_token":access_token})
        return response


@app.route('/schedule', methods=['POST'])
def post_schedule():

    if (request.remote_addr in allowed_hosts) or request.remote_addr.startswith( '172', 0, 4 ):
        logging.debug(" %s is allowed to post schedule" % request.remote_addr)
        data = request.json
        logging.info("Received updated schedule")
        logging.debug("received data: %s" % data)
        if not data:
            return jsonify({"message":"Bad request"}), 400
        for image, timestamp in data.items():
            db.update_image(image, timestamp)
            logging.debug("image entry %s updated at:  %s" % (image, timestamp))

        return json.dumps(request.json), 200
    else:
        logging.warning(" %s is NOT allowed to post schedule" % request.remote_addr)
        return {"message":"Host not allowed"}, 403


@app.route('/schedule', methods=['GET'])
def get_teams():
    # logging.info("IP address: %s " % request.remote_addr)
    # sys.stdout.flush()
    if (request.remote_addr in allowed_hosts) or request.remote_addr.startswith( '172', 0, 4 ):
        sys.stdout.flush()
        images = db.find_images()
        logging.info("sending schedule %s to component: %s" % (images, request.remote_addr))
        return json.dumps(images)
    else:
        logging.warning(" %s is NOT allowed to request schedule" % request.remote_addr)
        return abort(403)


# --- DB Access ---
db = Database('teams')

@app.before_request
def make_session_permanent():
    session.permanent = True
    sess_timeout = int(os.getenv("SESSION_TIMEOUT", default=60))
    app.permanent_session_lifetime = datetime.timedelta(seconds=sess_timeout)


if __name__ == '__main__':
    frontend_backoff = int(os.getenv("FRONTEND_STARTUP_BACKOFF", default=40))
    logging.warning("Waiting for DB server to start: %s seconds" % frontend_backoff)
    time.sleep(frontend_backoff)

    '''
        Use CMD in Dockerfile for production deployment:
            gunicorn -b 0.0.0.0:8080 controller:app
        or run locally with:
            app.run(host='0.0.0.0', port=8080)
    '''
