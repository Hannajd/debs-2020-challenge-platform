import os
import logging
import json
import re
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
            create_access_token, decode_token
)

from security import authenticate, identity, find_container_ip_addr
from database_access_object import Teams

# --- APP ---
app = Flask(__name__)

app.config['SECRET_KEY'] = os.getenv("SECRET_KEY")
if app.config['SECRET_KEY'] == 'UNDEFINED':
    raise ValueError('Please define SECRET_KEY!')

JWTManager(app) # Needed to create and validate JWT tokens 

# Init logging
LOG_FOLDER_NAME = "frontend_logs"
LOG_FILENAME = 'controller.log'
os.makedirs(LOG_FOLDER_NAME, exist_ok=True)
logger = logging.getLogger()
logging.basicConfig(
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(threadName)s -  %(levelname)s - %(message)s',
                    handlers=[
                     logging.FileHandler("%s/%s" % (LOG_FOLDER_NAME, LOG_FILENAME)),
                     logging.StreamHandler()
                    ])

# Init state
MIN_WAIT_TIME_SECONDS = 60
DELTA = datetime.timedelta(minutes=10) # average waiting time initial
CYCLE_TIME = datetime.timedelta(minutes=10)
UPDATE_TIME = datetime.datetime.utcnow()

SKIP_COLUMNS = ['image'] #TODO
SCORE_COLUMNS = ['total_runtime', 'latency', 'accuracy', 'timeliness']
MANAGER_URI = os.getenv("REMOTE_MANAGER_SERVER")
SCHEDULER_URI = find_container_ip_addr(os.getenv("SCHEDULER_IP"))
ALLOWED_HOSTS = [MANAGER_URI, SCHEDULER_URI]
SANITY_CHECK_FIELD = SCORE_COLUMNS[0]
logging.debug("Allowed hosts are: %s", ALLOWED_HOSTS)

# The table storing the info for each team, including image, latest scores, etc
TEAMS_DAO = Teams('teams')

# Dictionary of image -> status
# Describing the status of each image in the system
# image = string in the form "username/image_name" at DockerHub
TEAM_STATUS = {} 


def generate_ranking_table(result, last_run, time_to_wait):
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
    for rowIdx, row in enumerate(result):
        ranking[rowIdx+1] = get_ranking_fields(row)
        image = row.get('image', None)
        current_status = TEAM_STATUS.get(image, "") if image else ""
        if row.get('updated', None)  == "True" and time:
            queue.append({row['name']: {
                "eta": unconvert_time(time + DELTA*(marked_to_run)),
                "status": current_status
            }})
            marked_to_run += 1
    logging.info(ranking)
    return ranking, queue


def get_ranking_fields(row):
    new_row = {}
    for column, value in row.items():
        if column in SKIP_COLUMNS:
            continue
        elif column == 'last_run' and value:
            new_row[column] = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        else:
            new_row[column] = value
    return new_row


def update_waiting_time(seconds):
    global DELTA
    if seconds >= MIN_WAIT_TIME_SECONDS:
        DELTA = datetime.timedelta(minutes=seconds/60)


def round_time(tm):
    return tm - datetime.timedelta(minutes=tm.minute % 10,
                             seconds=tm.second,
                             microseconds=tm.microsecond)

def unconvert_time(s):
    return s.strftime("%Y-%m-%d %H:%M:%S")


# --- ROUTES ----
@app.route('/result', methods=['POST'])
def post_result():
    global CYCLE_TIME
    if (request.remote_addr in ALLOWED_HOSTS) or request.remote_addr.startswith( '172', 0, 4 ):
        jsonData = request.json
        team = jsonData.get('image')
        TEAM_STATUS[team] = '' # Clear team status
        logging.info("Received result: %s", jsonData)
        if not jsonData.get(SANITY_CHECK_FIELD, None):
            return jsonify({"message":"Bad request"}), 400
        loop_time = jsonData.get('piggybacked_manager_timeout', CYCLE_TIME)
        CYCLE_TIME = datetime.timedelta(seconds=loop_time)
        # update database
        TEAMS_DAO.update_result(jsonData)
        return json.dumps(jsonData), 200
    else:
        logging.warning("Host '%s' is NOT allowed to post results", request.remote_addr)
        return {"message": "Host not allowed"}, 403


@app.route('/', methods=['GET'])
def index():
    logging.debug("/ route requested by IP address: %s " % request.remote_addr)
    query, last_experiment_time, waiting_time = TEAMS_DAO.get_ranking()
    ranking, queue = generate_ranking_table(query, last_experiment_time, waiting_time)
    return render_template('table.html', post=ranking, team=queue)


@app.route('/score/<image_namespace>/<image_name>', methods=['GET'])
def team_score(image_namespace, image_name):
    image = image_namespace + '/' + image_name
    logging.debug("/score/%s route requested by IP address: %s ", image, request.remote_addr)
    teamScore = TEAMS_DAO.get_team_data(image, SCORE_COLUMNS)
    return jsonify(teamScore)

@app.route('/status_update', methods=['GET', 'POST'])
def status():
    if (request.remote_addr in ALLOWED_HOSTS) or request.remote_addr.startswith( '172', 0, 4):
        if request.method == 'GET':
            return jsonify(TEAM_STATUS), 200
        if request.method == 'POST':
            for team, status in request.json.items():
                TEAM_STATUS[team] = status
            return jsonify(TEAM_STATUS), 200
    else:
        logging.warning(" %s is NOT allowed to post schedule" % request.remote_addr)
        return abort(403)


@app.route('/add_team', methods=['GET', 'POST'])
def add_teams():
    global UPDATE_TIME
    access_token = session.get('access_token')

    if not access_token:
        return redirect(url_for('login'))

    # Validate credentials
    try:
        payload = decode_token(access_token)
        if not identity(payload):
            # Identification failed
            raise ValueError('Invalid identity!')
    except:
        logging.warn("Invalid access token at /add_team from IP: %s", request.remote_addr)
        return render_template('404.html'), 404

    if request.method == 'GET':
        return render_template('team_form.html')
    if request.method == 'POST':
        team = request.values.get('name')
        image = request.values.get('image')
        updated = str(bool(request.values.get('updated'))) # 'True' if checkbox set, otherwise 'False'
        logging.info("Request to add team %s with image %s and status %s", team, image, updated)
        # Validate image
        if not image:
            return {"message": "No image name provided"}, 500
        if not re.match(r'.+/.+', image):
            return {"message": "Incorrect image format!"}, 500
        try:
            TEAMS_DAO.add_team(team, image, updated)
            logging.info("Added team %s", team)
        except Exception as e:
            logging.error("Failed to add team %s with image %s and status %s: %s", team, image, updated, e)
            return {"message": "Failed to add team!"}, 500 
        UPDATE_TIME = datetime.datetime.utcnow()
        return render_template('success.html'), 200


@app.route('/login', methods=['GET', "POST"])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    elif request.method == 'POST':
        username = request.values.get('username')
        password = request.values.get('password')
        user = authenticate(username, password)
        if not user:
            logging.warn('Failed login attempt from IP: %s', request.remote_addr)
            return render_template('404.html'), 404
        access_token = create_access_token(identity=username, fresh=False)
        response = redirect(url_for('add_teams'))
        session['access_token'] = access_token
        response.headers['Authorization'] = 'Bearer {}'.format(access_token)
        response.method = 'GET'
        response.json = jsonify(data={"access_token": access_token})
        return response


@app.route('/schedule', methods=['POST'])
def post_schedule():

    if (request.remote_addr in ALLOWED_HOSTS) or request.remote_addr.startswith( '172', 0, 4 ):
        logging.debug("%s is allowed to post schedule", request.remote_addr)
        data = request.json
        logging.info("Received updated schedule")
        logging.debug("received data: %s" % data)
        if not data:
            return jsonify({"message":"Bad request"}), 400
        for image, timestamp in data.items():
            TEAMS_DAO.update_image(image, timestamp)
            logging.debug("image entry %s updated at:  %s" % (image, timestamp))

        return json.dumps(request.json), 200
    else:
        logging.warning(" %s is NOT allowed to post schedule" % request.remote_addr)
        return {"message":"Host not allowed"}, 403


@app.route('/schedule', methods=['GET'])
def get_teams():
    if (request.remote_addr in ALLOWED_HOSTS) or request.remote_addr.startswith( '172', 0, 4 ):
        images = TEAMS_DAO.get_image_statuses()
        logging.info("sending schedule %s to component: %s" % (images, request.remote_addr))
        return json.dumps(images)
    else:
        logging.warning(" %s is NOT allowed to request schedule" % request.remote_addr)
        return abort(403)


@app.before_request
def make_session_permanent():
    session.permanent = True
    session_timeout_seconds = int(os.getenv("FLASK_SESSION_TIMEOUT_SECONDS", default=60))
    app.permanent_session_lifetime = datetime.timedelta(seconds=session_timeout_seconds)


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
