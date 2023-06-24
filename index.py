# fmt: off
from datetime import datetime, timezone
from flask import Flask, request, Response
from flask_cors import CORS
import pymongo
import dotenv
from os import environ
from pywebpush import webpush, WebPushException
import json
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
import hashlib
from functools import wraps
from lib.markdown_image_extractor import getMarkdownImage

app = Flask(__name__)
app.url_map.strict_slashes = False
CORS(app)

dotenv.load_dotenv()
MONGODB_SECRET = environ.get('MONGODB_SECRET')
WEBPUSH_PRIVATE_KEY = environ.get('WEBPUSH_PRIVATE_KEY')
GENERIC_HOOK_KEY_HASH = environ.get('GENERIC_HOOK_KEY_HASH')

client = pymongo.MongoClient(MONGODB_SECRET)
db = client.webpush
subscribersCollection = db.subscriptions
notificationsCollection = db.notifications

def json_response(status, message=None, data=None):
	d = {}
	if 100 <= status <= 399: d['type'] = 'success'
	elif 400 <= status <= 599: d['type'] = 'error'
	if data: d.update(data)
	if message: d['message'] = message
	return Response(json.dumps(d), status=status, mimetype='application/json')

def success_json(message=None, data=None):
	return json_response(200, message, data)
def error_json(message=None, data=None):
	return json_response(400, message, data)

def sha256(s):
	hash = hashlib.sha256()
	hash.update(s.encode('utf-8'))
	return hash.hexdigest()

def unixTimeMs(time: datetime):
	return int(time.replace(tzinfo=timezone.utc).timestamp() * 1000)

def makeNotification(title: str, body: str, options = {}):
	notificationId = str(uuid4())
	createdTime = datetime.utcnow() # will become a time in db
	notification = {
		'_id': notificationId,
		'createdTime': createdTime,
		'data': {
			'title': title,
			'body': body,
			'options': options,
		}
	}
	notificationsCollection.insert_one(notification)
	return notification

def sendNotification(notification):
	subscribers = subscribersCollection.find({})
	executor = ThreadPoolExecutor(max_workers=75)
	def worker(subscriber):
		try:
			print("Sending notification to {}".format(subscriber['_id']))
			webpush(
				subscription_info=subscriber['subscription_info'],
				data=json.dumps({"type": "notification", "id": notification['_id'], "time": unixTimeMs(notification['createdTime']), "data": notification['data']}),
				vapid_private_key=WEBPUSH_PRIVATE_KEY,
				vapid_claims={
					'sub': 'mailto:vapid_claims@4hcomputers.club',
				}
			)
		except Exception as e:
			print(e.args)
			subscribersCollection.update_one({"_id": subscriber['_id']}, {"$push": {"failed": notification['_id']}})
	attempted = []
	for subscriber in subscribers:
		executor.submit(worker, subscriber)
		# worker(subscriber)
		attempted.append(subscriber['_id'])
	notificationsCollection.update_one({"_id": notification['_id']}, {"$push": {"attempted": {"$each": attempted}}})

def require_api_key(f):
	@wraps(f)
	def wrapper(*args, **kwargs):
		if 'api-key' not in request.headers:
			return error_json('No API key provided')
		apiKey = request.headers.get('api-key')
		if (sha256(apiKey) != GENERIC_HOOK_KEY_HASH):
			return error_json('Invalid API key')
		return f(*args, **kwargs)
	return wrapper

@app.route('/')
def index():
	return 'hi'

@app.route('/hooks/notification/generic', methods=['POST'])
@require_api_key
def genericNotification():
	data = request.get_json()
	notification = makeNotification(data['title'], data['body'])
	sendNotification(notification)
	return success_json(data=notification['data'])

@app.route('/hooks/notification/contentful', methods=['POST'])
@require_api_key
def contentfulNotification():
	data = request.get_json()
	print(data)
	if data['sys']['revision'] != 1:
		return error_json('not the first revision')
	img = getMarkdownImage(data['fields']['contentText']['en-US'])
	notification = makeNotification('4-H Fair: New post', data['fields']['title']['en-US'], {
		'image': img,
		'timestamp': unixTimeMs(datetime.fromisoformat(data['sys']['updatedAt'][:-1])),
	})
	sendNotification(notification)
	print(notification)
	return success_json(data=notification['data'])

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)
