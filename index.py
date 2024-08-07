# fmt: off
from datetime import datetime
from time import mktime
from flask import Flask, request, Response
from flask_cors import CORS
import pymongo
from bson import ObjectId
import dotenv
from os import environ
from pywebpush import webpush
import json
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
import hashlib
from functools import wraps
from lib.markdown_image_extractor import getMarkdownImage
from common import unixTimeMs

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

def sendOneNotification(subscriber, notification):
	try:
		print("Sending notification to {}".format(subscriber['_id']))
		webpush(
			subscription_info=subscriber['subscription_info'],
			data=json.dumps({
				"type": "notification",
				"id": notification['_id'],
				"time": unixTimeMs(notification['createdTime']),
				"data": notification['data'],
			}),
			vapid_private_key=WEBPUSH_PRIVATE_KEY,
			vapid_claims={'sub': 'mailto:vapid_claims@4hcomputers.club'},
			# fix for windows (https://learn.microsoft.com/en-us/windows/apps/design/shell/tiles-and-notifications/push-request-response-headers)
			headers={'X-WNS-Type': 'wns/raw', 'X-WNS-Cache-Policy': 'no-cache'}
		)
	except Exception as e:
		print(subscriber['_id'], e.args)
		"""
		404 - Endpoint Not Found - The URL specified is invalid and should not be used again.
		410 - Endpoint Not Valid - The URL specified is no longer valid and should no longer be used. A User has become permanently unavailable at this URL.
		"""
		if e.response is not None:
			print('WebPushException', e.args)
			print(e.response.text)
			print(e.response.headers)
		if e.response is not None and (e.response.status_code == 404 or e.response.status_code == 410):
			subscribersCollection.update_one({"_id": subscriber['_id']}, {"$set": {
				"valid": False,
				"invalidReason": [e.args, e.response.status_code, e.response.text, e.response.headers]
			}}) # mark invalid
		subscribersCollection.update_one({"_id": subscriber['_id']}, {"$push": {"failed": notification['_id']}})
		notificationsCollection.update_one({"_id": notification['_id']}, {"$push": {"failed": subscriber['_id']}})

def sendNotification(notification):
	subscribers = subscribersCollection.find({"valid": {"$ne": False}, "registered": True})
	executor = ThreadPoolExecutor(max_workers=75)
	attempted = []
	for subscriber in subscribers:
		executor.submit(sendOneNotification, subscriber, notification)
		# worker(subscriber)
		attempted.append(subscriber['_id'])
	notificationsCollection.update_one({"_id": notification['_id']}, {"$push": {"attempted": {"$each": attempted}}})

def require_api_key(f):
	@wraps(f)
	def wrapper(*args, **kwargs):
		if 'api-key' not in request.headers:
			return json_response(401, 'No API key provided', None)
		apiKey = request.headers.get('api-key')
		if (sha256(apiKey) != GENERIC_HOOK_KEY_HASH):
			return json_response(401, 'Invalid API key', None)
		return f(*args, **kwargs)
	return wrapper

@app.route('/')
def index():
	return 'hi'

@app.route('/admin/getAllSubscriptions')
@require_api_key
def getAllSubscriptions():
	subscriptions = subscribersCollection.find({"registered": True})
	subscriptionsList = list(map(lambda s: {**s, "_id":str(s["_id"]), "created": mktime(s["created"].timetuple())}, subscriptions))
	print(subscriptionsList)
	return success_json(data={'subscriptions': subscriptionsList})

@app.route('/admin/sendNotification', methods=['POST'])
@require_api_key
def sendNotificationAdmin():
	data = request.get_json()
	notification = makeNotification(data['title'], data['body'])
	subscriber = subscribersCollection.find_one({"_id": ObjectId(data['subscriberId'])})
	sendOneNotification(subscriber, notification)
	return success_json()

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
	postType = data['fields']['type']['en-US']
	if postType == 'silent':
		return error_json('silent post, not sending notification')
	img = getMarkdownImage(data['fields']['contentText']['en-US'])
	notification = makeNotification(
		f'4-H Fair: {"Emergency Alert" if postType == "emergency" else "New post"}',
		data['fields']['title']['en-US'], {
			'image': img,
			'timestamp': unixTimeMs(datetime.fromisoformat(data['sys']['updatedAt'][:-1])),
			'actions': [
				{
					'action': f'/#{data["sys"]["id"]}',
					'title': f'View Full {"Alert" if postType == "emergency" else "Post"}'
				}
			]
		}
	)
	sendNotification(notification)
	print(notification)
	return success_json(data=notification['data'])

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=5000, debug=True)
