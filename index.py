# fmt: off
from datetime import datetime
import time
from flask import Flask, request, Response
from flask_cors import CORS
import pymongo
import dotenv
from os import environ
from pywebpush import webpush, WebPushException
import json
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
app.url_map.strict_slashes = False
CORS(app)

dotenv.load_dotenv()
MONGODB_SECRET = environ.get('MONGODB_SECRET')
WEBPUSH_PRIVATE_KEY = environ.get('WEBPUSH_PRIVATE_KEY')

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

def makeNotification(title, body, options = {}):
	notificationId = str(uuid4())
	createdTime = datetime.utcnow()
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
				data=json.dumps({"type": "notification", "id":notification['_id'], "time":time.mktime(notification['createdTime'].timetuple()), "data": notification['data']}),
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

@app.route('/')
def index():
	return 'hi'

@app.route('/hooks/notification/generic', methods=['POST'])
def genericNotification():
	data = request.get_json()
	notification = makeNotification(data['title'], data['body'])
	sendNotification(notification)
	return success_json()

if __name__ == '__main__':
	app.run(debug=True)
