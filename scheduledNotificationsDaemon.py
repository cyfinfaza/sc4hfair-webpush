import asyncio
from dotenv import load_dotenv
from os import environ
import motor.motor_asyncio
from datetime import datetime
import json
from pywebpush import webpush
from common import unixTimeMs
import aiohttp

load_dotenv()
MONGODB_SECRET = environ.get('MONGODB_SECRET')
WEBPUSH_PRIVATE_KEY = environ.get('WEBPUSH_PRIVATE_KEY')
NUM_PROCESSORS = int(environ.get('NUM_PROCESSORS', 5))
CONTENTFUL_ACCESS_TOKEN = environ.get('CONTENTFUL_ACCESS_TOKEN')

client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_SECRET)
db = client.webpush
subscriptionsCollection = db.subscriptions
scheduledNotificationsCollection = db.scheduledNotifications

tentSlugs: dict[str, str] = {}

async def sendOneNotification(subscriber, scheduledNotification, eventData):
	global tentSlugs
	try:
		print(f'sending notification {scheduledNotification["_id"]} to {subscriber["_id"]}')

		tentSlug = eventData.get('tent')
		tentInfo = ''
		if tentSlug:
			tentName = tentSlugs.get(tentSlug, tentSlug)
			tentInfo = f' {"near" if eventData.get("near") else "in"} {tentName}'

		notifTime = scheduledNotification['when']
		eventTime = datetime.fromisoformat(eventData['time']).strftime('%I:%M %p')

		notification_data = {
			'type': 'notification',
			'id': str(scheduledNotification['_id']),
			'data': {
				'title': f'Upcoming 4-H event: {eventData["title"]}',
				'body': f'Starting at {eventTime}' + tentInfo,
				'options': {
				'timestamp': unixTimeMs(notifTime),
					'actions': [
						{
							'action': f'/schedule#{scheduledNotification["eventId"]}',
							'title': 'More Details'
						}
					],
				}
			}
		}
		if tentSlug:
			notification_data['data']['options']['actions'].append({
				'action': f'/map?locate={tentSlug}',
				'title': 'Show on Map',
			})

		webpush(
			subscription_info=subscriber['subscription_info'],
			data=json.dumps(notification_data),
			vapid_private_key=WEBPUSH_PRIVATE_KEY,
			vapid_claims={'sub': 'mailto:vapid_claims@4hcomputers.club'},
			headers={'X-WNS-Type': 'wns/raw', 'X-WNS-Cache-Policy': 'no-cache'}
		)

		await scheduledNotificationsCollection.update_one(
			{'_id': scheduledNotification['_id']},
			{'$set': {'sent': True, 'sentTime': datetime.utcnow()}}
		)

		print(f'successfully sent notification {scheduledNotification["_id"]} to {subscriber["_id"]}')

	except Exception as e:
		print(e)
		print(f'error for subscriber {subscriber["_id"]}: {str(e)}')

		if hasattr(e, 'response') and e.response is not None:
			print('WebPushException', str(e))
			print(e.response.text)
			print(e.response.headers)

			if e.response.status_code in [404, 410]:
				await subscriptionsCollection.update_one(
					{'_id': subscriber['_id']},
					{'$set': {'valid': False}}
				)

		await subscriptionsCollection.update_one(
			{'_id': subscriber['_id']},
			{'$push': {'failed': scheduledNotification['_id']}}
		)
		await scheduledNotificationsCollection.update_one(
			{'_id': scheduledNotification['_id']},
			{'$set': {'sent': False, 'sentTime': datetime.utcnow()}}
		)

graphqlQuery = '''{
	scheduledEventCollection(order: time_ASC, limit: 1, where: {sys: {id: "%s"}}) {
		items {
			title
			time
			tent
			near
		}
	}
}'''

async def process_notifications(queue: asyncio.Queue, session: aiohttp.ClientSession):
	while True:
		scheduledNotification = await queue.get()
		success = False
		try:
			# Fetch the subscriber based on the target in scheduledNotification
			subscriber = await subscriptionsCollection.find_one({'_id': scheduledNotification['target'], 'valid': {'$ne': False}})

			if subscriber:
				async with session.get(
					'https://graphql.contentful.com/content/v1/spaces/e34g9w63217k/',
					params={
						'query': graphqlQuery % scheduledNotification['eventId']
					},
					headers={
						'Content-Type': 'application/json',
						'Authorization': f'Bearer {CONTENTFUL_ACCESS_TOKEN}',
					}
				) as response:
					data = await response.json()
					items = data['data']['scheduledEventCollection']['items']
					if len(items) == 0:
						print(f'no event {scheduledNotification["eventId"]} found for notification {scheduledNotification["_id"]}')
					else:
						await sendOneNotification(subscriber, scheduledNotification, items[0])
						success = True
			else:
				print(f'subscriber not found for notification {scheduledNotification["_id"]}')

			if success == False:
				await scheduledNotificationsCollection.update_one(
					{'_id': scheduledNotification['_id']},
					{'$set': {'sent': False, 'sentTime': datetime.utcnow()}}
				)

		except Exception as e:
			print(f'error processing notification: {str(e)}')
		finally:
			queue.task_done()

async def check_and_queue_notifications(queue: asyncio.Queue):
	current_time = datetime.utcnow()

	cursor = scheduledNotificationsCollection.find({
		'when': {'$lte': current_time},
		'sent': {'$exists': False}
	})

	async for notification in cursor:
		await queue.put(notification)

async def update_tent_slugs(session: aiohttp.ClientSession):
	global tentSlugs
	while True:
		try:
			async with session.get('https://raw.githubusercontent.com/cyfinfaza/sc4hfair-sveltekit/main/src/data/tentSlugs.json') as response:
				data = await response.json(content_type='')
				tentSlugs = data
				print(f'tent slugs updated')
		except Exception as e:
			print(f'error updating tent slugs: {str(e)}')

		await asyncio.sleep(3600) # update every 1 hour, shouldn't need to be very frequent

async def main():
	queue = asyncio.Queue()

	async with aiohttp.ClientSession() as session:
		# start multiple notification processors
		processors = [
			asyncio.create_task(process_notifications(queue, session))
			for _ in range(NUM_PROCESSORS)
		]

		# start the tent slug updater
		asyncio.create_task(update_tent_slugs(session))

		while True:
			await check_and_queue_notifications(queue)
			await asyncio.sleep(60) # poll every 60 seconds

if __name__ == '__main__':
	asyncio.run(main(), debug=True)
