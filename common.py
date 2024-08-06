from datetime import datetime, timezone

def unixTimeMs(time: datetime):
	return int(time.replace(tzinfo=timezone.utc).timestamp() * 1000)