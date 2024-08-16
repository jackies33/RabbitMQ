



import requests
import json
import datetime


ALERTMANAGER_URL = 'http://10.50.164.36:9093/api/v2/alerts'


alert = [
    {
        "labels": {
            "alertname": "HighCPUUsage",
            "severity": "critical"
        },
        "annotations": {
            "summary": "High CPU usage detected",
            "description": "The CPU usage has exceeded the threshold"
        },
        "startsAt": datetime.datetime.utcnow().isoformat("T") + "Z",
        "endsAt": (datetime.datetime.utcnow() + datetime.timedelta(hours=1)).isoformat("T") + "Z",
        "generatorURL": "http://whatafuck.com"
    }
]


headers = {
    'Content-Type': 'application/json',
}

response = requests.post(ALERTMANAGER_URL, headers=headers, data=json.dumps(alert))


if response.status_code == 200:
    print("Alert sent successfully!")
else:
    print(f"Failed to send alert: {response.status_code}")
    print(response.text)
