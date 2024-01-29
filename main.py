import requests
from google.auth import crypt
from google.auth import jwt
import time
import os
import io
import json
import base64
import random
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

credential_path = "arcane-ion-411909-78ae8033d6d1.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

sa_keyfile = credential_path

with io.open(credential_path, "r", encoding="utf-8") as json_file:
    data = json.loads(json_file.read())
    sa_email = data['client_email']

audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
topic = os.getenv("TOPIC")
print(topic)
url = f"https://pubsub.googleapis.com/v1/projects/arcane-ion-411909/topics/{topic}:publish"

sensor_configurations = [
    {"type": "temperature", "min_frequency": 20, "max_frequency": 100, "location": "59.31512881414854, "
                                                                                   "18.052156579299012"},
    {"type": "humidity", "min_frequency": 30, "max_frequency": 80, "location": "50.665746377944934, 19.075875553151967"},
    {"type": "light_intensity", "min_frequency": 50, "max_frequency": 100, "location": "41.89331889917149, "
                                                                                       "12.450566437004108"}
]


def generate_jwt(sa_keyfile,
                 sa_email,
                 audience,
                 expiry_length=3600):

    now = int(time.time())

    payload = {
        'iat': now,
        "exp": now + expiry_length,
        'iss': sa_email,
        'aud': audience,
        'sub': sa_email,
        'email': sa_email
    }

    signer = crypt.RSASigner.from_service_account_file(sa_keyfile)
    jwt_token = jwt.encode(signer, payload)
    return jwt_token


def publish_with_jwt_request(signed_jwt, encoded_element, url):
    headers = {
        'Authorization': 'Bearer {}'.format(signed_jwt.decode('utf-8')),
        'content-type': 'application/json'
    }
    json_data = {
        "messages": [
            {
                "data": encoded_element,
            }
        ]
    }

    response = requests.post(url, headers=headers, json=json_data)
    print(f"Data sent to Pub/Sub - Status Code: {response.status_code}")


def generate_sensor_data(sensor_type):
    if sensor_type == 'temperature':
        return random.uniform(20, 30)
    elif sensor_type == 'humidity':
        return random.uniform(40, 60)
    elif sensor_type == 'light_intensity':
        return random.uniform(100, 1000)


while True:
    for config in sensor_configurations:
        sensor_type = config['type']
        frequency = random.randint(config['min_frequency'], config['max_frequency'])
        sensor_data = generate_sensor_data(sensor_type)
        location = config['location']
        current_date = datetime.utcnow()

        data = {
            'sensor_type': sensor_type,
            'sensor_data': sensor_data,
            'location': location,
            'date': current_date.isoformat(),
        }
        base64_encoded_data = base64.b64encode(json.dumps(data).encode()).decode()

        signed_jwt = generate_jwt(sa_keyfile, sa_email, audience)
        publish_with_jwt_request(signed_jwt, base64_encoded_data, url)

        time.sleep(frequency / 1000)
