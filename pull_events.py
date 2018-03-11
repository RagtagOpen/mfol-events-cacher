import boto3
import json
import re
import requests
import sys


def fetch_events_as_geojson():
    resp = requests.get('https://event.marchforourlives.com/cms/event/march-our-lives-events_attend/search_results/?all=1')

    features = []
    for detail_text in re.findall(r'var event_details = {(.*?)};', resp.text, re.DOTALL):
        props = {}
        for k, v in re.findall(r"'(.*?)': '(.*?)',?", detail_text):
            if v == "False":
                v = False
            elif v == "True":
                v = True
            elif v == "None":
                v = None

            props[k] = v

        feature = {
            'type': 'Feature',
            'properties': None,
            'geometry': {
                'type': 'Point',
                'coordinates': [
                    float(props.pop('longitude')),
                    float(props.pop('latitude')),
                ]
            }
        }
        feature['properties'] = props

        features.append(feature)

    feature_coll = {
        'type': 'FeatureCollection',
        'features': features,
    }

    return json.dumps(feature_coll, separators=(',',':'))


def push_to_s3(bucket, key, geojson):
    client = boto3.client('s3')
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=geojson.encode('utf8'),
        ACL='public-read',
        ContentType='application/json',
    )


def main():
    bucket = os.environ.get('S3_BUCKET')
    key = os.environ.get('S3_KEY')

    assert bucket, "Please set an S3_BUCKET environment variable"
    assert key, "Please set an S3_KEY environment variable"

    geojson = fetch_events_as_geojson()
    push_to_s3(bucket, key, geojson)


if __name__ == '__main__':
    main()
