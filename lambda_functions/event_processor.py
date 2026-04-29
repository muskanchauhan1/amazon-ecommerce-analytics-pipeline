import json
import boto3
import hashlib
from datetime import datetime
import geoip2.database

s3 = boto3.client('s3')
firehose = boto3.client('firehose')

# GeoIP for enriching IP addresses
GEO_DB_PATH = '/opt/python/GeoLite2-City.mmdb'

def lambda_handler(event, context):
    enriched_records = []
    geo_reader = geoip2.database.Reader(GEO_DB_PATH)

    for record in event['Records']:
        # Kinesis record: base64 encoded JSON
        payload = json.loads(record['kinesis']['data'])

        # Enrich with geo data
        try:
            geo = geo_reader.city(payload.get('ip_address', ''))
            payload['country'] = geo.country.iso_code
            payload['city'] = geo.city.name
        except Exception:
            payload['country'] = 'Unknown'
            payload['city'] = 'Unknown'

        # Add processing metadata
        payload['event_id'] = hashlib.md5(
            f"{payload.get('user_id')}{payload.get('timestamp')}".encode()
        ).hexdigest()
        payload['processed_ts'] = datetime.utcnow().isoformat()
        payload['source'] = 'kinesis_stream'

        enriched_records.append(json.dumps(payload))

    # Batch write to Firehose (raw zone in S3)
    firehose.put_record_batch(
        DeliveryStreamName='ecommerce-events-stream',
        Records=[{'Data': rec + '\n'} for rec in enriched_records]
    )

    geo_reader.close()
    return {'status': 'success', 'records_processed': len(enriched_records)}
