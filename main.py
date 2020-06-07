from google.cloud import bigquery
import base64
import datetime

client = bigquery.Client()
# table_id = "tanelis.tweets_stemmed.stem_words"


query_sql = """
SELECT
MAX(PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
CONCAT(date, " ", REGEXP_EXTRACT(date_string, r"(\\d{2}:\\d{2}:\\d{2})")))) AS last_tweet_time
FROM
`tanelis.tweets_eu.raw_tweets`
WHERE
date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
"""


def query_last_tweet_time():
    data = []

    client = bigquery.Client()
    query_job = client.query(query_sql)
    results = query_job.result()

    for row in results:
        data.append(row["last_tweet_time"])

    return data[0]


def pubsub_tweet_monitor_live(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
    event (dict): Event payload.
    context (google.cloud.functions.Context): Metadata for the event.
    """

    last_tweet_time = query_last_tweet_time()
    now = datetime.datetime.now()

    time_difference = now - last_tweet_time
    print("Time elapsed between last tweet and now: {}".format(time_difference))

    # event data
    if 'data' in event:
        event_data = base64.b64decode(event['data']).decode('utf-8')
    else:
        event_data = 'no data in pub/sub event'
    print('Event data: {}!'.format(event_data))


def test():
    pubsub_tweet_monitor_live('', '')


test()
