from google.cloud import bigquery
import base64

client = bigquery.Client()
table_id = "tanelis.tweets_stemmed.stem_words"


def pubsub_tweet_monitor_live(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    query = """
    SELECT
    MAX(PARSE_DATETIME('%Y-%m-%d %H:%M:%S',
    CONCAT(date, " ", REGEXP_EXTRACT(date_string, r"(\\d{2}:\\d{2}:\\d{2})")))) AS last_tweet_time
    FROM
    `tanelis.tweets_eu.raw_tweets`
    WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    query_job = client.query(query)  # Make an API request.

    print(query_job)

    for row in query_job:
        # Row values can be accessed by field name or index.
        print("row_num={}, last_tweet_time={}".format(row[0], row["last_tweet_time"]))

    #table = client.get_table(table_id)

    # insert_rows = []
    # for row in query_job:
    #     list_row = list(row)
    #     stem_words = [stemmer.stem(item) for item in list_row[2]]
    #     list_row.append(stem_words)

    #     insert_rows.append(list_row)

    # errors = client.insert_rows(table, insert_rows)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print(errors)

    # event data
    if 'data' in event:
        event_data = base64.b64decode(event['data']).decode('utf-8')
    else:
        event_data = 'no data in pub/sub event'
    print('Event data: {}!'.format(event_data))


def test():
    pubsub_tweet_monitor_live('', '')


test()