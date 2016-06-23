from schemas import event_columns, impression_columns


IMPRESSION_EVENT = 'impression'
DEDUPLICATION_DELTA = 60


def event_split(row):
    """
    Parses the event CSV record to a tuple with corresponded data types
    """
    # It can be modified for using dictionary with self-defined data structure

    r = row.split(',')
    out = (int(r[event_columns['timestamp']]),
           r[event_columns['event_id']],
           int(r[event_columns['advertiser_id']]),
           r[event_columns['user_id']],
           r[event_columns['event_type']])
    return out


def event_fields_for_deduplication(x):
    """
    Picks only required event fields for further processing.
    Event Id was omitted
    """
    return (x[event_columns['user_id']],
            x[event_columns['advertiser_id']],
            x[event_columns['event_type']]), \
           x[event_columns['timestamp']]


def impression_split(row):
    """
    Parses the impression CSV record to a tuple with corresponded data types
    """
    # It can be modified for using dictionary with self-defined data structure

    r = row.split(',')
    out = (int(r[impression_columns['timestamp']]),
           int(r[impression_columns['advertiser_id']]),
           int(r[impression_columns['creative_id']]),
           r[impression_columns['user_id']])
    return out


def impression_fields_for_scanning(x):
    """
    Picks only required impression fields for the further processing.
    Inserts 'impression' value into the data set. It will be used later
    when events and impressions will be combined to calculate attributions.
    creative_id was omitted
    """
    return (x[impression_columns['user_id']],
            x[impression_columns['advertiser_id']],
            IMPRESSION_EVENT), \
           x[impression_columns['timestamp']]


def deduplicate_list(items, delta):
    """
    Removes items from an unsorted list of timestamps or any other integer
    values if items are closer than delta
    e.g.
    delta=60
    [1, 100, 100, 200, 250, 400] --> [1, 100, 200, 400]
    :param items: list of int items
    :param delta: controlling distance between items
    :return: list
    """
    data = items
    new = []
    if data:
        data.sort()
        prev = -1
        for i in data:
            if i - prev >= delta or prev < 0:
                new.append(i)
            prev = i
    return new


def deduplicate(key_value, diff=None):
    """
    This is a wrapper for RDD .map() operation.
    Main logic is implemented in deduplicate_list()
    """
    # Can use parameter from configuration file instead of constant later
    # delta = config.get('events', 'duplicate_time_range')
    if diff:
        delta = diff
    else:
        delta = DEDUPLICATION_DELTA
    key = key_value[0]    # a tuple (user_id, advertiser_id, event_type)
    value = key_value[1]  # a list of timestamps
    return key, deduplicate_list(value, delta)


def get_attributed_events(ts_events):
    """
    Processes a list of tuples where each tuple is a pair of timestamp
    and event type (1450573038, 'purchase')
    It sorts all tuples by first element (timestamp) and processes all events
    that happened chronologically after an impression.
    [(1450573030, 'purchase')
     (1450573031, 'impression')
     (1450573032, 'visit')
     (1450573033, 'click')
     (1450573034, 'impression')
     (1450573035, 'click')]
    Only (1450573032, 'visit') and (1450573035, 'click') will be picked by
    attribution processing
    """
    attributed_events = []
    # Sorting (ts, event) tuple by timestamp
    ts_events.sort(key=lambda tup: tup[0])

    catch_attr_event = False
    for ts, event in ts_events:
        if event == IMPRESSION_EVENT:
            catch_attr_event = True
        else:
            if catch_attr_event:
                attributed_events.append((ts, event))
                catch_attr_event = False
    return attributed_events


def attributed_events(key_value):
    """
    This is a wrapper for RDD .map() operation.
    Main logic is implemented in get_attributed_events()
    """
    key = key_value[0]  # A tuple (user_id, advertiser_id)
    value = key_value[1]  # a list of tuples (timestamp, event_type)
    return key, get_attributed_events(value)


def process_events(rdd):
    """
    This is a set of RDD operations that parses the event records and
    de-duplicates events.
    """
    # groupByKey() operation may affect spark performance. It is better to
    # substitute it by
    # - reduceByKey() or by
    # - map() and Broadcast operations
    # There are some improvements can be done

    return rdd \
        .map(event_split) \
        .map(event_fields_for_deduplication) \
        .groupByKey().mapValues(list) \
        .map(deduplicate) \
        .flatMapValues(lambda x: x)


def process_impressions(rdd):
    """
    This is a set of RDD operations that parses the impression records.
    """
    return rdd \
        .map(impression_split) \
        .map(impression_fields_for_scanning)


def process_attribution(rdd):
    """
    This is a set of RDD operations that processes the attributed events.

    Previously created data format:
    ((user_id, advertiser_id, event_type), timestamp)

    Transformations:
    1. map(lambda x: ((x[0][0], x[0][1]), (x[1], x[0][2]))) as:
    ((user_id, advertiser_id), (timestamp, event_type))
    2. .map(lambda x: ((x[0][1], x[1][1], x[0][0]), 1)) as:
    ((advertiser_id, event_type, user_id), 1)

    Improvement can be done to increase readability and support,
    dictionary can be used
    """
    # groupByKey() operation may affect spark performance. It is better to
    # substitute it by
    # - reduceByKey() or by
    # - map() and Broadcast operations
    # There are some improvements can be done

    return rdd \
        .map(lambda x: ((x[0][0], x[0][1]), (x[1], x[0][2]))) \
        .groupByKey() \
        .mapValues(list) \
        .map(attributed_events) \
        .flatMapValues(lambda x: x) \
        .map(lambda x: ((x[0][1], x[1][1], x[0][0]), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .cache()


def produce_count_of_events(rdd):
    """
    This is a set of RDD operations that calculates the count of attributed
    events for each advertiser, grouped by event type

    Previously created data format:
    ((advertiser_id, event_type, user_id), number_of_records)

    Transformations:
    1. .map(lambda x: ((x[0][0], x[0][1]), x[1])) as:
    (advertiser_id, event_type), number_of_records)
    2. .map(lambda x: "{},{},{}".format(x[0][0], x[0][1], x[1])) as:
    "advertiser_id, event_type, number_of_records"

    """
    return rdd \
        .map(lambda x: ((x[0][0], x[0][1]), x[1])) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: "{},{},{}".format(x[0][0], x[0][1], x[1]))


def produce_count_of_users(rdd):
    """
    This is a set of RDD operations that calculates the count of each unique
    users that have generated attributed events for each advertiser,
    grouped by event type.

    Previously created data format:
    ((advertiser_id, event_type, user_id), number_of_records)

    Transformations:
    1. .map(lambda x: ((x[0][0], x[0][1]), 1))  as:
    (advertiser_id, event_type), 1)
    2. .map(lambda x: "{},{},{}".format(x[0][0], x[0][1], x[1])) as:
    "advertiser_id, event_type, number_of_records"
    """
    return rdd \
        .map(lambda x: ((x[0][0], x[0][1]), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: "{},{},{}".format(x[0][0], x[0][1], x[1]))
