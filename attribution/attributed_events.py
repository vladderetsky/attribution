import argparse
from pyspark import SparkContext, SparkConf

from lib.config import config
from lib.cleanup import cleanup_local_fs
from attribution.rdd_helper import (
    process_events,
    process_impressions,
    process_attribution,
    produce_count_of_events,
    produce_count_of_users
)


def create_argument_parser():
    parser = argparse.ArgumentParser(usage='spark-submit attribution/attributed_events.py [-h] [--app_name APP_NAME] [--events EVENTS] [--imps IMPS]',
                                     description='This is a simple attribution application that produces a report \
    that can then be fed into a database. It processes input datasets of events and impressions to compute attributed \
    events and output some simple statistics on them.',
                                     epilog='Sample: spark-submit attribution/attributed_events.py \
                                     --events=/tmp/events.csv --imps=/tmp/impressions.csv')

    parser.add_argument("--app_name", help='Spark Application name')
    parser.add_argument("--events", help='Event CSV file location')
    parser.add_argument("--imps", help='Impression SCV file location')
    return parser.parse_args()


if __name__ == "__main__":

    # Processing arguments, assigning values or default values from configuration
    args = create_argument_parser()
    if args.app_name:
        app_name = args.app_name
    else:
        app_name = config.get('app', 'spark_app_name')

    if args.events:
        event_file = args.events
    else:
        event_file = config.get('input', 'event_file')

    if args.imps:
        impression_file = args.imps
    else:
        impression_file = config.get('input', 'impression_file')

    count_of_events_path = config.get('output', 'count_of_events_path')
    count_of_users_path = config.get('output', 'count_of_users_path')

    # SparkConfig with master='local' is used here
    # It should be changes to 'master' if application is used in cluster
    conf = SparkConf().setAppName(app_name).setMaster("local")
    conf.set("spark.ui.showConsoleProgress", False)
    sc = SparkContext(conf=conf)

    # Loading and processing events, applying de-duplication
    deduplicated_events_rdd = process_events(sc.textFile(event_file))

    # Loading and processing impressions
    impressions_rdd = process_impressions(sc.textFile(impression_file))

    # Union of events and impressions
    all_events_rdd = deduplicated_events_rdd.union(impressions_rdd)

    # Calculation the attributed events
    attributed_events_rdd = process_attribution(all_events_rdd)

    # Precessing the count_of_events report
    cleanup_local_fs(count_of_events_path)
    count_of_events_rdd = produce_count_of_events(attributed_events_rdd)
    count_of_events_rdd.saveAsTextFile(count_of_events_path)

    # Precessing the count_of_users report
    cleanup_local_fs(count_of_users_path)
    count_of_users_rdd = produce_count_of_users(attributed_events_rdd)
    count_of_users_rdd.saveAsTextFile(count_of_users_path)

    sc.stop()

