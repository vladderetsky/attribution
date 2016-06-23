from spark_test_executor import SparkTestCase
from attribution.rdd_helper import (
    produce_count_of_events,
    produce_count_of_users
)


class CountOfEventsTest(SparkTestCase):

    def test_produce_count_of_events(self):
        in_list = [((0, u'visit', u'a'), 1),
                   ((0, u'purchase', u'a'), 1),
                   ((0, u'click', u'a'), 1),
                   ((1, u'click', u'a'), 2),
                   ((0, u'visit', u'b'), 2),
                   ((0, u'click', u'b'), 1)]

        expected = ['0,click,2',
                    '0,purchase,1',
                    '0,visit,3',
                    '1,click,2']

        result = produce_count_of_events(self.sc.parallelize(in_list)).collect()
        self.assertListEqual(sorted(expected), sorted(result))


class CountOfUsersTest(SparkTestCase):

    def test_produce_count_of_users(self):
        in_list = [((0, u'visit', u'a'), 1),
                   ((0, u'purchase', u'a'), 1),
                   ((0, u'click', u'a'), 1),
                   ((1, u'click', u'a'), 2),
                   ((0, u'visit', u'b'), 2),
                   ((0, u'click', u'b'), 1)]

        expected = ['0,click,2',
                    '0,purchase,1',
                    '0,visit,2',
                    '1,click,1']

        result = produce_count_of_users(self.sc.parallelize(in_list)).collect()
        self.assertListEqual(sorted(expected), sorted(result))
