from spark_test_executor import SparkTestCase
from attribution.rdd_helper import process_attribution


class AttributionTest(SparkTestCase):

    def test_process_attribution(self):
        in_list = [((u'a', 0, u'click'), 10),
                   ((u'a', 0, u'impression'), 20),
                   ((u'a', 0, u'visit'), 30),
                   ((u'a', 0, u'impression'), 40),
                   ((u'a', 0, u'purchase'), 50),
                   ((u'a', 0, u'impression'), 60),
                   ((u'a', 0, u'click'), 70),
                   ((u'a', 1, u'impression'), 80),
                   ((u'a', 1, u'click'), 90),
                   ((u'a', 1, u'purchase'), 100),
                   ((u'a', 1, u'impression'), 110),
                   ((u'a', 1, u'impression'), 120),
                   ((u'a', 1, u'click'), 130),
                   ((u'b', 0, u'impression'), 140),
                   ((u'b', 0, u'visit'), 150),
                   ((u'b', 0, u'impression'), 160),
                   ((u'b', 0, u'visit'), 170),
                   ((u'b', 0, u'impression'), 180),
                   ((u'b', 0, u'click'), 190),
                   ((u'b', 0, u'purchase'), 200)]

        expected = [((0, u'visit', u'a'), 1),
                    ((0, u'purchase', u'a'), 1),
                    ((0, u'click', u'a'), 1),
                    ((1, u'click', u'a'), 2),
                    ((0, u'visit', u'b'), 2),
                    ((0, u'click', u'b'), 1)]

        result = process_attribution(self.sc.parallelize(in_list)).collect()
        # Should apply sorted() for each list, otherwise cannot assert
        self.assertListEqual(sorted(expected), sorted(result))
