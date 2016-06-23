from spark_test_executor import SparkTestCase
from attribution.rdd_helper import process_events, process_impressions


class EventsProcessingTest(SparkTestCase):

    def test_process_events(self):
        """
        This test covers event data de-duplication
        """
        in_list = [u'10,ev1,0,usr1,visit',
                   u'80,ev2,0,usr1,click',
                   u'80,ev3,0,usr1,click',
                   u'90,ev4,0,usr1,click',
                   u'150,ev5,0,usr1,click',
                   u'300,ev6,0,usr1,click',
                   u'310,ev7,0,usr1,visit',
                   u'330,ev7,0,usr1,visit']

        expected = [((u'usr1', 0, u'visit'), 10),
                    ((u'usr1', 0, u'click'), 80),
                    ((u'usr1', 0, u'click'), 150),
                    ((u'usr1', 0, u'click'), 300),
                    ((u'usr1', 0, u'visit'), 310)]

        result = process_events(self.sc.parallelize(in_list)).collect()
        self.assertListEqual(sorted(expected), sorted(result))


class ImpressionProcessingTest(SparkTestCase):

    def test_process_impressions(self):
        """
        This test covers impressions data processing
        """
        in_list = [u'10,0,1,usr1',
                   u'20,0,2,usr1',
                   u'30,0,3,usr1',
                   u'40,1,1,usr2',
                   u'50,1,4,usr2']

        expected = [((u'usr1', 0, u'impression'), 10),
                    ((u'usr1', 0, u'impression'), 20),
                    ((u'usr1', 0, u'impression'), 30),
                    ((u'usr2', 1, u'impression'), 40),
                    ((u'usr2', 1, u'impression'), 50)]

        result = process_impressions(self.sc.parallelize(in_list)).collect()
        self.assertListEqual(sorted(expected), sorted(result))
