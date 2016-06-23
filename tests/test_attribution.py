import unittest
from attribution.rdd_helper import attributed_events, get_attributed_events


class AttributionTest(unittest.TestCase):

    def test_attributed_events(self):
        event_tuple = (
            ('xyz', 1),
            [(6, 'impression'),
             (1, 'a'),
             (5, 'c'),
             (3, 'impression'),
             (4, 'b'),
             (7, 'd'),
             (8, 'e'),
             (2, 'impression')])

        expected_tuple = (
            ('xyz', 1),
            [(4, 'b'),
             (7, 'd')])

        res_tuple = attributed_events(event_tuple)
        self.assertTupleEqual(res_tuple,
                              expected_tuple,
                              msg="attributed_events() processed events incorrectly")

    def test_get_attributed_events(self):
        event_list = [
            (6, 'impression'),
            (1, 'a'),
            (5, 'c'),
            (3, 'impression'),
            (3, 'impression'),
            (4, 'b'),
            (7, 'd'),
            (8, 'e'),
            (2, 'impression')]
        expected_list = [
            (4, 'b'),
            (7, 'd')]

        res_list = get_attributed_events(event_list)
        self.assertListEqual(res_list,
                             expected_list,
                             msg="get_attributed_events() processed events incorrectly")
