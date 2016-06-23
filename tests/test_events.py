import unittest
from attribution.rdd_helper import event_split
from attribution.schemas import event_columns


class EventParsingTest(unittest.TestCase):

    def test_events_split(self):
        row = '1,2,3,4,5'
        expected = (1, '2', 3, '4', '5')
        res = event_split(row)
        self.assertTupleEqual(res,
                              expected,
                              msg="Event fields were parsed incorrectly")

    def test_event_parse_fields(self):
        row = '1,2,3,4,5,6,7'
        expected = (1, '2', 3, '4', '5')
        res = event_split(row)
        for event_column in event_columns:
            self.assertEqual(res[event_columns[event_column]],
                             expected[event_columns[event_column]],
                             msg="Error in parsing event: {} field".format(event_columns[event_column]))
