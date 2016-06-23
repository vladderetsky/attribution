import unittest
from attribution.rdd_helper import impression_split
from attribution.schemas import impression_columns


class ImpressionParsingTest(unittest.TestCase):

    def test_impressions_split(self):
        row = '1,2,3,4,5,6,7'
        expected = (1, 2, 3, '4')
        res = impression_split(row)
        self.assertTupleEqual(res,
                              expected,
                              msg="Impression fields were parsed incorrectly")

    def test_impression_parse_fields(self):
        row = '1,2,3,4,5,6,7'
        expected = (1, 2, 3, '4')
        res = impression_split(row)
        for imp_col in impression_columns:
            self.assertEqual(res[impression_columns[imp_col]],
                             expected[impression_columns[imp_col]],
                             msg="Error in parsing impression: {} field".format(impression_columns[imp_col]))
