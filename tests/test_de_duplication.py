import unittest
from attribution.rdd_helper import deduplicate, deduplicate_list


class DeDuplicationTest(unittest.TestCase):

    def test_deduplicate_list(self):
        delta = 3
        test_list = [1, 2, 2, 2, 5, 9, 9, 15, 17, 22, 30]
        expected_list = [1, 5, 9, 15, 22, 30]
        res_list = deduplicate_list(test_list, delta)
        self.assertListEqual(res_list,
                             expected_list,
                             msg="List was de-duplicated incorrectly")

    def test_deduplicate_list_empty(self):
        delta = 3
        test_list = []
        expected_list = []
        res_list = deduplicate_list(test_list, delta)
        self.assertListEqual(res_list,
                             expected_list,
                             msg="Empty List was de-duplicated incorrectly")

    def test_deduplicate_list_big_delta(self):
        delta = 100
        test_list = [1, 2, 5, 9, 9, 15, 17, 22, 30]
        expected_list = [1]
        res_list = deduplicate_list(test_list, delta)
        self.assertListEqual(res_list,
                             expected_list,
                             msg="De-duplication issue while using huge Delta value")

    def test_deduplicate(self):
        test_tuple = ((1, 2, 3), [10, 20, 20, 20, 50, 90, 90, 150, 170, 220, 300])
        expected_tuple = ((1, 2, 3), [10, 150, 300])
        res_tuple = deduplicate(test_tuple)
        self.assertTupleEqual(res_tuple,
                              expected_tuple,
                              msg="Tuple was de-duplicated incorrectly")

    def test_deduplicate_with_delta(self):
        delta = 30
        test_tuple = ((1, '2', 3), [10, 20, 20, 20, 50, 90, 90, 150, 170, 220, 300])
        expected_tuple = ((1, '2', 3), [10, 50, 90, 150, 220, 300])
        res_tuple = deduplicate(test_tuple, delta)
        self.assertTupleEqual(res_tuple,
                              expected_tuple,
                              msg="Tuple was de-duplicated with delta parameter incorrectly")
