"""
Spark Unit Test Executor

For running test locally:

Run the following steps to install Spark 1.6.1 distribution

$ wget http://apache.mirrors.ionfish.org/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
$ tar zxvf spark-1.6.1-bin-hadoop2.6.tgz
$ rm spark-1.6.1-bin-hadoop2.6.tgz
$ ln -s spark-1.6.1-bin-hadoop2.6  spark
$ export SPARK_HOME=$APPS/spark
$ export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.9-src.zip
$ export PATH=$PATH:$SPARK_HOME/python:$SPARK_HOME/bin:$SPARK_HOME/python/lib/py4j-0.9-src.zip

"""

import unittest
import logging
from pyspark import SparkConf, SparkContext

logging.getLogger('py4j.java_gateway').setLevel(logging.WARN)  # reducing some noise
logging.getLogger("py4j").setLevel(logging.WARN)


class SparkTestCase(unittest.TestCase):

    def setUp(self):
        # Each Spark related unit test method will have a new spark context
        app_name = self.__class__.__name__
        conf = SparkConf().setAppName(app_name).setMaster("local")
        conf.set("spark.ui.showConsoleProgress", False)
        self.sc = SparkContext(conf=conf)

    def tearDown(self):
        self.sc.stop()


class RecycleSparkTestCase(SparkTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    @classmethod
    def setUpClass(cls):
        # The Spark Test Class and all its unit tests will have a common spark context
        app_name = cls.__class__.__name__
        conf = SparkConf().setAppName(app_name).setMaster("local")
        conf.set("spark.ui.showConsoleProgress", False)
        cls.sc = SparkContext(conf=conf)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
