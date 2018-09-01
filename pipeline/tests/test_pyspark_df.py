import unittest

import numpy as np
import pyspark

from pipeline.types import PySparkDataFrame


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster('local[1]').setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.sqlContext = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()


class TestPySparkDataFrame(PySparkTestCase):

    def test_type_check(self):
        # ok
        type = PySparkDataFrame(schema={'a': float, 'b': np.dtype('O')})
        instance = self.sqlContext.createDataFrame(data=[{'a': 1.0, 'b': 's'}, {'a': 1.0, 'b': 's'}])
        self.assertEqual(type.check_schema(instance), [])

        # extra column is ok
        instance = self.sqlContext.createDataFrame(data=[{'a': 1.0, 'b': 's', 'c': 1.0},
                                                         {'a': 1.0, 'b': 's', 'c': 1.0}])
        self.assertEqual(type.check_schema(instance), [])

        # missing column
        instance = self.sqlContext.createDataFrame(data=[{'a': 1.0}, {'a': 1.0}])
        self.assertEqual(len(type.check_schema(instance)), 1)

        # wrong column type
        instance = self.sqlContext.createDataFrame(data=[{'a': 1.0, 'b': 1},
                                                         {'a': 1.0, 'b': 1}])
        self.assertEqual(len(type.check_schema(instance)), 1)
