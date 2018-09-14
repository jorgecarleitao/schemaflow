import unittest
import datetime

import numpy as np
import pyspark
from pyspark.sql.types import Row

from schemaflow.types import PySparkDataFrame, infer_schema


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
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0, b='s'), Row(a=1.0, b='s')])
        self.assertEqual(type.check_schema(instance), [])

        # extra column is ok
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0, b='s', c=1.0),
                                                         Row(a=1.0, b='s', c=1.0)])
        self.assertEqual(type.check_schema(instance), [])

        # missing column
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0), Row(a=1.0)])
        self.assertEqual(len(type.check_schema(instance)), 1)

        # wrong column type
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0, b=1),
                                                         Row(a=1.0, b=1)])
        self.assertEqual(len(type.check_schema(instance)), 1)

    def test_date_time(self):
        instance_type = PySparkDataFrame(schema={'a': datetime.datetime})
        instance = self.sqlContext.createDataFrame(data=[Row(a=datetime.datetime.now())])
        self.assertEqual(len(instance_type.check_schema(instance)), 0)

        instance_type = PySparkDataFrame(schema={'a': datetime.date})
        instance = self.sqlContext.createDataFrame(data=[Row(a=datetime.datetime.now().date())])
        self.assertEqual(len(instance_type.check_schema(instance)), 0)

        # wrong types
        instance_type = PySparkDataFrame(schema={'a': datetime.datetime})
        instance = self.sqlContext.createDataFrame(data=[Row(a=datetime.datetime.now().date())])
        self.assertEqual(len(instance_type.check_schema(instance)), 1)

        instance_type = PySparkDataFrame(schema={'a': datetime.date})
        instance = self.sqlContext.createDataFrame(data=[Row(a=datetime.datetime.now())])
        self.assertEqual(len(instance_type.check_schema(instance)), 1)

    def test_int_float(self):
        instance_type = PySparkDataFrame(schema={'a': float})
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0)])
        self.assertEqual(len(instance_type.check_schema(instance)), 0)

        instance_type = PySparkDataFrame(schema={'a': int})
        instance = self.sqlContext.createDataFrame(data=[Row(a=1)])
        self.assertEqual(len(instance_type.check_schema(instance)), 0)

        # wrong types
        instance_type = PySparkDataFrame(schema={'a': float})
        instance = self.sqlContext.createDataFrame(data=[Row(a=1)])
        self.assertEqual(len(instance_type.check_schema(instance)), 1)

        instance_type = PySparkDataFrame(schema={'a': int})
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0)])
        self.assertEqual(len(instance_type.check_schema(instance)), 1)

    def test_bool(self):
        instance_type = PySparkDataFrame(schema={'a': bool})
        instance = self.sqlContext.createDataFrame(data=[Row(a=False)])
        self.assertEqual(len(instance_type.check_schema(instance)), 0)

        instance = self.sqlContext.createDataFrame(data=[Row(a=1)])
        self.assertEqual(len(instance_type.check_schema(instance)), 1)

    def test_infer(self):
        instance = self.sqlContext.createDataFrame(data=[Row(a=1.0, b='a'), Row(a=1.0, b='b')])

        schema = infer_schema({'a': instance})
        self.assertEqual(schema, {'a': PySparkDataFrame(schema={'a': float, 'b': np.dtype('O')})})
