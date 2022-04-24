import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGbdtClassifier(unittest.TestCase):
    def test_gbdtclassifier(self):

        df = pd.DataFrame([
            [1.0, "A", 0, 0, 0],
            [2.0, "B", 1, 1, 0],
            [3.0, "C", 2, 2, 1],
            [4.0, "D", 3, 3, 1]
        ])
        
        batchSource = BatchOperator.fromDataframe(
            df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
        streamSource = StreamOperator.fromDataframe(
            df, schemaStr=' f0 double, f1 string, f2 int, f3 int, label int')
        
        GbdtClassifier()\
            .setLearningRate(1.0)\
            .setNumTrees(3)\
            .setMinSamplesPerLeaf(1)\
            .setPredictionDetailCol('pred_detail')\
            .setPredictionCol('pred')\
            .setLabelCol('label')\
            .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
            .fit(batchSource)\
            .transform(batchSource)\
            .print()
        
        GbdtClassifier()\
            .setLearningRate(1.0)\
            .setNumTrees(3)\
            .setMinSamplesPerLeaf(1)\
            .setPredictionDetailCol('pred_detail')\
            .setPredictionCol('pred')\
            .setLabelCol('label')\
            .setFeatureCols(['f0', 'f1', 'f2', 'f3'])\
            .fit(batchSource)\
            .transform(streamSource)\
            .print()
        
        StreamOperator.execute()
        pass