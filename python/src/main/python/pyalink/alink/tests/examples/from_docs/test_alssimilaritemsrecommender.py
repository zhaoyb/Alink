import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAlsSimilarItemsRecommender(unittest.TestCase):
    def test_alssimilaritemsrecommender(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        sdata = StreamOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
            .setNumIter(10).setRank(10).setLambda(0.01)
        
        model = als.linkFrom(data)
        alsRec = AlsSimilarItemsRecommender().setModelData(model) \
            .setItemCol("item").setRecommCol("rec").setK(1).setReservedCols(["item"])
        
        alsRec.transform(data).print();
        pass