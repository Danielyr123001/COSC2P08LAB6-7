import pytest
import pandas as pd

import average_pandas as avpd
import average_spark_rdd as avrdd
import average_spark_df as avdf


@pytest.fixture
def data():
    # Sample data
    return [1, 2, 3, 4, 5]


# Sample DataFrames for testing
def test_average_pandas(data):
    result = avpd.average(data)
    assert result == 3.0

def test_average_spark(data):
    result = avdf.average(data)
    assert result == 3.0
    
def test_average_rdd(data):
    result = avrdd.average(data)
    assert result == 3.0

