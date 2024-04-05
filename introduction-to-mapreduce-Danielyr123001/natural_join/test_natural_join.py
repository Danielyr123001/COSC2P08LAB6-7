import pytest
import pandas as pd

import natural_join_pandas as njpd
import natural_join_rdd as njrdd
import natural_join_spark_df as njdf

# Sample DataFrames for testing
def test_natural_join_pandas():
    """Test natural join without parallelism."""
    data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    data2 = [(1, 25), (2, 30), (4, 40)]

    columns1 = ["id", "name"]
    columns2 = ["id", "age"]
    result = njpd.natural_join(data1, columns1, data2, columns2, "id")

    # Assert the correctness of the result
    expected_result = pd.DataFrame([(1, "Alice", 25), (2, "Bob", 30)], columns=["id", "name", "age"])
    pd.testing.assert_frame_equal(result, expected_result)

# Sample DataFrames for testing
def test_natural_join_spark():
    data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    data2 = [(1, 25), (2, 30), (4, 40)]

    columns1 = ["id", "name"]
    columns2 = ["id", "age"]
    result = njdf.natural_join(data1, columns1, data2, columns2, "id")

    # Assert the correctness of the result
    expected_result = [(1, "Alice", 25), (2, "Bob", 30)]
    assert sorted(result) == sorted(expected_result)
    

# Sample DataFrames for testing
def test_natural_join_rdd():
    """Test natural join with parallelism."""
    data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    data2 = [(1, 25), (2, 30), (4, 40)]

    result =  njrdd.natural_join(data1, data2)

    # Assert the correctness of the result
    expected_result = [(1, "Alice", 25), (2, "Bob", 30)]
    assert sorted(result) == sorted(expected_result)

