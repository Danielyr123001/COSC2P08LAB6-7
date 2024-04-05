import pytest
import word_count_rdd as wcrdd
import word_count_spark_df as wcdf
import word_count_pandas as wcpd

@pytest.fixture
def sample_text():
    """Create sample text for testing."""
    return "data/test.txt"

def test_word_count_RDD(sample_text):
    """Test word count."""
    text = sample_text
    word_counts = wcrdd.word_count(text)
    expected_word_counts = {"world": 2,"hello": 3}
    assert word_counts == expected_word_counts

def test_word_count_DF(sample_text):
    """Test word count."""
    text = sample_text
    word_counts = wcdf.word_count(text)
    expected_word_counts = {"world": 2,"hello": 3}
    assert word_counts == expected_word_counts

def test_word_count(sample_text):
    """Test word count."""
    text = sample_text
    word_counts = wcpd.word_count(text)
    expected_word_counts = {"world": 2,"hello": 3}
    assert word_counts == expected_word_counts
