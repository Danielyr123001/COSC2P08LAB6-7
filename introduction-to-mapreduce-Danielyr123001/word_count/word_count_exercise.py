import word_count_rdd as wcrdd
import word_count_spark_df as wcdf
import word_count_pandas as wcpd

text_file = "data/wordcount.txt"
#word_counts = wcrdd.word_count(text_file)
word_counts = wcdf.word_count(text_file)
#word_counts = wcpd.word_count(text_file)
print("Word Counts:")
for word, count in word_counts.items():
    print(f"{word}: {count}")

