from pyspark import SparkContext
import sys
import re
import math

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: WordStandardDeviation.py <input> <output>")
        sys.exit(-1)

    sc = SparkContext(appName="WordStandardDeviation")
    text_file = sc.textFile(sys.argv[1])

    words = text_file.flatMap(lambda line: re.findall(r"\b\w+\b", line))
    word_lengths = words.map(lambda word: len(word))

    count = word_lengths.count()
    total_length = word_lengths.sum()
    total_square = word_lengths.map(lambda x: x**2).sum()

    mean = total_length / count if count != 0 else 0
    variance = (total_square / count) - (mean ** 2) if count != 0 else 0
    std_dev = math.sqrt(variance)

    result = [
        f"Standard Deviation: {std_dev:.2f}",
        f"Count: {count}",
        f"Length: {total_length}",
        f"Square: {int(total_square)}"
    ]

    sc.parallelize(result).saveAsTextFile(sys.argv[2])
    sc.stop()