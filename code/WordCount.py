from pyspark import SparkContext
import sys
import re

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: WordCount.py <input> <output>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="WordCount")

    text_file = sc.textFile(input_path)

    counts = (
        text_file
        .flatMap(lambda line: re.findall(r"\b\w+\b", line))
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )

    counts.saveAsTextFile(output_path)

    sc.stop()