from pyspark import SparkContext
import sys
import re

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: WordMean.py <input> <output>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="WordMean")

    text_file = sc.textFile(input_path)

    words = text_file.flatMap(lambda line: re.findall(r"\b\w+\b", line))

    word_lengths = words.map(lambda word: (len(word), 1))

    total_length, total_words = word_lengths.reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    mean_length = total_length / total_words if total_words != 0 else 0

    result = [
        f"Total Words: {total_words}",
        f"Total Length: {total_length}",
        f"Average Word Length: {mean_length:.2f}"
    ]

    sc.parallelize(result).saveAsTextFile(output_path)
    sc.stop()