from pyspark import SparkContext
import sys
import os
import re

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: InvertedIndex.py <input> <output>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="InvertedIndex")

    files_rdd = sc.wholeTextFiles(input_path)

    word_file_pairs = files_rdd.flatMap(
        lambda file: [(word.lower(), os.path.basename(file[0])) 
                      for word in re.findall(r'\b\w+\b', file[1])]
    )

    unique_word_file_pairs = word_file_pairs.distinct()

    inverted_index = unique_word_file_pairs.groupByKey().mapValues(lambda files: sorted(set(files)))

    formatted_result = inverted_index.map(lambda x: f"{x[0]}: {', '.join(x[1])}")

    formatted_result.saveAsTextFile(output_path)

    sc.stop()
