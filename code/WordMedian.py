from pyspark import SparkContext
import sys
import re

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: WordMedian.py <input> <output>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="WordMedian")

    text_file = sc.textFile(input_path)

    words = text_file.flatMap(lambda line: re.findall(r"\b\w+\b", line))

    word_lengths = words.map(lambda word: (len(word), 1))

    length_counts = word_lengths.reduceByKey(lambda a, b: a + b).sortByKey()

    length_counts_list = length_counts.collect()

    total_words = sum(count for _, count in length_counts_list)

    median_index = total_words // 2
    is_even = (total_words % 2 == 0)

    cumulative = 0
    median = 0
    prev_length = 0

    for length, count in length_counts_list:
        cumulative += count
        if not is_even and cumulative > median_index:
            median = length
            break
        elif is_even:
            if cumulative == median_index:
                prev_length = length
            elif cumulative > median_index:
                if prev_length == 0:
                    median = length
                else:
                    median = (prev_length + length) / 2.0
                break

    result = [f"median is: {median:.2f}" if isinstance(median, float) else f"median is: {median}"]
    for length, count in length_counts_list:
        result.append(f"{length} {count}")

    sc.parallelize(result).saveAsTextFile(output_path)
    sc.stop()