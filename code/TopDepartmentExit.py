from pyspark import SparkContext
import sys
import csv
from io import StringIO

def parse_csv(line):
    return next(csv.reader(StringIO(line)))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: TopDepartmentExit.py <input_path> <output_path>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="TopDepartmentExit")

    lines = sc.textFile(input_path)

    header = lines.first()
    header_fields = parse_csv(header)

    dept_idx = header_fields.index("Department")
    left_idx = header_fields.index("Left")

    mapped = (
        lines
        .filter(lambda row: row != header)
        .map(parse_csv)
        .map(lambda fields: (
            fields[dept_idx],
            (int(fields[left_idx]), 1)
        ))
    )

    reduced = mapped.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    attrition = reduced.map(
        lambda x: (x[0], x[1][0] / x[1][1])
    )

    sorted_result = attrition.sortBy(lambda x: x[1], ascending=False)

    formatted_output = sorted_result.map(
        lambda x: f"{x[0]}: Attrition Rate = {x[1]:.2%}"
    )

    formatted_output.saveAsTextFile(output_path)

    sc.stop()