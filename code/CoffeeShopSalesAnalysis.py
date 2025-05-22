from pyspark import SparkContext
import sys
import csv
from io import StringIO

def parse_csv(line):
    return next(csv.reader(StringIO(line)))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: CoffeeShopSalesAnalysis.py <input_path> <output_path>")
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="CoffeeShopSalesAnalysis")

    lines = sc.textFile(input_path)

    header = lines.first()
    header_fields = parse_csv(header)
    product_type_idx = header_fields.index("product_type")
    qty_idx = header_fields.index("transaction_qty")

    result = (
        lines
        .filter(lambda row: row != header)
        .map(parse_csv)
        .map(lambda record: (record[product_type_idx], int(record[qty_idx])))
        .reduceByKey(lambda a, b: a + b)
	    .sortBy(lambda x: x[1], ascending=False)
    )

    result.saveAsTextFile(output_path)

    sc.stop()