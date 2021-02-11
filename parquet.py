from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import sys


def create_schema(schema_file):
    schema_mapping = {
        'varchar': StringType(),
        'char' : StringType(),
        'nvarchar': StringType(),
        'nchar': StringType(),
        'int': IntegerType(),
        'decimal': DecimalType(),
        'numeric': DecimalType(),
        'date': DateType(),
        'datetime': TimestampType(),
        'datetime2': TimestampType()
    }
    
    with open(schema_file, 'r') as f:
        export = []
        for line in f.readlines():
            export.append(line.replace('\n', '').split('_'))
        final_export = []
        for line in export:
            final_export.append(StructField('"' + line[0] + '"', schema_mapping[line[1]], True if line[2] == 1 else False))


def main(csv_file, schema_file, parquet_file):
    sc = SparkContext(appName="CSV2Parquet")
    sqlContext = SQLContext(sc)

    schema = StructType(create_schema(schema_file))

    print('Creating dataframe')
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', schema=schema).load(csv_file)
    print('Writing dataframe')
    df.write.parquet(parquet_file, mode='overwrite')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3])
