from pyspark.sql import SparkSession
from get_all_variables import src_path,dest_path

def main():
    spark = SparkSession.builder.\
        appName("awsDemoApp").\
        getOrCreate()

    spark.sparkContext.addPyFile('awsDemoScripts.zip')

    df = spark.read.option("inferSchema","true").\
        option("header","true").csv(src_path)
    df.show()
    df.write.parquet(dest_path)

if __name__ == '__main__':
    main()

