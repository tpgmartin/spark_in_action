# Need to configure log level
from pyspark.sql import SparkSession

class CsvToDataFrame(object):

    def __init__(self, filename):
        self.start(filename)
    
    def start(self, filename):
        spark = SparkSession.builder.appName("CSV to Dataset").master("local").getOrCreate()

        self.df = spark.read.csv(filename, inferSchema=True, header=True)

if __name__ == "__main__":

    app = CsvToDataFrame("data/books.csv")
    print(app.df.show(5))
