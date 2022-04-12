import pyspark

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)


def address_split(x):
    for i in range(len(x[0])):
        return (x[0][i], (x[1], x[2]))

path = "/data/ethereum/scams.json"
df = sqlContext.read.json(path)
# ([addresses], category, status)
df.select("result.*","*")
# (address i, (category, status))
