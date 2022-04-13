import pyspark
import json
import time

sc = pyspark.SparkContext()
sqlContext = pyspark.SQLContext(sc)

def validateTransaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        pass

def address_split(x):
    for i in range(len(x[0])):
        return (x[0][i], (x[1], x[2]))

path = "/user/dma30/Project"
scams_df = sqlContext.read.option('multiLine', True).json(sc.wholeTextFiles(path).values()).drop('id', 'url', 'name','coin', 'description', 'reporter', 'ip', 'nameservers', 'subcategory')
# ([addresses], category, status)
scams_RDD = scams_df.rdd.map(address_split)
# (address i, (category, status))
lines_tra = sc.textFile('/data/ethereum/transactions')
clean_lines_tra = lines_tra.filter(validateTransaction)
address_val_pair = clean_lines_tra.map(lambda l: (l.split(',')[2], (float(l.split(',')[3]), time.strftime("%y.%m", time.gmtime(float(l.split(',')[6]))))))
joined_RDD = address_val_pair.join(scams_RDD)
# (to add, ((val, time), (cat, status)))

# most lucrative form of scam
key_cat = joined_RDD.map(lambda x: (x[1][1][0], x[1][0][0]))
most_lucrative_cat = key_cat.reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).collect()

for rec in most_lucrative_cat:
    print(rec)