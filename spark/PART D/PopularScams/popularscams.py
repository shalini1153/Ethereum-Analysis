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


path = "hdfs://andromeda.eecs.qmul.ac.uk/user/sj005/Scams.csv"
scams_df = sc.textFile(path)
#scams_df = sqlContext.read.option('multiLine', True).json(sc.wholeTextFiles(path).values()).drop('id', 'url', 'name','coin', 'description', 'reporter', 'ip', 'nameservers', 'subcategory')
# ([addresses], category, status)
#print(scams_df)
#scams_RDD = scams_df.rdd.map(address_split)
#print(scams_RDD.take(10))
#scams_RDD.collect().foreach(println)
# (address i, (category, status))
scams_RDD= scams_df.map(lambda f: (f.split(',')[0], f.split(',')[1]))
lines_tra = sc.textFile('/data/ethereum/transactions')
clean_lines_tra = lines_tra.filter(validateTransaction)
address_val_pair = clean_lines_tra.map(lambda l: (l.split(',')[2], (float(l.split(',')[3]), time.strftime("%y.%m", time.gmtime(float(l.split(',')[6]))))))
joined_RDD = address_val_pair.join(scams_RDD)
# (to add, ((val, time), (cat, status)))
#print(joined_RDD.take(10))
# most lucrative form of scam
key_cat = joined_RDD.map(lambda x: (x[1][1][0], x[1][0][0]))
print(key_cat)
most_lucrative_cat = key_cat.reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).collect()
print("Test4")
print(len(most_lucrative_cat))
for rec in most_lucrative_cat:
    print(rec)
