import pyspark
import time

# This function checks good lines of transactions

sc = pyspark.SparkContext()


def validateTransaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        int(fields[4])
        return True
    except:
        pass


transacs = sc.textFile("/data/ethereum/blocks")
validated_transacs = transacs.filter(validateTransaction)
time_epoch = validated_transacs.map(lambda a: ((a.split(',')[2]), int(a.split(',')[4])))
aggregateResult = time_epoch.reduceByKey(lambda x, y: x+y)
top10Result = aggregateResult.takeOrdered(10, key=lambda x: -x[1])
for record in top10Result:
    print("{}: {}".format(record[0], record[1]))

