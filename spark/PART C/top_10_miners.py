import pyspark
import time

# This function checks good lines of transactions

sc = pyspark.SparkContext()


def validateBlocks(line):
    try:
        fields = line.split(',')
        if len(fields) != 9:
            return False
        int(fields[4])
        return True
    except:
        pass


blocks = sc.textFile("/data/ethereum/blocks")
validated_blocks = blocks.filter(validateBlocks)
time_epoch = validated_blocks.map(lambda a: ((a.split(',')[2]), int(a.split(',')[4])))
aggregateResult = time_epoch.reduceByKey(lambda x, y: x+y)
top10Result = aggregateResult.takeOrdered(10, key=lambda x: -x[1])
for record in top10Result:
    print("{}: {}".format(record[0], record[1]))



