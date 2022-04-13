import pyspark
import time

# This function checks good lines of transactions

sc = pyspark.SparkContext()


def validateTransaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        int(fields[6])
        return True
    except:
        pass


transacs = sc.textFile("/data/ethereum/transactions")
good_transacs = transacs.filter(validateTransaction)
time_epoch = good_transacs.map(lambda a: (int(a.split(',')[6]), int(a.split(',')[3])))
date_month_year = time_epoch.map(lambda t:(time.strftime("%m%y", time.gmtime(t[0])),int(t[1])))

aTuple = (0,0)
rdd1 = date_month_year.aggregateByKey(aTuple, lambda a,b: (a[0] + b,    a[1] + 1),
                                       lambda a,b: (a[0] + b[0], a[1] + b[1]))
finalResult = rdd1.mapValues(lambda v: v[0]/v[1])

finalResult.saveAsTextFile('part_a_2')