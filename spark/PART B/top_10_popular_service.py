import pyspark
import time


# This function checks good lines of transactions

sc = pyspark.SparkContext()


def validateTransaction(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        float(fields[3])
        return True
    except:
        pass


transacs = sc.textFile("/data/ethereum/transactions")
validated_Transactions = transacs.filter(validateTransaction)
to_address = validated_Transactions.map(lambda a: (a.split(',')[2], float(a.split(',')[3])))
aggregate = to_address.reduceByKey(lambda x,y: x+y)

def validateContract(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        return True
    except:
        pass

contracts = sc.textFile("/data/ethereum/contracts")
validated_contracts = contracts.filter(validateContract)
contract_address = validated_contracts.map(lambda a: a.split(',')[0])
joinDataset = aggregate.join(contract_address)
top10Results = joinDataset.takeOrdered(10, key = lambda x: -x[1][0])


for record in top10Results:
    print("{}: {}".format(record[0],record[1]))
