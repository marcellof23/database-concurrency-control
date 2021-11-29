from mvcc import *
from ts import *


def main():

    mvcc = Simulation()
    mvcc.ts.populateTransactionLog(sys.argv[1])
    mvcc.ts.setTransactionsTimestamp(mvcc.ts.log)
    mvcc.ts.setTransactionsData(mvcc.ts.log)
    mvcc.simulate()


if __name__ == '__main__':
    main()
