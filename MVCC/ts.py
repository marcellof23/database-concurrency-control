from base import *


class Timestamp:
    def __init__(self):
        self.log = []
        self.data = {}
        self.ts_mapper = {}

    def getTimestamp(self, ts_name):
        try:
            return self.ts_mapper[ts_name]['timestamp']
        except (KeyError, NameError):
            print("No transaction found!")

    def setTimestamp(self, ts, value):
        ts["timestamp"] = value

    def getDataWriteTimestamp(self, data_name, ver):
        if(str(ver) in self.data[data_name]['ver']):
            return self.data[data_name]['ver'][str(ver)]['w_ts']
        else:
            return 0

    # def getLargestReadTimestamp(self, data_name, ver, )

    def getDataReadTimestamp(self, data_name, ver):
        if(str(ver) in self.data[data_name]['ver']):
            return self.data[data_name]['ver'][str(ver)]['r_ts']
        else:
            return 0

    def getLatestTimestamp(self, data_name):
        return self.data[data_name]['latest_ts']

    def setDataWriteTimestamp(self, data_name, value, ver):
        self.data[data_name]['ver'][str(ver)] = {
            'w_ts': value, 'r_ts': self.getDataReadTimestamp(data_name, ver)}

    def setDataReadTimestamp(self, data_name, value, ver):
        self.data[data_name]['ver'][str(ver)] = {
            'w_ts': self.getDataWriteTimestamp(data_name, ver), 'r_ts': value}

    def setLatestTimeStamp(self, data_name, value):
        self.data[data_name]['latest_ts'] = value

    def mapNewTransaction(self, ts_name, value):
        self.ts_mapper[ts_name] = {'timestamp': value, 'status': 'new'}

    def populateTransactionLog(self, file):

        for line in fileinput.FileInput(file):
            info = line.split()
            self.log.append(
                {"transaction": info[0], "action": info[1], "data": info[2], "timestamp": None})

    def setTransactionsTimestamp(self, log_array):

        checked_transactions = []

        for op in log_array:
            ts_name = op['transaction']
            ts_counter = int(ts_name[1:(len(ts_name))])
            if ts_name in checked_transactions:
                self.setTimestamp(op, self.ts_mapper[ts_name]['timestamp'])
            else:
                checked_transactions.append(ts_name)
                self.mapNewTransaction(ts_name, ts_counter)
                self.setTimestamp(op, ts_counter)
        print(self.ts_mapper)

    def setTransactionStatus(self, ts_name, status):

        self.ts_mapper[ts_name]['status'] = status

    def setTransactionsData(self, log_array):
        for op in log_array:
            data_name = op['data']
            if not data_name in self.data:
                self.data[data_name] = {'latest_ts': 0, 'ver': {'0': {'w_ts': 0,
                                                                      'r_ts': 0}}}
