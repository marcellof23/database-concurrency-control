from base import *
from ts import *


class Simulation:

    def __init__(self):
        self.ts = Timestamp()

    def Read(self, timestamp, ts_name, data_name, data_read_TS, data_write_TS):

        ver = self.ts.getLatestTimestamp(data_name)
        print("\tExecuting read transaction operation!\n\tSetting new read timestamp for item {0}...".format(
            data_name))
        self.ts.setDataReadTimestamp(data_name, max(
            data_read_TS, timestamp), ver)
        self.ts.setTransactionStatus(ts_name, 'pass')
        print("\tRead timestamp for item '{0}' is now {1}".format(
            data_name, self.ts.getDataReadTimestamp(data_name, ver)))
        return 'executed'

    def Write(self, timestamp, ts_name, data_name, data_read_TS, data_write_TS):

        if timestamp < data_read_TS:
            print("\tRejecting write transaction operation!\n\tRunning rollback on transaction {0}...".format(
                ts_name))
            self.ts.setTransactionStatus(ts_name, 'abort')
            return 'rejected'
        else:
            print("\tExecuting write transaction operation!\n\tSetting new write timestamp for item {0}...".format(
                data_name))
            self.ts.setLatestTimeStamp(data_name,  timestamp)
            ver = self.ts.getLatestTimestamp(data_name)
            self.ts.setDataReadTimestamp(
                data_name, timestamp, ver)
            self.ts.setDataWriteTimestamp(
                data_name, timestamp,  ver)
            self.ts.setTransactionStatus(ts_name, 'pass')

            print("\tWrite timestamp for item '{0}' is now {1}".format(
                data_name, self.ts.getDataWriteTimestamp(data_name, ver)))
            return 'executed'

    def simulate(self):

        print("\n================== MVCC SIMULATION ==================")
        for op in self.ts.log:
            ts_name = op['transaction']
            ts_action = op['action']
            ts_status = self.ts.ts_mapper[ts_name]['status']
            data_name = op['data']
            latest_ts = self.ts.getLatestTimestamp(data_name)
            timestamp = self.ts.getTimestamp(ts_name)
            data_read_TS = self.ts.getDataReadTimestamp(
                data_name, latest_ts)
            data_write_TS = self.ts.getDataWriteTimestamp(
                data_name, latest_ts)
            curr_ver = 0
            if(ts_action == 'read'):
                curr_ver = latest_ts
                if(curr_ver > timestamp):
                    for idx in self.ts.data[data_name]['ver']:
                        if(int(idx) >= curr_ver):
                            break
                        else:
                            curr_ver = int(idx)
            else:
                curr_ver = max(latest_ts, timestamp)

            print("\n>>> Transaction {0} Version {3} execute operation {1} for item {2}".format(
                ts_name, ts_action.upper(), data_name, curr_ver))
            if ts_status != 'abort':
                if ts_action == 'read':
                    op['status'] = self.Read(
                        timestamp, ts_name, data_name, data_read_TS, data_write_TS)
                elif ts_action == 'write':
                    op['status'] = self.Write(
                        timestamp, ts_name, data_name, data_read_TS, data_write_TS)
                else:
                    print("Unknown property of operation '{0}' at the transaction {1}!".format(
                        ts_action, ts_name))
            else:
                print(
                    "Cannot run a specitif transaction {0}, since it has been aborted".format(ts_name))

        print("\n==================  END OF SIMULATION  ==================\n")
