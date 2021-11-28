import { Database } from './database';
import AssertPlus from 'assert-plus';
import { Cache } from './cache';
import { Transaction, TransactionExecutor } from './transaction';

interface TransactionStorage {
    [key: number]: Cache;
}

class SerialDatabase extends Database {
    private transactions: TransactionStorage = {};
    private time: number = 0;

    getTime(): number {
        return this.time;
    }

    getTransaction(txNum: number): Cache {
        AssertPlus.ok(
            this.transactions.hasOwnProperty(txNum),
            `Transaction Number '${txNum}' is not found.`
        );

        return this.transactions[txNum];
    }

    commitTransaction(cache: Cache) {
        this.time++;
        AssertPlus.ok(!this.transactions.hasOwnProperty(this.time));
        this.transactions[this.time] = cache;
        cache.commit();
    }

    begin(tx: Transaction): TransactionExecutor {
        return new TransactionExecutor(this, tx);
    }

    equal(other: any): boolean {
        AssertPlus.object(other);

        let result = true;
        const otherKeys = Object.keys(other);
        const thisKeys = Object.keys(this.storage);
        otherKeys.forEach((x) => {
            if (!thisKeys.includes(x)) {
                result = false;
            }
        });
        for (const each in thisKeys.values()) {
            if (!otherKeys.includes(each) || !result) {
                result = false;
                console.info(`${each}`);
                break;
            }

            result = result && this.storage[each] == other[each];
        }
        return result;
    }
}

export { SerialDatabase };
