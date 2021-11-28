import { Cache } from './cache';
import { Database } from './database';
import { SerialDatabase } from './serialDatabase';

type Transaction = (cache: Cache) => void;

class TransactionExecutor {
    private cache: Cache;
    private time: number;
    constructor(private db: SerialDatabase, private tx: Transaction) {
        this.cache = new Cache(db);
        this.time = this.db.getTime();
    }

    readPhase() {
        this.tx(this.cache);
    }

    validateAndWritePhase(): boolean {
        const finishTime: number = this.db.getTime();
        for (let i = this.time + 1; i < finishTime + 1; i++) {
            const cache = this.db.getTransaction(i);
            const writeSet = [...cache.getWriteSet()];
            const readSet = [...this.cache.getReadSet()];
            const intersect = writeSet.filter((each) => readSet.includes(each));
            /* If Not Disjoint */
            if (intersect.length != 0) {
                return false;
            }
        }
        this.db.commitTransaction(this.cache);
        return true;
    }
}

export { Transaction, TransactionExecutor };
