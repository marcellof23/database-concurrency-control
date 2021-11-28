import { Transaction, TransactionExecutor } from './core/transaction';
import { Cache } from './core/cache';
import { DataKey } from './core/database';
import { SerialDatabase } from './core/serialDatabase';
import AssertPlus from 'assert-plus';

const initData: Transaction = (db: Cache) => {
    db.write('haha', 100);
    db.write('hihi', 200);
    db.write('huhu', 300);
};

const transactionGenerator: (k: DataKey[], v: number) => Transaction = (keys, value) => {
    return (db: Cache) => {
        keys.forEach((each) => {
            const val: number = db.read(each) as number;
            db.write(each, val + value);
        });
    };
};

const increaseHaha: Transaction = transactionGenerator(['haha'], 25);
const increaseHihi: Transaction = transactionGenerator(['hihi'], 75);
const increaseHahihu: Transaction = transactionGenerator(['haha', 'hihi', 'huhu'], 25);

const db: SerialDatabase = new SerialDatabase();
AssertPlus.ok(db.equal({}));

const txInit: TransactionExecutor = db.begin(initData);
txInit.readPhase();
AssertPlus.ok(txInit.validateAndWritePhase());
AssertPlus.ok(db.equal({ haha: 100, hihi: 200, huhu: 300 }));

const tx1: TransactionExecutor = db.begin(increaseHahihu);
const tx2: TransactionExecutor = db.begin(increaseHahihu);
tx1.readPhase();
tx2.readPhase();
AssertPlus.ok(tx1.validateAndWritePhase());
AssertPlus.ok(!tx2.validateAndWritePhase());
AssertPlus.ok(db.equal({ haha: 125, hihi: 225, huhu: 325 }));

const tx3: TransactionExecutor = db.begin(increaseHaha);
const tx4: TransactionExecutor = db.begin(increaseHihi);
tx3.readPhase();
tx4.readPhase();
AssertPlus.ok(tx3.validateAndWritePhase());
AssertPlus.ok(tx4.validateAndWritePhase());
AssertPlus.ok(db.equal({ haha: 150, hihi: 300, huhu: 325 }));

console.info(db.toString());
