import { Database, DataKey, DataStorage, DataValue } from './database';

class Cache {
    private storage: DataStorage = {};
    private readSet: Set<DataKey> = new Set<DataKey>();
    constructor(private db: Database) {}

    write(key: DataKey, val: DataValue) {
        this.storage[key] = val;
    }

    read(key: DataKey): DataValue {
        this.readSet.add(key);
        if (this.storage.hasOwnProperty(key)) {
            return this.storage[key];
        }
        return this.db.read(key);
    }

    commit() {
        const self: Cache = this;
        Object.keys(this.storage).forEach((key) => {
            self.db.write(key, self.storage[key]);
        });
    }

    getReadSet(): Set<DataKey> {
        return new Set(this.readSet);
    }

    getWriteSet(): Set<DataKey> {
        return new Set(Object.keys(this.storage));
    }
}

export { Cache };
