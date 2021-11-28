import AssertPlus from 'assert-plus';

type DataValue = number | string | object | boolean;
type DataKey = string;

interface DataStorage {
    [key: DataKey]: DataValue;
}

class Database {
    protected storage: DataStorage;

    constructor() {
        this.storage = {};
    }

    getStorage(): DataStorage {
        return { ...this.storage };
    }

    toString(): string {
        return JSON.stringify(this.storage, null, 2);
    }

    write(key: DataKey, val: DataValue) {
        this.storage[key] = val;
    }

    read(key: DataKey): DataValue {
        AssertPlus.ok(this.storage.hasOwnProperty(key), `Key '${key}' is not found.`);

        return this.storage[key];
    }
}

export { Database, DataStorage, DataKey, DataValue };
