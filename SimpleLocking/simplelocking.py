# Author: Rafi Raihansyah Munandar / 13519154

import copy
import sys

lockTableItems = []
transactionTableItems = []
waitingTransactionItems = []

# Transaction States
A = "Available"
W = "Waiting"
C = "Committed"

# Available Items
ITEMS = ["X", "Y", "Z"]


class lockTable:
    def __init__(self, lockedDataItem, transactionID):
        self.lockedDataItem = lockedDataItem
        self.lockHeldBy = transactionID


class transactionTable:
    def __init__(self, transactionID, transactionState):
        self.transactionID = transactionID
        self.transactionState = transactionState
        self.lockedItems = []
        self.blockedOperation = []

    def addBlockedOperation(self, operation):
        self.blockedOperation.append(operation)

    def setBlockedOperation(self, blockedOps):
        self.blockedOperation = blockedOps

    def addLockedItem(self, item):
        self.lockedItems.append(item)

    def changeTransactionState(self, state):
        self.transactionState = state

# Utilities


def getTransNumber(op: str):
    transNumber = ""
    for char in op:
        if char.isdigit():
            transNumber += char

    return int(transNumber)


def getOperationData(op: str, itemList: list[str]):
    item = ""
    for char in itemList:
        if op.find(char) != -1:
            item = char
    transNumber = getTransNumber(op)

    return item, transNumber


def findTransaction(transNumber: int) -> transactionTable:
    for trans in transactionTableItems:
        if trans.transactionID == transNumber:
            return trans


# Operations

def operateBegin(op: str):
    transID = getTransNumber(op)
    print(f"[B{transID}]\t Beginning Transaction Number {transID}")
    transactionTableItems.append(transactionTable(transID, A))
    return


def opereateRead(op: str):
    item, transNumber = getOperationData(op, ITEMS)
    print(
        f"[R{transNumber}({item})]\t Executing READ on {item} in Transaction {transNumber}")

    if len(lockTableItems) != 0:
        for lockedItem in lockTableItems:
            if lockedItem.lockedDataItem == item and lockedItem.lockHeldBy != transNumber:
                print(
                    f"[!]\t Conflicting Lock: Item '{item}' is locked by {lockedItem.lockHeldBy}")
                print("[!]\t Calling Waiting Procedure")
                handleWait(findTransaction(transNumber),
                           findTransaction(lockedItem.lockHeldBy), op)


def operateWrite(op: str):
    item, transNumber = getOperationData(op, ITEMS)
    print(
        f"[W{transNumber}({item})]\t Executing WRITE on {item} in Transaction {transNumber}")
    conflict = False

    if len(lockTableItems) == 0:
        lockTableItems.append(lockTable(item, transNumber))
        findTransaction(transNumber).addLockedItem(item)
        print(
            f"[XL({item})]\t Locking item '{item}' under Transaction {transNumber}")
    else:
        for lockedItem in lockTableItems:
            if lockedItem.lockedDataItem == item and lockedItem.lockHeldBy != transNumber:
                conflict = True
                print(
                    f"[!]\t Conflicting Lock: Item '{item}' is locked by {lockedItem.lockHeldBy}")
                print("[!]\t Calling Waiting Procedure")
                handleWait(findTransaction(transNumber),
                           findTransaction(lockedItem.lockHeldBy), op)
        if not conflict:
            lockTableItems.append(lockTable(item, transNumber))
            findTransaction(transNumber).addLockedItem(item)
            print(
                f"[XL({item})]\t Locking item '{item}' under Transaction {transNumber}")


def operateCommit(op: str):
    transNumber = getTransNumber(op)
    print(f"[C{transNumber}]\t Committing Transaction {transNumber}")
    for trans in transactionTableItems:
        if trans.transactionID == transNumber:
            trans.changeTransactionState(C)
    handleUnlock(transNumber)


def checkWaiting(op: str):
    item, transNumber = getOperationData(op, ITEMS)
    for trans in transactionTableItems:
        if trans.transactionID == transNumber and trans.transactionState == W:
            trans.addBlockedOperation(op)
            return True
    return False


def checkOperation(op: str):
    skip = False
    skip = checkWaiting(op)
    if not skip:
        if op.find('B') != -1:      # Begin operation
            operateBegin(op)
        elif op.find('C') != -1:    # Commit operation
            operateCommit(op)
        elif op.find('W') != -1:    # Read write operation
            operateWrite(op)
        elif op.find('R') != -1:    # Read write operation
            opereateRead(op)

# Handlers


def handleWait(req: transactionTable, hold: transactionTable, op: str):
    print(
        f"[!]\t Changing Transaction {req.transactionID} state to 'Waiting'")
    req.changeTransactionState(W)
    if not op in req.blockedOperation:
        req.addBlockedOperation(op)
    if not req in waitingTransactionItems:
        waitingTransactionItems.append(req)


def handleUnlock(transNumber: int):
    print(f"[!]\t Unlocking all items under Transaction {transNumber}")
    for trans in transactionTableItems:
        if trans.transactionID == transNumber:
            for lockedItem in trans.lockedItems:
                for lockTable in lockTableItems:
                    if lockTable.lockedDataItem == lockedItem:
                        lockTableItems.remove(lockTable)

    handleResumeWaiting()


def handleResumeWaiting():
    print("[?]\t Checking for any waiting transaction that can be resumed after freeing item...")
    if len(waitingTransactionItems) == 0:
        print("[!]\t No waiting transaction found.")
        return
    for waitingTrans in waitingTransactionItems:
        tempBlockedOp = copy.deepcopy(waitingTrans.blockedOperation)
        for blockedOperation in waitingTrans.blockedOperation:
            waitingTrans.changeTransactionState(A)
            print("[?]\t Attempting operation " +
                  blockedOperation[:-1] + "...")
            checkOperation(blockedOperation)
            if waitingTrans.transactionState != W:
                tempBlockedOp.remove(blockedOperation)
        waitingTrans.setBlockedOperation(tempBlockedOp)

        if (len(waitingTrans.blockedOperation) == 0):
            waitingTransactionItems.remove(waitingTrans)


def main():
    if len(sys.argv) != 2:
        print("Please specify a filename: python simplelocking.py <filename>")
        return
    filename = sys.argv[1]
    try:
        with open(filename, 'r') as input:
            for operation in input:
                checkOperation(operation)
    except:
        print(f"File not found! Please try again")


main()
