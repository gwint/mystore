from threading import Lock, get_ident

class LockHandler:
    def __init__(self, numLocks):
        self._perThreadAcquiredLocks = {}
        self._allStoredLocks = [Lock() for _ in range(numLocks)]

    def acquireLocks(self, *args):
        if len(args) == 0:
            raise ValueError("Must provide the name of at least one Lock")

        lockNames = sorted(args)

        currThreadId = get_ident()

        for name in lockNames:
            if name >= len(self._allStoredLocks):
                raise ValueError("Cannot acquire lock: invalid lock name provided: %d" % name)

            lockToAcquire = self._allStoredLocks[name]

            currentThreadLocks = \
                    self._perThreadAcquiredLocks.get(currThreadId, set())

            if name in currentThreadLocks:
                raise ValueError("Thread %d has attempted to acquire lock %s, which it has previously acquired" % (currThreadId, name))

            if currThreadId not in self._perThreadAcquiredLocks:
                self._perThreadAcquiredLocks[currThreadId] = set()

            self._perThreadAcquiredLocks[currThreadId].add(name)

            lockToAcquire.acquire()

    def releaseLocks(self, *args):
        if len(args) == 0:
            raise ValueError("The name of at least one lock must be provided when attempting to call releaseLocks")

        lockNames = sorted(args)[::-1]

        currThreadId = get_ident()

        for name in lockNames:
            lockToRelease = self._allStoredLocks[name]
            lockToRelease.release()
            self._perThreadAcquiredLocks[currThreadId].remove(name)

    def __str__(self):
        return "Lock Elements: %s" % self._locks


if __name__ == "__main__":
    handler = LockHandler(3)

    handler.acquireLocks(0,2)
    print("This message should be seen")
    handler.acquireLocks(0)

