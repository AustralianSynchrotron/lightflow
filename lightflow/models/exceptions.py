
class ImportWorkflowError(RuntimeError):
    pass


class DirectedAcyclicGraphInvalid(RuntimeError):
    pass


class DataStoreNotConnected(RuntimeError):
    pass


class DataStoreIDExists(RuntimeError):
    pass


class DataStoreIDInvalid(RuntimeError):
    pass


class TaskReturnActionInvalid(RuntimeError):
    pass


class RequestActionUnknown(RuntimeError):
    pass


class DagNameUnknown(RuntimeError):
    pass
