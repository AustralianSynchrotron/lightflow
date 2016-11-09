
class WorkflowArgumentError(RuntimeError):
    pass


class WorkflowImportError(RuntimeError):
    pass


class DirectedAcyclicGraphInvalid(RuntimeError):
    pass


class DataStoreNotConnected(RuntimeError):
    pass


class DataStoreIDExists(RuntimeError):
    pass


class DataStoreIDInvalid(RuntimeError):
    pass


class DataStoreGridfsIdInvalid(RuntimeError):
    pass


class DataStoreDecodeUnknownType(RuntimeError):
    pass


class TaskReturnActionInvalid(RuntimeError):
    pass


class RequestActionUnknown(RuntimeError):
    pass


class RequestFailed(RuntimeError):
    pass


class DagNameUnknown(RuntimeError):
    pass
