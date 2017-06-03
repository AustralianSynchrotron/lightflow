
class LightflowException(RuntimeError):
    """ Lightflow base class for all exceptions. """
    def __init__(self, message=''):
        self.message = message

    def __str__(self):
        return self.message

    def __repr__(self):
        return "<LightflowException - {}>".format(self.message)


class ConfigLoadError(RuntimeError):
    """ Raise this if there is a configuration loading error. """
    pass


class ConfigOverwriteError(RuntimeError):
    pass


class ConfigNotDefinedError(RuntimeError):
    pass


class ConfigFieldError(RuntimeError):
    pass


class WorkflowArgumentError(RuntimeError):
    pass


class WorkflowImportError(RuntimeError):
    pass


class DirectedAcyclicGraphInvalid(RuntimeError):
    pass


class DirectedAcyclicGraphUndefined(RuntimeError):
    pass


class DataInvalidIndex(RuntimeError):
    pass


class DataInvalidAlias(RuntimeError):
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


class EventTypeUnknown(RuntimeError):
    pass


class JobEventTypeUnsupported(RuntimeError):
    pass


class WorkerEventTypeUnsupported(RuntimeError):
    pass


class JobStatInvalid(RuntimeError):
    pass


class AbortWorkflow(LightflowException):
    pass


class StopTask(LightflowException):
    def __init__(self, message='', *, skip_successors=True):
        super().__init__(message)
        self.skip_successors = skip_successors
