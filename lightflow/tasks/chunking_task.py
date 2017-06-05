import re
from lightflow.models import BaseTask
from lightflow.queue import JobType
from lightflow.models.utils import find_indices


class ChunkingTask(BaseTask):
    """ The Chunking task divides up lists based on patterns. """

    def __init__(self, name, dag_name, match_pattern, in_key, out_key=None,
                 flush_string='|', flush_on_end=True,
                 force_consecutive=True, decimate=1,  *,
                 queue=JobType.Task,
                 callback_init=None, callback_finally=None,
                 force_run=False, propagate_skip=True):
        """ Initialise the Chunking task.

        All task parameters except the name, callback, queue, force_run and propagate_skip
        can either be their native type or a callable returning the native type.

        Args:
            name (str): The name of the task.
            dag_name (str): Dag to run with eat chunk.
            in_key: Key to the data object containing list to chunk.
            out_key: Key to the data object to store chunk. If None, list in_key is overwitten with chunk.
            match_pattern (str): The regex pattern used to match like entries. Pattern will be used with
            re.search. Capture groups can be used. If the groups are unnamed the first group is used,
            if a group is named "match" then it will be used.
            flush_string (str): Flush the current chunk if this string is encountered. Only applicable
            in consecutive mode.
            flush_on_end (bool): Flush the final chunk at end of list. Only applicable in consecutive mode.
            force_consecutive (bool): If True only matching entries in list that are consecutive in the list
            will be chunked together. I.e., matching entries that are separated by another valid match will
            end up in different chunks.
            decimate (int): Decimate chunks by this factor. E.g., a decimate value of 2 will only keep every second
            element in the chunk.
            callback_init (callable): A callable that is called shortly before the task
                                      is run. The definition is:
                                        def (data, store, signal, context)
                                      where data the task data, store the workflow
                                      data store, signal the task signal and
                                      context the task context.
            callback_finally (callable): A callable that is always called at the end of
                                         a task, regardless whether it completed
                                         successfully, was stopped or was aborted.
                                         The definition is:
                                           def (status, data, store, signal, context)
                                         where status specifies whether the task was
                                           success: TaskStatus.Success
                                           stopped: TaskStatus.Stopped
                                           aborted: TaskStatus.Aborted
                                           raised exception: TaskStatus.Error
                                         data the task data, store the workflow
                                         data store, signal the task signal and
                                         context the task context.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, queue=queue,
                         callback_init=callback_init, callback_finally=callback_finally,
                         force_run=force_run, propagate_skip=propagate_skip)
        self._dag_name = dag_name
        self._in_key = in_key
        self._out_key = out_key if out_key is not None else in_key
        self._pattern = match_pattern
        self._flush_string = flush_string
        self._flush_on_end = flush_on_end if force_consecutive is True else True
        self._force_consecutive = force_consecutive
        self._decimate = decimate

    def run(self, data, store, signal, context, **kwargs):
        """ The main run method of the Chunking task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            store (DataStoreDocument): The persistent data store object that allows the
                                       task to store data for access across the current
                                       workflow run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.
            context (TaskContext): The context in which the tasks runs.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        if self._pattern is not None and self._in_key is not None:
            saved_list = store.get('chunking_list', default=[])

            try:
                new_list = data[self._in_key]
            except KeyError:
                return
            new_list += saved_list

            matches = []
            if '?P' in self._pattern:
                for item in new_list:
                    try:
                        matches.append(re.search(self._pattern, item).groupdict()['match'])
                    except AttributeError:
                        matches.append(None)
            else:
                for item in new_list:
                    match = re.search(self._pattern, item)
                    if match is None:
                        matches.append(None)
                    else:
                        if len(match.groups()):
                            matches.append(match.groups()[0])
                        else:
                            matches.append(match.group())

            if self._force_consecutive:
                previous = None
                chunked_list = []
                for num, item in enumerate(matches):
                    if new_list[num] == self._flush_string:
                        if num + 1 < len(matches):
                            chunked_list.append([])
                            previous = matches[num + 1]
                        continue
                    elif item is None:
                        continue
                    elif item != previous:
                        chunked_list.append([new_list[num]])
                        previous = item
                    else:
                        chunked_list[-1].append(new_list[num])

                if not self._flush_on_end:
                    store.set('chunking_list', chunked_list.pop())

            else:
                unique_matches = set(matches)
                chunked_indices = [find_indices(matches, unique) for unique in unique_matches]
                chunked_list = [[new_list[i] for i in cis] for cis in chunked_indices]

            for chunk in chunked_list:
                data[self._out_key] = chunk[::self._decimate if self._decimate > len(chunk) else None]
                signal.start_dag(self._dag_name, data=data)
