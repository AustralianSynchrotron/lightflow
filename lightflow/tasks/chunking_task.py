import re
from lightflow.models import BaseTask
from lightflow.models import Action


def indices(lst, element, value=False):
    """ Returns the indices, or optionally the value, for all occurrences of 'element' in 'lst'.

    Args:
        lst (list): List to search.
        element:  Element to find.
        value: If True return the list value instead of the element.

    Returns:
        list: List of indices or values
    """
    result = []
    offset = -1
    while True:
        try:
            offset = lst.index(element, offset+1)
        except ValueError:
            return result
        result.append(offset if not value else lst[offset])


class ChunkingTask(BaseTask):
    """ The Chunking task divides up lists based on patterns. """

    def __init__(self, name, dag_name, key, match_pattern,
                 flush_string='|', flush_on_end=True,
                 force_consecutive=True, decimate=1,
                 force_run=False, propagate_skip=True):
        """ Initialise the Chunking task.

        Args:
            name (str): The name of the task.
            dag_name (str): Dag to run with eat chunk.
            key: Key to the data object containing list to chunk, and to overwrite with chunk.
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
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, force_run, propagate_skip)
        self._dag_name = dag_name
        self._key = key
        self._pattern = match_pattern
        self._flush_string = flush_string
        self._flush_on_end = flush_on_end if force_consecutive is True else True
        self._force_consecutive = force_consecutive
        self._decimate = decimate

    def run(self, data, data_store, signal, **kwargs):
        """ The main run method of the Chunking task.

        Args:
            data (MultiTaskData): The data object that has been passed from the
                                  predecessor task.
            data_store (DataStore): The persistent data store object that allows the task
                                    to store data for access across the current workflow
                                    run.
            signal (TaskSignal): The signal object for tasks. It wraps the construction
                                 and sending of signals into easy to use methods.

        Returns:
            Action: An Action object containing the data that should be passed on
                    to the next task and optionally a list of successor tasks that
                    should be executed.
        """
        if self._pattern is not None and self._key is not None:
            try:
                saved_list = data_store.get('chunking_list')
            except KeyError:
                saved_list = []

            try:
                new_list = data[self._key]
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
                    try:
                        matches.append(re.search(self._pattern, item).groups(0)[0])
                    except AttributeError:
                        matches.append(None)

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
                    data_store.set('chunking_list', chunked_list.pop())

            else:
                unique_matches = set(matches)
                chunked_indices = [indices(matches, unique) for unique in unique_matches]
                chunked_list = [[new_list[i] for i in cis] for cis in chunked_indices]

            for chunk in chunked_list:
                data[self._key] = chunk[::self._decimate if self._decimate > len(chunk) else None]
                signal.run_dag(self._dag_name, data=data)
