import re
from lightflow.tasks import TriggerTask
from lightflow.models import Action


class ChunkingTask(TriggerTask):
    """ The Chunking task divides up lists based on patterns. """

    def __init__(self, name, dag_name, key=None, pattern=None, decimate=1, force_run=False, propagate_skip=True):
        """ Initialise the Chunking task.

        Args:
            name (str): The name of the task.
            key: Key to the data object containing list to chunk.
            pattern: The pattern used to match like entries.
            force_run (bool): Run the task even if it is flagged to be skipped.
            propagate_skip (bool): Propagate the skip flag to the next task.
        """
        super().__init__(name, dag_name, force_run, propagate_skip)
        self._key = key
        self._pattern = pattern
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

            for num, item in enumerate(new_list):
                match = re.search(self._pattern, item)
                try:
                    previous = match.groupdict()['match']
                except KeyError:
                    previous = match.group(0)
                except AttributeError:
                    continue

                chunked_list = [[item]]
                break

            for item in new_list[num + 1:]:
                if item == '|':
                    current = item
                else:
                    match = re.search(self._pattern, item)
                    try:
                        current = match.groupdict()['match']
                    except KeyError:
                        current = match.group(0)
                    except AttributeError:
                        continue

                if current == previous:
                    chunked_list[-1].append(item)
                else:
                    if item != '|':
                        chunked_list.append([item])
                    else:
                        pass

                previous = current

            data_store.set('chunking_list', chunked_list.pop())

            for chunk in chunked_list:
                data[self._key] = chunk[::self._decimate if self._decimate > len(chunk) else None]
                signal.run_dag(self._dag_name, data=data)
