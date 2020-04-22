

class Task:
    """
    Base class for a single task
    """
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self):
        return self._name

    def run(self):
        print('I am task')
