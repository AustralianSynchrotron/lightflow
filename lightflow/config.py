import os
import sys
import ruamel.yaml as yaml


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]


class Config(metaclass=Singleton):
    def __init__(self):
        self.config = yaml.load(self.default())

        if 'LIGHTFLOW_CONFIG' not in os.environ:
            if os.path.isfile(os.path.join(os.getcwd(), 'lightflow.cfg')):
                self.update_from_file(os.path.join(os.getcwd(), 'lightflow.cfg'))
            elif os.path.isfile(expand_env_var('~/lightflow.cfg')):
                self.update_from_file(expand_env_var('~/lightflow.cfg'))
            else:
                print('No config found!!!')  # TODO: logging
        else:
            self.update_from_file(expand_env_var(os.environ['LIGHTFLOW_CONFIG']))

        # append the workflow paths to the PYTHONPATH
        for workflow_path in self.config['workflows']:
            if os.path.isdir(os.path.abspath(workflow_path)):
                if workflow_path not in sys.path:
                    sys.path.append(workflow_path)
            else:
                # logger.error('DAG directory {} does not exist!'.format(workflow_path))
                print('DAG directory {} does not exist!'.format(workflow_path))

    def get(self, key, default=None):
        return self.config.get(key, default)

    def update_from_file(self, filename):
        with open(filename, 'r') as config_file:
            self.config.update(yaml.load(config_file.read()))

    @staticmethod
    def default():
        return """
    workflows:
      - ./examples

    celery:
      broker: redis://localhost:6379/0
      backend: redis://localhost:6379/0

    signal:
      host: localhost
      port: 6379
      db: 0

    datastore:
      host: localhost
      port: 27017
      database: lightflow

    logging:
      version: 1
      disable_existing_loggers: false
      formatters:
        verbose:
          format: '[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d | %(message)s'
          datefmt: '%H:%M:%S'
        simple:
          (): 'colorlog.ColoredFormatter'
          format: '%(log_color)s[%(asctime)s][%(levelname)s] %(blue)s%(processName)s%(reset)s | %(message)s'
          datefmt: '%H:%M:%S'
      handlers:
        console:
          class: logging.StreamHandler
          level: INFO
          formatter: simple
      loggers:
        celery:
          handlers:
            - console
          level: INFO

        root:
          handlers:
            - console
          level: INFO
    """
