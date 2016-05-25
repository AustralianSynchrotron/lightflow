import os
import sys
import logging.config
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

        if 'logging' in self.config:
            logging.config.dictConfig(self.config['logging'])

        # append the workflow paths to the PYTHONPATH
        for workflow_path in self.config['workflows']:
            if os.path.isdir(os.path.abspath(workflow_path)):
                if workflow_path not in sys.path:
                    sys.path.append(workflow_path)
            else:
                # logger.error('DAG directory {} does not exist!'.format(workflow_path))
                print('DAG directory {} does not exist!'.format(workflow_path))

    def get(self, key):
        return self.config[key]

    def update_from_file(self, filename):
        with open(filename, 'r') as config_file:
            self.config.update(yaml.load(config_file.read()))

    def default(self):
        return """
    workflows:
      - ./dags

    celery:
      broker: redis://localhost:6379/0
      backend: redis://localhost:6379/0

    datastore:
      host: localhost
      port: 1234
      database_name: lightflow_db
      username: none
      password: none

    logging:
      version: 1
      disable_existing_loggers: false
      formatters:
        standard:
          format: '[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d | %(message)s'
          datefmt: '%H:%M:%S'
      handlers:
        console:
          class: logging.StreamHandler
          level: INFO
          formatter: standard

      root:
        handlers:
          - console
        level: INFO
    """
