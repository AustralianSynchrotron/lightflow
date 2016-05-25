import os
import sys
import logging.config
import ruamel.yaml as yaml


class __ConfigSingleton:
    """The singleton class for the Settings.

    Contains a dictionary with the values read from the configuration file.
    """
    d = {}
    loaded = False


def Config():
    """Method that returns the singleton object for the settings."""
    conf = __ConfigSingleton()
    if not conf.loaded:
        conf.d = yaml.load(default())

        if 'LIGHTFLOW_CONFIG' not in os.environ:
            if os.path.isfile(os.path.join(os.getcwd(), 'lightflow.cfg')):
                update_from_file(os.path.join(os.getcwd(), 'lightflow.cfg'))
            elif os.path.isfile(expand_env_var('~/lightflow.cfg')):
                update_from_file(expand_env_var('~/lightflow.cfg'))
            else:
                print('No config found!!!') # TODO: logging
        else:
            update_from_file(expand_env_var(os.environ['LIGHTFLOW_CONFIG']))

        if 'logging' in conf.d:
            logging.config.dictConfig(conf.d['logging'])

        # append the workflow paths to the PYTHONPATH
        for workflow_path in conf.d['workflows']:
            if os.path.isdir(workflow_path):
                if workflow_path not in sys.path:
                    sys.path.append(workflow_path)
            else:
                #logger.error('DAG directory {} does not exist!'.format(workflow_path))
                print('DAG directory {} does not exist!'.format(workflow_path))

        conf.loaded = True

    return conf.d


def update_from_file(filename):
    with open(filename, 'r') as config_file:
        __ConfigSingleton().d.update(yaml.load(config_file.read()))


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


def default():
    return """
workflows:
  - /home/molla/projects/lightflow/dags

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
