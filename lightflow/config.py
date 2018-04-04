import os
import sys
import ruamel.yaml as yaml

from lightflow.models.exceptions import ConfigLoadError, ConfigFieldError


LIGHTFLOW_CONFIG_ENV = 'LIGHTFLOW_CONFIG'
LIGHTFLOW_CONFIG_NAME = 'lightflow.cfg'


def expand_env_var(env_var):
    """ Expands, potentially nested, environment variables.

        Args:
            env_var (str): The environment variable that should be expanded.

        Returns:
            str: The fully expanded environment variable.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


class Config:
    """ Hosts the global configuration.

    The configuration is read from a structured YAML file or a dictionary.
    The location of the file can either be specified directly, is given in
    the environment variable LIGHTFLOW_CONFIG_ENV, is looked for in the
    current execution directory or in the home directory of the user.
    """
    def __init__(self):
        self._config = None

    @classmethod
    def from_file(cls, filename, *, strict=True):
        """ Create a new Config object from a configuration file.

        Args:
            filename (str): The location and name of the configuration file.
            strict (bool): If true raises a ConfigLoadError when the configuration
                cannot be found.

        Returns:
            An instance of the Config class.

        Raises:
            ConfigLoadError: If the configuration cannot be found.
        """
        config = cls()
        config.load_from_file(filename, strict=strict)
        return config

    def load_from_file(self, filename=None, *, strict=True):
        """ Load the configuration from a file.

        The location of the configuration file can either be specified directly in the
        parameter filename or is searched for in the following order:

            1. In the environment variable given by LIGHTFLOW_CONFIG_ENV
            2. In the current execution directory
            3. In the user's home directory

        Args:
            filename (str): The location and name of the configuration file.
            strict (bool): If true raises a ConfigLoadError when the configuration
                cannot be found.

        Raises:
            ConfigLoadError: If the configuration cannot be found.
        """
        self.set_to_default()

        if filename:
            self._update_from_file(filename)
        else:
            if LIGHTFLOW_CONFIG_ENV not in os.environ:
                if os.path.isfile(os.path.join(os.getcwd(), LIGHTFLOW_CONFIG_NAME)):
                    self._update_from_file(
                        os.path.join(os.getcwd(), LIGHTFLOW_CONFIG_NAME))
                elif os.path.isfile(expand_env_var('~/{}'.format(LIGHTFLOW_CONFIG_NAME))):
                    self._update_from_file(
                        expand_env_var('~/{}'.format(LIGHTFLOW_CONFIG_NAME)))
                else:
                    if strict:
                        raise ConfigLoadError('Could not find the configuration file.')
            else:
                self._update_from_file(expand_env_var(os.environ[LIGHTFLOW_CONFIG_ENV]))

        self._update_python_paths()

    def load_from_dict(self, conf_dict=None):
        """ Load the configuration from a dictionary.

        Args:
            conf_dict (dict): Dictionary with the configuration.
        """
        self.set_to_default()
        self._update_dict(self._config, conf_dict)
        self._update_python_paths()

    def to_dict(self):
        """ Returns a copy of the internal configuration as a dictionary. """
        return dict(self._config)

    @property
    def workflows(self):
        """ Return the workflow folders """
        return self._config.get('workflows')

    @property
    def data_store(self):
        """ Return the data store settings """
        return self._config.get('store')

    @property
    def signal(self):
        """ Return the signal system settings """
        return self._config.get('signal')

    @property
    def logging(self):
        """ Return the logging settings """
        return self._config.get('logging')

    @property
    def celery(self):
        """ Return the celery settings """
        return self._config.get('celery')

    @property
    def cli(self):
        """ Return the cli settings """
        return self._config.get('cli')

    @property
    def extensions(self):
        """ Return the custom settings of extensions """
        if 'extensions' not in self._config:
            raise ConfigFieldError(
                'The extensions section is missing in the configuration')
        return self._config.get('extensions')

    @property
    def workflow_polling_time(self):
        """ Return the waiting time between status checks of the running dags (sec) """
        if 'graph' not in self._config:
            raise ConfigFieldError('The graph section is missing in the configuration')
        return self._config.get('graph').get('workflow_polling_time')

    @property
    def dag_polling_time(self):
        """ Return the waiting time between status checks of the running tasks (sec) """
        if 'graph' not in self._config:
            raise ConfigFieldError('The graph section is missing in the configuration')
        return self._config.get('graph').get('dag_polling_time')

    def set_to_default(self):
        """ Overwrite the configuration with the default configuration. """
        self._config = yaml.safe_load(self.default())

    def _update_from_file(self, filename):
        """ Helper method to update an existing configuration with the values from a file.

        Loads a configuration file and replaces all values in the existing configuration
        dictionary with the values from the file.

        Args:
            filename (str): The path and name to the configuration file.
        """
        if os.path.exists(filename):
            try:
                with open(filename, 'r') as config_file:
                    yaml_dict = yaml.safe_load(config_file.read())
                    if yaml_dict is not None:
                        self._update_dict(self._config, yaml_dict)
            except IsADirectoryError:
                raise ConfigLoadError(
                    'The specified configuration file is a directory not a file')
        else:
            raise ConfigLoadError('The config file {} does not exist'.format(filename))

    def _update_dict(self, to_dict, from_dict):
        """ Recursively merges the fields for two dictionaries.

        Args:
            to_dict (dict): The dictionary onto which the merge is executed.
            from_dict (dict): The dictionary merged into to_dict
        """
        for key, value in from_dict.items():
            if key in to_dict and isinstance(to_dict[key], dict) and \
                    isinstance(from_dict[key], dict):
                self._update_dict(to_dict[key], from_dict[key])
            else:
                to_dict[key] = from_dict[key]

    def _update_python_paths(self):
        """ Append the workflow and libraries paths to the PYTHONPATH. """
        for path in self._config['workflows'] + self._config['libraries']:
            if os.path.isdir(os.path.abspath(path)):
                if path not in sys.path:
                    sys.path.append(path)
            else:
                raise ConfigLoadError(
                    'Workflow directory {} does not exist'.format(path))

    @staticmethod
    def default():
        """ Returns the default configuration. """
        return '''
workflows:
  - ./examples

libraries: []

celery:
  broker_url: redis://localhost:6379/0
  result_backend: redis://localhost:6379/0
  worker_concurrency: 8
  result_expires: 0
  worker_send_task_events: True
  worker_prefetch_multiplier: 1

signal:
  host: localhost
  port: 6379
  password: null
  database: 0
  polling_time: 0.5

store:
  host: localhost
  port: 27017
  database: lightflow
  username: null
  password: null
  auth_source: admin
  auth_mechanism: SCRAM-SHA-1

graph:
  workflow_polling_time: 0.5
  dag_polling_time: 0.5

cli:
  time_format: '%d/%m/%Y %H:%M:%S'

extensions: {}

logging:
  version: 1
  disable_existing_loggers: false
  formatters:
    verbose:
      format: '[%(asctime)s][%(levelname)s] %(name)s %(filename)s:%(funcName)s:%(lineno)d | %(message)s'
      datefmt: '%d/%m/%Y %H:%M:%S'
    simple:
      (): 'colorlog.ColoredFormatter'
      format: '%(log_color)s[%(asctime)s][%(levelname)s] %(blue)s%(processName)s%(reset)s | %(message)s'
      datefmt: '%d/%m/%Y %H:%M:%S'
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
    '''
