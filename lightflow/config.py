import json
import jinja2
import logging.config
import ruamel.yaml as yaml
from jinja2 import Template


class __ConfigSingleton:
    """The singleton class for the Settings.

    Contains a dictionary with the values read from the configuration file.
    """
    d = {}


def Config():
    """Method that returns the singleton object for the settings."""
    return __ConfigSingleton().d


def read(filename, template_string=None):
    """Read the settings file and replace the template params.

    Args:
        filename: The filename of the configuration file
        template_string: a JSON string with template arguments for the configuration file
    """
    Config().clear()
    with open(filename, 'r') as config_file:
        if template_string is not None:
            template = Template(config_file.read())
            Config().update(yaml.load(template.render(json.loads(template_string))))
        else:
            Config().update(yaml.load(config_file.read()))

    # set the global logging settings
    logging.config.dictConfig(Config()['logging'])
