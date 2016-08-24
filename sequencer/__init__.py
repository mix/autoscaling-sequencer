__author__ = 'Ankit Chaudhary'
__email__ = 'ankit@mix.com'
__version__ = '0.1.0'

import logging

from logging import FileHandler

logging.getLogger(__name__).addHandler(FileHandler(filename='sequencer.log'))
