# read and log the version of package
import os
import logging
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='webhdfs')

version = "latest"

try:
    fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
    version = open(fn).read().strip()
    logger.debug("Loading WebHDFS version: " + version)
except IOError:
    pass
