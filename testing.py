import logging

from logging.handlers import SysLogHandler

server = '172.16.31.196'
port = 514

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(SysLogHandler(address=(server, port)))
logger.warning('This is a warning message from python script')