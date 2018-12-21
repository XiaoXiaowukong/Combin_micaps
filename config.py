import logging
import logging.config
# from pymongo import MongoClient

config = {"key1":"value1"}

logging.config.fileConfig("logger.conf")
logger = logging.getLogger("cse")