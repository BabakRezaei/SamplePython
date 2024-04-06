import logging
import datetime

# Logger config
logger = logging.getLogger()

formatter = logging.Formatter(
    '%(levelname)s | ciga | %(asctime)s | %(pathname)s | line %(lineno)d | %(message)s')

logger.setLevel(logging.INFO)

current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"logs/app_{current_datetime}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

streamHandler = logging.StreamHandler()
streamHandler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.addHandler(file_handler)
