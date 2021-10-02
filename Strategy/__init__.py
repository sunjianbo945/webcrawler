import logging
import datetime

file_name = 'strategy.{day}'.format(day=datetime.datetime.now().strftime('%Y%m%d'))

logger = logging.getLogger(file_name)
hdlr = logging.FileHandler('C:\\Users\\Jianbo\\{file}.log'.format(file=file_name))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)