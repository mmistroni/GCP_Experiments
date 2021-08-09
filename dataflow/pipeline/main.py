import logging
from modules.edgar_quarterly_form4 import run 

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()