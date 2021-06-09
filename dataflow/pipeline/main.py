import logging
import modules.edgar_quarterly_form4 as ef4

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    ef4.run()