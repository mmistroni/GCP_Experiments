from modules import edgar_daily_form4
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  edgar_daily_form4.run()