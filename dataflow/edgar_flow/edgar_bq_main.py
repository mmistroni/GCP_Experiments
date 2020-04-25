from modules.edgar_bq_reader import run
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()