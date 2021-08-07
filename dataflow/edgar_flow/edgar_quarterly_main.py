from modules.edgar_quarterly import run
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()