from modules.sector_loader import run
import logging
import sys
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
