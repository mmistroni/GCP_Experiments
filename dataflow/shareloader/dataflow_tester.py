import logging
from shareloader.modules import launcher
import sys

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  launcher.run(sys.argv)
