import logging
from shareloader.modules import launcher

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  launcher.run()
