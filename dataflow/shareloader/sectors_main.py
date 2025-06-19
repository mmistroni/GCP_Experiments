from shareloader.modules import sector_loader
import logging
import sys
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  sector_loader.run()
