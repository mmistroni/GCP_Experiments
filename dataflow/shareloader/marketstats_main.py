from shareloader.modules import marketstats
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  marketstats.run()