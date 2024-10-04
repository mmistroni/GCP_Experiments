from shareloader.modules import marketstats2
import logging

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  marketstats2.run()
