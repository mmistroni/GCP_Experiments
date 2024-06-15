from mypackage import dftester
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  dftester.run()
