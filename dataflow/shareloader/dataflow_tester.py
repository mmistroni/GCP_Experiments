from mypackage import launcher
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  launcher.run()
