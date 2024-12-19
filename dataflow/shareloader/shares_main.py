from shareloader.modules import shareloader
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  shareloader.run()
