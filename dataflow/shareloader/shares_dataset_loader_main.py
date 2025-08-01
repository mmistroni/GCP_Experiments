from shareloader.modules import share_datset_loader
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  share_datset_loader.run()
