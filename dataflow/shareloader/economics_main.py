from shareloader.modules import economy_monitor
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  economy_monitor.run()
