from shareloader.modules import state_of_uk_economy
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  state_of_uk_economy.run()
