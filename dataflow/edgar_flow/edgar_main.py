from modules.write_to_edgar import run
import logging
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()