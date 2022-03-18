import logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
def handler(event, context):
  LOGGER.info(f'Event structure: {event}')