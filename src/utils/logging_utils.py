# Databricks notebook source
# Class to manage loggings. Has 5 methods: log_info, log_warn, log_error, log_check_pass, log_check_not_pass

import logging
import pytz
import sys
from datetime import datetime


# Colors definitions
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
ORANGE = "\033[38;5;208m"
RESET = "\033[0m"

def log_setup_logic():
  # Set timezone
  tz = pytz.timezone('Europe/Lisbon')

  # Custom converter using the timezone
  def custom_time(*args):
    return datetime.now(tz).timetuple()

  logging.getLogger("py4j").setLevel(logging.WARN)
  # Reconfigure root logger to write to stdout
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  # Clear existing handlers (important in notebooks)
  if logger.hasHandlers():
    logger.handlers.clear()

  # Force logs to show in the notebook
  stream_handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

  # Override the time converter
  formatter.converter = custom_time

  stream_handler.setFormatter(formatter)

  logger.addHandler(stream_handler)

  logging.info("Logs initialized")
  logging.info("This log is timestamped in Europe/Lisbon timezone")


# Write an information message to logs
def log_info(message):
  logging.info(message)

# Write a warning message to logs
def log_warn(message):
  logging.warning(f"{ORANGE}{message}{RESET}")

# Write an error message to logs
def log_error(message):
  logging.error(f"{RED}{message}{RESET}")
  sys.exit(1) 

def log_check_pass(message):
  logging.info(f"{GREEN}{message}{RESET}")

def log_check_not_pass(message):
  logging.warning(f"{ORANGE}{message}{RESET}")