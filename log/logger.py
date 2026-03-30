import os
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

class ThreadmarkLogger:
    def __init__(self, name="ThreadmarkProject"):
        self.logger = logging.getLogger(name)
        
        # avoid duplicated registration handlers when multiple instances are created
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

            # 1. Get current date for folder name (YYYYMMDD)
            current_date = datetime.now().strftime("%Y%m%d")
            log_dir = os.path.join("output","logs", current_date) # ex. output/logs/20260327 

            # 2. Create directory if it doesn't exist
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # 3. Define log file path
            log_filepath = os.path.join(log_dir, "app.log")

            # 4. file hander (rotate per 5MB with 3 backup files)
            file_handler = RotatingFileHandler(log_filepath, maxBytes=5*1024*1024, backupCount=3)
            file_handler.setFormatter(formatter)

            # 2. colsole handler
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(stream_handler)

    def get_logger(self):
        return self.logger

# Create a common logger instance that can be imported and used across the project
common_logger = ThreadmarkLogger().logger