import logging
from datetime import datetime
import os
from pathlib import Path
from typing import Optional

class LoggerSetup:
    """A utility class for standardized logging setup across the application."""
    
    def __init__(self, 
                 logger_name: str,
                 log_dir: str = "logs",
                 log_level: int = logging.INFO,
                 log_to_console: bool = True,
                 log_to_file: bool = True,
                 extra_logger: Optional[str] = None):
        """Initialize logger setup with configurable options.
        
        Args:
            logger_name: Name of the logger (typically the class/module name)
            log_dir: Directory where log files will be stored
            log_level: Logging level (e.g., logging.INFO, logging.DEBUG)
            log_to_console: Whether to output logs to console
            log_to_file: Whether to output logs to file
            extra_logger: Name of an additional logger for special cases
                         (e.g., for changes tracking, detailed reports)
        """
        self.logger_name = logger_name
        self.log_dir = log_dir
        self.log_level = log_level
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup main logger
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)
        
        # Clear any existing handlers
        self.logger.handlers = []
        
        # Add handlers based on configuration
        if log_to_file:
            self._add_file_handler()
        if log_to_console:
            self._add_console_handler()
            
        # Setup extra logger if specified
        self.extra_logger = None
        if extra_logger:
            self.extra_logger = self._setup_extra_logger(extra_logger)
            
        self.logger.info(f"Logger initialized: {logger_name}")
        
    def _add_file_handler(self):
        """Add a file handler to the logger."""
        # Create a unique log filename
        log_filename = f'{self.logger_name.lower()}_{self.timestamp}.log'
        log_path = Path(self.log_dir) / log_filename
        
        # Create and configure file handler
        file_handler = logging.FileHandler(log_path)
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        self.logger.addHandler(file_handler)
        
        # Store log file path
        self.log_file = str(log_path)
        self.logger.info(f"Logging to file: {self.log_file}")
        
    def _add_console_handler(self):
        """Add a console handler to the logger."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(
            logging.Formatter('%(levelname)s: %(message)s')
        )
        self.logger.addHandler(console_handler)
        
    def _setup_extra_logger(self, extra_name: str) -> logging.Logger:
        """Setup an additional logger for special purposes.
        
        Args:
            extra_name: Name of the extra logger
            
        Returns:
            logging.Logger: Configured extra logger
        """
        # Create extra log filename
        extra_filename = f'{extra_name.lower()}_{self.timestamp}.log'
        extra_path = Path(self.log_dir) / extra_filename
        
        # Setup extra logger
        extra_logger = logging.getLogger(f"{self.logger_name}_{extra_name}")
        extra_logger.setLevel(self.log_level)
        
        # Clear any existing handlers
        extra_logger.handlers = []
        
        # Add file handler for extra logger
        extra_handler = logging.FileHandler(extra_path)
        extra_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
        extra_logger.addHandler(extra_handler)
        
        self.logger.info(f"Extra logger initialized: {extra_name}")
        return extra_logger
    
    def get_logger(self) -> logging.Logger:
        """Get the main logger instance.
        
        Returns:
            logging.Logger: The configured logger instance
        """
        return self.logger
    
    def get_extra_logger(self) -> Optional[logging.Logger]:
        """Get the extra logger instance if it exists.
        
        Returns:
            Optional[logging.Logger]: The extra logger instance or None
        """
        return self.extra_logger

# Example usage
def main():
    # Basic usage
    logger_setup = LoggerSetup("TestLogger")
    logger = logger_setup.get_logger()
    logger.info("This is a test message")
    
    # With extra logger for detailed changes
    detailed_logger = LoggerSetup(
        "DetailedLogger",
        extra_logger="changes"
    )
    main_log = detailed_logger.get_logger()
    changes_log = detailed_logger.get_extra_logger()
    
    main_log.info("Main process started")
    changes_log.info("Detailed change log entry")

if __name__ == "__main__":
    main()