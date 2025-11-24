import logging
import colorlog

class CustomColoredFormatter(colorlog.ColoredFormatter):
    def format(self, record):
        # Create a padded field with the level name and padding outside the brackets
        record.levelname_bracket = f"[{record.levelname}]"
        # Calculate padding needed (8 is your desired width)
        pad = 8 - len(record.levelname)
        record.levelname_pad = " " * pad if pad > 0 else ""
        return super().format(record)

def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = colorlog.StreamHandler()
        formatter = CustomColoredFormatter(
            "[%(asctime)s] %(log_color)s [%(name)s] %(levelname_bracket)s%(levelname_pad)s %(message)s%(reset)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'purple',
                'CRITICAL': 'red',
            },
            style='%'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger