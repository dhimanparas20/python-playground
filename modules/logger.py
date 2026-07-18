import logging
import os

import colorlog

SUCCESS_LEVEL = 25
logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")


def _success(self, message, *args, **kwargs):
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


logging.Logger.success = _success


class WorkerIDFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        worker = os.getenv("UVICORN_WORKER")
        if worker:
            record.worker_id = f"-{worker}"
        else:
            record.worker_id = f"-{os.getpid()}"
        return True


class CustomColoredFormatter(colorlog.ColoredFormatter):
    def format(self, record):
        # Create a padded field with the level name and padding outside the brackets
        record.levelname_bracket = f"[{record.levelname}]"
        # Calculate padding needed (8 is your desired width)
        pad = 3 - len(record.levelname)
        record.levelname_pad = " " * pad if pad > 0 else ""
        return super().format(record)


def get_logger(name, show_time: bool = False):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = colorlog.StreamHandler()
        worker_filter = WorkerIDFilter()
        handler.addFilter(worker_filter)
        if show_time:
            formatter = CustomColoredFormatter(
                "[%(asctime)s] %(log_color)s[%(name)s%(worker_id)s]%(levelname_pad)s %(message)s%(reset)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                log_colors={
                    "SUCCESS": "bold_green",
                    "INFO": "bold_cyan",
                    "DEBUG": "bold_white",
                    "WARNING": "bold_yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
                style="%",
            )
        else:
            formatter = CustomColoredFormatter(
                "%(log_color)s[%(name)s%(worker_id)s]%(levelname_pad)s %(message)s%(reset)s",
                log_colors={
                    "SUCCESS": "bold_green",
                    "INFO": "bold_cyan",
                    "DEBUG": "bold_white",
                    "WARNING": "bold_yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
                style="%",
            )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger