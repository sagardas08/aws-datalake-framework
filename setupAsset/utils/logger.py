from datetime import datetime, timedelta
from io import StringIO
import functools
from pathlib import Path
from logging import (
    getLogger,
    Formatter,
    StreamHandler,
    FileHandler,
    getLevelName,
)

import boto3


CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0


class Logger:
    def __init__(
        self,
        level=WARNING,
        log_type="C",
        log_name="delete-logger",
        file_name=None,
    ):
        """
        Logger class to instantiate a logging object
        :param level: The level of logging - info, debug, warn, critical
        :param log_type: The type of logging required - C for console logs,
        F for file logs, S/S3 for logs to be stored on S3
        """
        # log format -> 01/Feb/2022 16:06:56 - loggerName - LogLevel :
        self.formatter = Formatter(
            "%(asctime)s - %(name)s - %(levelname)s : %(message)s",
            datefmt="%d/%b/%Y %H:%M:%S",
        )
        self.string_object = StringIO()
        self.log_type = log_type
        self.level = level
        self.log_name = log_name if log_name else "root-logger"
        self.file_name = file_name if file_name else "deploy_logs.log"
        self.logger = self._get_logger()

    def _get_logger(self):
        """
        Defines the different handlers in case of different log types
        :return: logging object
        """
        logger = getLogger(self.log_name)
        if self.log_type == "C":
            console_handler = StreamHandler()
            console_handler.setLevel(self.level)
            console_handler.setFormatter(self.formatter)
            logger.addHandler(console_handler)
        elif self.log_type == "F":
            try:
                file_handler = FileHandler(self.file_name)
            except FileNotFoundError or IsADirectoryError:
                path = Path(self.file_name)
                path.parent.mkdir(parents=True, exist_ok=True)
            finally:
                file_handler = FileHandler(self.file_name)
                file_handler.setLevel(self.level)
                file_handler.setFormatter(self.formatter)
                logger.addHandler(file_handler)
        elif self.log_type == "S" or self.log_type == "S3":
            log_handler = StreamHandler(self.string_object)
            log_handler.setLevel(self.level)
            log_handler.setFormatter(self.formatter)
            logger.addHandler(log_handler)
        return logger

    def write(self, level=None, message=""):
        """
        Self logging method
        :param level: int level of the log level
        :param message: The log message to be stored
        :return:
        """
        if level is not None:
            return self.logger.log(level, message)
        else:
            return self.logger.log(self.level, message)

    def write_logs_to_s3(self, region, log_bucket):
        """
        Class method to write the logs to S3 using a StringIO object
        :return: None
        """
        if self.log_type == "S3" or self.log_type == "S":
            s3 = boto3.resource("s3", region_name=region)
            content = self.string_object.getvalue()
            try:
                s3.Object(log_bucket, self.file_name).put(Body=content)
            except Exception as e:
                raise e


def log(function_to_decorate=None, *, param_logger=None):
    """
    A decorator class to be used for logging functions
    :param function_to_decorate: The function above which @log will be used to decorate
    :param param_logger: A logger object may or may not be passed as one of the params for logging
    :return:
    """

    def decorator_log(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if param_logger is None:
                logger_params = [
                    x for x in kwargs.values() if isinstance(x, Logger)
                ] + [x for x in args if isinstance(x, Logger)]
                logger = next(
                    iter(logger_params),
                    Logger(log_name="root-logger", log_type="C"),
                )
            else:
                logger = param_logger
            func_name = func.__name__
            args_repr = [
                repr(arg) for arg in args if not isinstance(arg, Logger)
            ]
            kwargs_repr = [
                f"{k}={v}"
                for k, v in kwargs.items()
                if not isinstance(v, Logger)
            ]
            signature = ", ".join(args_repr + kwargs_repr)
            try:
                if len(signature) > 0:
                    logger.write(
                        level=WARNING,
                        message=f"Executing function {func_name} with arguments: {signature}",
                    )
                else:
                    logger.write(
                        level=WARNING,
                        message=f"Executing function {func_name} with no key arguments",
                    )
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                logger.write(
                    level=CRITICAL,
                    message=f"Error encountered in function {func_name}: {str(e)}",
                )
                if logger.log_type in ["S", "S3"]:
                    logger.write_logs_to_s3()
                raise e

        return wrapper

    if function_to_decorate is None:
        return decorator_log
    else:
        return decorator_log(function_to_decorate)
