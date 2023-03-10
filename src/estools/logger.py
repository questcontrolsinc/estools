import logging
from logging.handlers import RotatingFileHandler
from os import makedirs as os_makedirs

LOG_DIR = './logs'
LOG_FILE = f'{LOG_DIR}/estools.log'
LOG_MAX_BYTES = 10_485_760  # 10M


class LoggerFormatter(logging.Formatter):
    """
    Custom logger formatter for use of
    extra={'ctx': None} keyerror when no context is set
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format record
        """
        if not hasattr(record, 'ctx'):
            # if extra={'ctx': ...} does not exist on record, add it
            record.ctx = None

        return super().format(record)


class RotatingFileHandlerDir(RotatingFileHandler):
    def __init__(
        self,
        filename,
        mode: str = "a",
        maxBytes: int = 0,
        backupCount: int = 0,
        encoding: str | None = None,
    ) -> None:
        # log mkdir
        os_makedirs(LOG_DIR, exist_ok=True)
        super().__init__(filename, mode, maxBytes, backupCount, encoding)


def logger_ctx(ctx: dict) -> dict:
    """
    Context to logger ctx
    """
    return {'ctx': ctx}


logger = logging.getLogger(__name__)

file_handler = RotatingFileHandlerDir(
    filename=LOG_FILE, mode='w', maxBytes=LOG_MAX_BYTES, backupCount=1
)

# always overwrite if exists
file_handler.doRollover()

formatter = LoggerFormatter(
    '%(asctime)s [%(levelname)s] %(message)s (%(ctx)s) (%(threadName)s)',
    datefmt='%Y-%m-%d %H:%M:%S',
)
file_handler.setFormatter(formatter)
logger.setLevel(logging.INFO)

logger.addHandler(file_handler)
