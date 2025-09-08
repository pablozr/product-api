import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s"
)

logger = logging.getLogger(__name__)



if __name__ == "__main__":
    logger.info("Teste")
