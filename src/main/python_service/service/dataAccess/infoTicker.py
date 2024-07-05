import logging

import redis
from src.main.python_service.config import config


class InfoTicker:

    redis_client = redis.Redis(
        host=config.REDIS_HOST_NAME,
        port=config.REDIS_PORT,
        decode_responses=True
    )

    def store_ticker_info(self, ticker: str, data: dict):
        try:
            self.redis_client.hset(self._info_key(ticker), mapping=data)
        except redis.ResponseError as error:
            logging.debug(f'{error} ticker: {ticker}')

    @staticmethod
    def _info_key(ticker: str) -> str:
        return f'{ticker}:info'
