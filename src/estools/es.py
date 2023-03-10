from time import perf_counter
from typing import Any, Mapping

from elasticsearch import Elasticsearch, exceptions as es_exceptions, helpers as es_helpers

from .errors import EsError
from .helpers import timer_to_str
from .logger import logger, logger_ctx
from .types import JsonList

REQUEST_TIMEOUT = 4


class Es:
    """
    Elasticsearch client wrapper
    (docs: https://elasticsearch-py.readthedocs.io/en/v8.6.2/api.html)
    """

    def __init__(self, hosts: list[str], index: str) -> None:
        """
        Init
        """
        self._index = index
        http_compress = True
        request_timeout = REQUEST_TIMEOUT
        retry_on_timeout = True
        max_retries = 1
        sniff_on_start = True

        ctx = logger_ctx(
            {
                'hosts': hosts,
                'index': index,
                'http_compress': http_compress,
                'request_timeout': request_timeout,
                'retry_on_timeout': retry_on_timeout,
                'max_retries': max_retries,
                'sniff_on_start': sniff_on_start,
            }
        )

        logger.debug('Es init', extra=ctx)

        try:
            self._es = Elasticsearch(
                hosts=hosts,  # pyright: ignore
                http_compress=http_compress,
                request_timeout=request_timeout,
                retry_on_timeout=retry_on_timeout,
                max_retries=max_retries,
                sniff_on_start=sniff_on_start,
            )

        # connection/sniff failed
        except es_exceptions.TransportError as e:
            raise EsError(f'Failed to connect to Elasticsearch: {e}') from e

        # other exceptions
        except Exception as e:
            raise EsError(f'Elasticsearch operation failed: {e}') from e

        logger.debug('Elasticsearch connection ready', extra=ctx)

    def bulk_index(self, actions: JsonList) -> int:
        """
        Bulk index docs

        @return: int Number of indexed docs
        """
        ctx = logger_ctx({'index': self._index, 'num_of_actions': len(actions)})
        logger.debug('Sending bulk index actions to Elasticsearch', extra=ctx)
        start = perf_counter()

        try:
            aff, _ = es_helpers.bulk(self._es, actions=actions, index=self._index)

        # bulk index failed
        except es_helpers.errors.BulkIndexError as e:
            msg = f'Elasticsearch bulk index failed: {e}'
            logger.error(msg, extra=ctx)
            raise EsError(msg) from e

        msg = f'{aff:,} bulk index actions to Elasticsearch successful {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)

        return aff

    def count(self) -> int:
        """
        Index document count getter
        """
        ctx = logger_ctx({'index': self._index})
        logger.debug('Fetching index count from Elasticsearch', extra=ctx)

        res = self._es.count()
        count = res.get('count', 0)
        ctx['ctx']['count'] = count

        msg = f'{count:,} total documents'
        logger.debug(msg, extra=ctx)

        return count

    def search_scroll_slice(
        self,
        slice_id: int,
        slices: int,
        query: dict,
        size: int,
        sort: list[str | Mapping[str, Any]],
        keep_alive: str = '1m',
    ) -> tuple[int, str, list]:
        """
        Search scroll with slice

        @return: `slice_id, scroll_id, docs`
        """
        scroll_slice = {'id': slice_id, 'max': slices}
        ctx = logger_ctx(
            {
                'index': self._index,
                'slice': scroll_slice,
                'query': query,
                'size': size,
                'sort': sort,
                'keep_alive': keep_alive,
            }
        )
        logger.debug('Sending search scroll request to Elasticsearch', extra=ctx)
        start = perf_counter()

        # pylint: disable=unexpected-keyword-arg
        # (using kwargs)
        res = self._es.search(
            query=query, size=size, scroll=keep_alive, sort=sort, slice=scroll_slice
        )
        # pylint: enable=unexpected-keyword-arg

        # set scroll ID
        scroll_id = res.get('_scroll_id')
        if not scroll_id:
            raise EsError(f'Invalid scroll ID (slice_id {slice_id})')

        ctx['ctx']['scroll_id'] = scroll_id

        docs = res.get('hits', {}).get('hits', [])

        msg = f'{len(docs):,} documents in search scroll response {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)

        return slice_id, scroll_id, docs

    def search_scroll_slice_next(
        self,
        slice_id: int,
        scroll_id: str,
        keep_alive: str = '1m',
    ) -> tuple[int, list]:
        """
        Search scroll with scroll ID and slice

        @return: `slice_id, docs`
        """
        ctx = logger_ctx(
            {
                'index': self._index,
                'slice_id': slice_id,
                'scroll_id': scroll_id,
                'keep_alive': keep_alive,
            }
        )
        logger.debug('Sending scroll request to Elasticsearch', extra=ctx)
        start = perf_counter()

        # pylint: disable=unexpected-keyword-arg
        # (using kwargs)
        res = self._es.scroll(scroll_id=scroll_id, scroll=keep_alive)
        # pylint: enable=unexpected-keyword-arg

        docs = res.get('hits', {}).get('hits', [])

        msg = f'{len(docs):,} documents in scroll response {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)

        return slice_id, docs

    def shards(self) -> int:
        """
        Total number of shards getter
        """
        ctx = logger_ctx({'index': self._index})
        logger.debug('Fetching total shards from Elasticsearch', extra=ctx)
        start = perf_counter()

        stats = self._es.cluster.stats()
        shards = stats.get('indices', {}).get('shards', {}).get('total', 0)
        ctx['ctx']['shards'] = shards

        if not shards or not isinstance(shards, int):
            raise EsError(f'Invalid number of shards ({shards})')

        msg = f'{shards} total shards found {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)

        return shards
