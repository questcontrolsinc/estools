from concurrent.futures import Future, ThreadPoolExecutor, as_completed as futures_as_completed
from datetime import datetime, timedelta
import json
from os import linesep as PY_EOL
import random
from threading import Lock, Semaphore
import time
from typing import Callable, Iterator

import click

from .errors import TemplateError
from .es import Es
from .helpers import (
    file_count_lines,
    file_must_not_exist,
    file_to_lines,
    hosts_str_to_list,
    prog_bar,
    sort_str_to_list,
    template_to_json,
    timer_to_str,
)
from .logger import logger, logger_ctx
from .types import Json

MAX_WORKERS = 20
MAX_QUEUE_SIZE = 100
LOCK_MAIN = Lock()


class EsTools:
    """
    ES tools
    """

    # imported docs
    _imp_aff = 0

    def __init__(
        self,
        file_path: str | None,
        inp: str | None,
        index: str | None,
        out: str | None,
        size: int,
        sort: str,
        template: str | None,
    ) -> None:
        """
        Init
        """
        if index:
            # export or import using file
            if file_path:
                # 1 export to file
                if inp:
                    file_must_not_exist(file_path)

                    with open(file_path, mode='a', encoding='utf8') as f:

                        def write_to_file(line: dict) -> None:
                            f.write(json.dumps(line) + PY_EOL)

                        self.export(cb=write_to_file, hosts=inp, index=index, size=size, sort=sort)

                # 2 import from file
                if out:
                    self.import_to_index(
                        cb_iterator=file_to_lines,
                        cb_arg=file_path,
                        hosts=out,
                        index=index,
                        size=size,
                        total_docs=file_count_lines(file_path),
                    )

            # 3 ES export to ES import
            # elif inp and out:
            # finish

            # 4 import using template
            elif template and out is not None:
                self.import_to_index(
                    cb_iterator=self.template_to_docs,
                    cb_arg=template,
                    hosts=out,
                    index=index,
                    size=size,
                    total_docs=template_to_json(template).get('$total', 0),
                )

            # 5 export to stdout
            elif inp is not None:

                def stdout(doc: Json) -> None:
                    print(json.dumps(doc))

                self.export(
                    cb=stdout, hosts=inp, index=index, size=size, sort=sort, suppress_output=True
                )

        # template
        elif template:
            # 6 template export to file
            if file_path:
                self.export_template_to_file(file_path=file_path, template=template)

            # 7 template export to stdout
            else:
                self.export_template(template=template)

        else:
            # no options, display help
            print(click.get_current_context().get_help())

    def export(
        self,
        cb: Callable,
        hosts: str,
        index: str,
        size: int,
        sort: str,
        suppress_output: bool = False,
    ) -> None:
        """
        Export from ES to callback

        @return: `total_expected_docs, total_actual_docs`
        """
        sort_list = sort_str_to_list(sort)
        ctx = logger_ctx({'hosts': hosts, 'index': index, 'size': size, 'sort': sort_list})
        logger.debug('Export index to callback', extra=ctx)

        start = time.perf_counter()
        es = Es(hosts=hosts_str_to_list(hosts), index=index)
        total_expected_docs, total_actual_docs = 0, 0

        # fetch total docs for tracking progress (approx is ok)
        total_docs = es.count()

        if total_docs and not suppress_output:
            prog_bar(0)  # init

        # fetch total shards
        shards = es.shards()
        ctx['ctx']['shards'] = shards

        # limit max workers
        max_workers = min(shards, MAX_WORKERS)

        def document(doc: Json) -> Json:
            """
            Document for output
            """
            return {'_id': doc['_id'], **doc['_source']}

        # make all initial search scroll requests
        futures = []
        with ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix='ThreadPool'
        ) as executor:
            for i in range(max_workers):
                futures.append(
                    executor.submit(
                        es.search_scroll_slice,
                        slice_id=i,
                        slices=max_workers,
                        query={'match_all': {}},
                        size=size,
                        sort=sort_list,
                    )
                )

        # track scrolling slices
        scrolling = {}
        for future in futures_as_completed(futures):
            try:
                slice_id, scroll_id, docs = future.result()

                # continue scrolling if docs exist for search scroll
                if docs:
                    total_expected_docs += len(docs)
                    scrolling[slice_id] = scroll_id

                    for doc in docs:
                        total_actual_docs += 1
                        cb(document(doc))

                        if total_docs and not suppress_output:
                            prog_bar(total_actual_docs / total_docs, lock=LOCK_MAIN)

            except Exception as e:
                raise RuntimeError(f'Future result failed for search scroll: {e}') from e

        # continue with scrolling slices (scroll requests)
        while scrolling:
            futures = []
            with ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix='ThreadPool'
            ) as executor:
                for slice_id, scroll_id in scrolling.items():
                    futures.append(
                        executor.submit(
                            es.search_scroll_slice_next, slice_id=slice_id, scroll_id=scroll_id
                        )
                    )

            for future in futures_as_completed(futures):
                try:
                    slice_id, docs = future.result()

                    # no longer scrolling for slice
                    if not docs:
                        del scrolling[slice_id]
                        continue

                    total_expected_docs += len(docs)

                    for doc in docs:
                        total_actual_docs += 1
                        cb(document(doc))

                        if total_docs and not suppress_output:
                            prog_bar(total_actual_docs / total_docs, lock=LOCK_MAIN)

                except Exception as e:
                    raise RuntimeError(f'Future result failed for search scroll (next): {e}') from e

        if total_docs and not suppress_output:
            prog_bar(100)  # always display completed
            print()

        # the total should be equal
        if total_expected_docs != total_actual_docs:
            raise RuntimeError(
                f'{total_actual_docs:,} docs out of {total_expected_docs:,}'
                ' expected docs exported'
            )

        msg = f'{total_actual_docs:,} documents exported {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)

        if not suppress_output:
            print(msg)

    def export_template(self, template: str) -> None:
        """
        Export from template to stdout
        """
        logger.debug('Template to stdout', extra=logger_ctx({'template': template}))
        start = time.perf_counter()
        c = 0

        for doc in self.template_to_docs(template):
            print(json.dumps(doc))
            c += 1

        msg = f'{c:,} documents generated {timer_to_str(start)}'
        logger.debug(msg)

    def export_template_to_file(self, file_path: str, template: str) -> None:
        """
        Export from template to file
        """
        ctx = logger_ctx({'file': file_path, 'template': template})
        logger.debug('Template to file', extra=ctx)
        file_must_not_exist(file_path)

        start = time.perf_counter()
        c = 0
        with open(file_path, 'a', encoding='utf8') as f:
            for doc in self.template_to_docs(template, display_prog_bar=True):
                f.write(json.dumps(doc) + PY_EOL)
                c += 1

        print()
        msg = f'{c:,} documents written to "{file_path}" {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)
        print(msg)

    def import_to_index(
        self,
        cb_iterator: Callable,
        cb_arg: str,
        hosts: str,
        index: str,
        size: int,
        total_docs: int = 0,
    ) -> None:
        """
        Import from callback iterator to ES
        """
        ctx = logger_ctx({'hosts': hosts, 'index': index, 'cb_arg': cb_arg})
        logger.debug('Import from callback iterator to index', extra=ctx)

        start = time.perf_counter()
        es = Es(hosts=hosts_str_to_list(hosts), index=index)
        EsTools._imp_aff = 0  # reset

        if total_docs:
            # init
            prog_bar(0)

        batch = []

        def future_done(aff: int) -> None:
            EsTools._imp_aff += aff

            # update progress bar
            if total_docs:
                prog_bar(EsTools._imp_aff / total_docs, lock=LOCK_MAIN)

        blocking_executor = BlockingExecutor(max_tasks=MAX_QUEUE_SIZE, task_done_cb=future_done)

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS, thread_name_prefix='ThreadPool'
        ) as executor:
            # loop each line from generator
            for line in cb_iterator(cb_arg):
                # add line to batch
                if line:
                    batch.append(line)

                # batch full
                if len(batch) >= size:
                    # send batch
                    blocking_executor.task_submit(executor, es.bulk_index, batch)

                    # reset batch
                    batch = []

            # send final batch
            if len(batch):
                blocking_executor.task_submit(executor, es.bulk_index, batch)

            logger.debug(
                'Waiting on blocking tasks', extra=logger_ctx({'maxQueueSize': MAX_QUEUE_SIZE})
            )

        if total_docs:
            prog_bar(100)  # always show completed
            print()

        msg = f'{EsTools._imp_aff:,} documents bulk indexed {timer_to_str(start)}'
        logger.debug(msg, extra=ctx)
        print(msg)

    def template_to_docs(self, template_str: str, display_prog_bar: bool = False) -> Iterator[Json]:
        """
        Template to docs generator
        """
        template = template_to_json(template_str)

        # total number of docs to generate
        if '$total' in template:
            total = template['$total']
            del template['$total']
        else:
            # default
            total = 1

        try:
            total = int(total)
        except (TypeError, ValueError) as e:
            total = 0

        if total < 1:
            raise ValueError(
                ' Invalid template value for "$total", value must be a number greater than 0'
            )

        # display progress bar
        if display_prog_bar:
            prog_bar(0)

        counters, file_lists, indexes, values = {}, {}, {}, {}
        dt_now = datetime.now()

        for i in range(total):
            # shallow copy
            doc = dict(template)

            for field, value in list(doc.items()):
                ###########################
                # $concat
                if field == '$concat':
                    if not isinstance(value, dict):
                        raise TemplateError(
                            'Expected an object (dict) for $concat value', value['$concat']
                        )

                    for subfield, subval in value.items():
                        if not subfield in doc:
                            raise TemplateError(
                                f'Expected field "{subfield}" in document template for $concat', doc
                            )

                        if subval == '$id':
                            doc[subfield] = (
                                f'{doc[subfield]}{i + 1}'
                                if doc[subfield] is not None
                                else str(i + 1)
                            )
                        elif subval == '$index':
                            doc[subfield] = (
                                f'{doc[subfield]}{i}' if doc[subfield] is not None else str(i)
                            )
                        else:
                            raise TemplateError(f'Unknown $concat type "{subval}"', doc)

                    del doc[field]

                ###########################
                # $loop
                if field == '$loop':
                    if not isinstance(value, dict):
                        raise TemplateError(
                            'Expected an object (dict) for $loop value', value['$loop']
                        )

                    for subfield, subval in value.items():
                        # load list from file
                        if isinstance(subval, str):
                            if subfield in file_lists:
                                subval = file_lists[subfield]
                            else:
                                file_list = []
                                for line in file_to_lines(subval):
                                    if line:
                                        file_list.append(line)
                                file_lists[subfield] = file_list
                                subval = file_list

                        if not isinstance(subval, list):
                            raise TemplateError(
                                f'Expected an array (list) for $loop value for "{subfield}"', subval
                            )

                        if not len(subval):
                            raise TemplateError(
                                f'Array (list) for $loop for "{subfield}" must have'
                                ' at least one item',
                                subval,
                            )

                        # track current position
                        if subfield not in indexes:
                            indexes[subfield] = 0

                        # reset position
                        if indexes[subfield] >= len(subval):
                            indexes[subfield] = 0

                        doc[subfield] = subval[indexes[subfield]]

                        # increment
                        indexes[subfield] += 1

                    del doc[field]

                ###########################
                # $rand
                if field == '$rand':
                    if not isinstance(value, dict):
                        raise TemplateError(
                            'Expected an object (dict) for $rand value', value['$rand']
                        )

                    for subfield, subval in value.items():
                        # low, high values for random int/float
                        if isinstance(subval, dict):
                            low = subval.get('low')
                            high = subval.get('high')

                            if isinstance(low, int) and isinstance(high, int):
                                doc[subfield] = random.randint(low, high)
                            elif isinstance(low, float) and isinstance(high, float):
                                doc[subfield] = round(random.uniform(low, high), 2)
                            else:
                                raise TemplateError(
                                    f'Invalid values for "low" and "high" for'
                                    f' $rand for "{subfield}"',
                                    subval,
                                )
                        else:
                            # load list from file
                            if isinstance(subval, str):
                                if subfield in file_lists:
                                    subval = file_lists[subfield]
                                else:
                                    file_list = []
                                    for line in file_to_lines(subval):
                                        if line:
                                            file_list.append(line)
                                    file_lists[subfield] = file_list
                                    subval = file_list

                            if not isinstance(subval, list):
                                raise TemplateError(
                                    f'Expected an array (list) for $rand value for "{subfield}"',
                                    subval,
                                )

                            doc[subfield] = random.choice(subval)

                    del doc[field]

                ###########################
                # $timestamp
                if field == '$timestamp':
                    if not isinstance(value, dict):
                        raise TemplateError(
                            'Expected an object (dict) for $timestamp value', value['$timestamp']
                        )

                    for subfield, subval in value.items():
                        # {every: int, reduce: str}
                        if isinstance(subval, dict):
                            every = subval.get('every')
                            reduce = subval.get('reduce')

                            if not isinstance(every, int):
                                raise TemplateError(
                                    'Expected integer value for "every" for '
                                    f' $timestamp "{subfield}"',
                                    subval,
                                )

                            if not isinstance(reduce, str) or not reduce:
                                raise TemplateError(
                                    'Expected non-empty string value for "reduce" for '
                                    f' $timestamp "{subfield}"',
                                    subval,
                                )

                            try:
                                ts_reduce = int(reduce[0:-1])
                            except ValueError as e:
                                raise TemplateError(
                                    'Invalid value for "reduce" for $timestamp' f' "{subfield}"',
                                    subval,
                                ) from e

                            ts_reduce_unit = reduce[-1]

                            if subfield not in values:
                                values[subfield] = ts_reduce

                            if subfield not in counters:
                                counters[subfield] = 0

                            if counters[subfield] >= every:
                                values[subfield] += ts_reduce
                                counters[subfield] = 0

                            counters[subfield] += 1
                            subval = str(values[subfield]) + ts_reduce_unit

                        if isinstance(subval, str):
                            subval = [subval]

                        if not isinstance(subval, list):
                            raise TemplateError(
                                f'Expected an array (list) for $timestamp value for "{subfield}"',
                                subval,
                            )

                        # track current position
                        if subfield not in indexes:
                            indexes[subfield] = 0

                        # reset position
                        if indexes[subfield] >= len(subval):
                            indexes[subfield] = 0

                        ts_delta = subval[indexes[subfield]]

                        # now
                        if ts_delta == 'now':
                            doc[subfield] = int(dt_now.timestamp())
                        # diff
                        else:
                            ts_delta_unit = ts_delta[-1].lower()

                            if ts_delta_unit not in ['s', 'm', 'h', 'd']:
                                raise TemplateError(
                                    'Expected "s", "m", "h" or "d" unit for $timestamp value'
                                    f' for "{subfield}"',
                                    subval,
                                )

                            ts_delta_val = ts_delta[0:-1]

                            if not ts_delta_val.isdigit():
                                raise TemplateError(
                                    f'Expected an integer for $timestamp "{ts_delta_unit}" value'
                                    f' for "{subfield}"',
                                    subval,
                                )

                            seconds = int(ts_delta_val) if ts_delta_unit == 's' else 0
                            minutes = int(ts_delta_val) if ts_delta_unit == 'm' else 0
                            hours = int(ts_delta_val) if ts_delta_unit == 'h' else 0
                            days = int(ts_delta_val) if ts_delta_unit == 'd' else 0

                            doc[subfield] = int(
                                (
                                    dt_now
                                    - timedelta(
                                        days=days, hours=hours, minutes=minutes, seconds=seconds
                                    )
                                ).timestamp()
                            )

                        # increment
                        indexes[subfield] += 1

                    del doc[field]

                ###########################
                # $type
                if field == '$type':
                    if not isinstance(value, dict):
                        raise TemplateError(
                            'Expected an object (dict) for $type value', value['$type']
                        )

                    for subfield, subval in value.items():
                        if not subfield in doc:
                            raise TemplateError(
                                f'Expected field "{subfield}" in document template for $type', doc
                            )

                        if subval not in ['float', 'int', 'str']:
                            raise TemplateError(
                                f'Invalid type "{subval}" for $type for "{subfield}"',
                                value['$type'],
                            )

                        try:
                            if subval == 'float':
                                doc[subfield] = float(doc[subfield])
                            elif subval == 'int':
                                doc[subfield] = int(doc[subfield])
                            elif subval == 'str':
                                doc[subfield] = str(doc[subfield])
                        except ValueError as e:
                            raise TemplateError(
                                f'Failed to convert "{subfield}" value to "{subval}" for $type: {e}'
                            ) from e

                    del doc[field]

            # display progress bar
            if display_prog_bar:
                prog_bar((i + 1) / total)

            yield doc


class BlockingExecutor:
    """
    ThreadPoolExecutor wrapper used for blocking based on max number of tasks
    """

    def __init__(self, max_tasks: int, task_done_cb: Callable | None = None) -> None:
        """
        Init
        """
        self._semaphore = Semaphore(max_tasks)
        self._task_done_cb = task_done_cb

    def task_done_cb(self, future: Future) -> None:
        """
        Task done callback handler
        """
        try:
            if self._task_done_cb:
                self._task_done_cb(future.result())

            self._semaphore.release()
        except Exception as e:
            raise RuntimeError(f'Future result failed: {e}') from e

    def task_submit(self, executor: ThreadPoolExecutor, fn: Callable, *args, **kwargs) -> Future:
        """
        Submit task
        """
        try:
            self._semaphore.acquire()
            future = executor.submit(fn, *args, **kwargs)
        except Exception as e:
            self._semaphore.release()
            raise RuntimeError(f'Future submit failed: {e}') from e

        future.add_done_callback(self.task_done_cb)
        return future
