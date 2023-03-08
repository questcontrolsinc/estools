from contextlib import suppress as contextlib_suppress
import json
from pathlib import Path
import sys
import time
import typing as t
from urllib.parse import urlparse

from .types import JsonObject


def file_count_lines(file_path: str) -> int:
    """
    Count lines in file
    """
    file_must_exist(file_path)

    count = 0

    with open(file_path, 'r', encoding='utf8') as f:
        count = sum(1 for line in f)

    return count


def file_must_exist(file_path: str) -> None:
    """
    Trigger error if file does not exist
    """
    if not Path(file_path).is_file():
        raise FileNotFoundError(f'File does not exists: "{file_path}"')


def file_must_not_exist(file_path: str) -> None:
    """
    Trigger error if file already exists
    """
    if Path(file_path).is_file():
        raise FileExistsError(f'File already exists: "{file_path}"')


def file_to_lines(file_path: str) -> t.Iterator[str]:
    """
    File to lines generator
    """
    file_must_exist(file_path)

    with open(file_path, 'r', encoding='utf8') as f:
        for line in f:
            yield line.strip()


def hosts_str_to_list(hosts: str) -> list[str]:
    """
    Hosts string to list of hosts
    """
    h = hosts.split(',')

    for k, v in enumerate(h):
        v = v.strip()

        # rm empty
        if not v:
            del h[k]
            continue

        # add default scheme
        if not v.startswith('http'):
            v = 'http://' + v
            h[k] = v

        url = urlparse(v)

        # add default port
        if not url.port:
            h[k] += ':9200'

    if len(h) < 1:
        raise ValueError('Invalid number of hosts')

    return h


def prog_bar(completed_percent: float, maxwidth: float = 0.5, lock: object | None = None) -> None:
    """
    Progress bar
    """
    completed = min(completed_percent * 100, 100)
    width = round(100 * maxwidth)
    done = round(completed * maxwidth)

    ctx_lock = lock if lock else contextlib_suppress()
    with ctx_lock:
        sys.stdout.write('\r')
        sys.stdout.write(f'[{"=" * done}{"." * (width - done)}] {completed:.2f}% ')
        sys.stdout.flush()


def sort_str_to_list(sort: str) -> list[dict[str, str]]:
    """
    Sort string to dict, like `"f1,f2:desc"` => `[{"f1": "asc"}, {"f2": "desc"}]`
    """
    sl = []

    if not sort:
        return sl

    for s in sort.split(','):
        s = s.split(':')

        if len(s) == 2:
            sl.append({s[0].strip(): s[1].strip()})
        else:
            sl.append({s[0].strip(): 'asc'})

    return sl


def template_to_json(template: str) -> JsonObject:
    """
    Template to JSON object
    """
    if not template:
        raise ValueError('Invalid template name (empty)')

    path = Path(template)

    if not path.is_file():
        raise FileNotFoundError(f'Template file "{path.absolute()}" does not exist')

    try:
        with open(path.absolute(), 'r', encoding='utf8') as f:
            template_obj = json.load(f)
    except json.decoder.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON template: {e}') from e

    return template_obj


def timer_to_str(start_time: float) -> str:
    """
    Timer using start time to string
    """

    def seconds_to_str(seconds: int) -> str:
        """
        Seconds to string
        """
        h, r = divmod(seconds, 3600)
        m, s = divmod(r, 60)

        if h:
            return f'{int(h)}h {int(m)}m {s:.2f}s'
        elif m:
            return f'{int(m)}m {s:.2f}s'
        else:
            return f'{s:.2f}s'

    return f'[Done in {seconds_to_str(time.perf_counter() - start_time)}]'
