import logging

import click

from .estools import EsTools
from .logger import logger, logger_ctx

DEFAULT_SIZE = 10_000


@click.command()
@click.option('-d', '--debug', type=bool, is_flag=True, help='Enable debug mode')
@click.option(
    '-f', '--file', type=click.Path(exists=False), help='Input from or output to file path'
)
@click.option('-i', '--inp', type=str, help='Input from Elasticsearch host(s)')
@click.option('--index', type=str, help='Index name')
@click.option('-o', '--out', type=str, help='Output to Elasticsearch host(s)')
@click.option(
    '--size',
    type=int,
    default=DEFAULT_SIZE,
    help=f'Number of batch objects per Elasticsearch operation (default: {DEFAULT_SIZE:,})',
)
@click.option(
    '--sort', type=str, help='Sort by field(s) for export (like: "field" or "field,field2:desc")'
)
@click.option('-t', '--template', type=str, help='Template file used to generate documents')
@click.option('--version', type=bool, is_flag=True, help='Display version')
def cli(
    debug: bool,
    file: str | None,
    inp: str | None,
    index: str | None,
    out: str | None,
    size: int,
    sort: str,
    template: str | None,
    version: bool,
) -> None:
    """
    EsTools: Exporter, Importer & Indexer for Elasticsearch

    \b
    Exporter examples:
        Export index to file:
            estools --index INDEX -i "127.0.0.1" --file ./index.json
        Export index to stdout (optionally with gzip):
            estools --index INDEX -i "127.0.0.1" | gzip > ./index.json.gz
    Importer examples:
        Import file to index:
            estools --index INDEX --file ./index.json -o "127.0.0.1"
    Indexer examples:
        Template documents to index:
            estools --index INDEX --template ./template.json -o "127.0.0.1"
        Template documents to file:
            estools --index INDEX --template ./template.json --file ./index.json
    """
    if version:
        from estools import __version__

        print(f'EsTools version {__version__}')
        return

    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(
            'cli()',
            extra=logger_ctx(
                {
                    'f': file,
                    'inp': inp,
                    'index': index,
                    'out': out,
                    'size': size,
                    'sort': sort,
                    't': template,
                }
            ),
        )

    try:
        EsTools(
            file_path=file, inp=inp, index=index, out=out, size=size, sort=sort, template=template
        )
    except Exception as e:
        ctx = {'type': type(e)}

        # log error
        logger.exception(str(e), extra={'ctx': ctx})

        # output error
        print(click.style(f' {str(e).strip()} ({ctx}) ', bg='red'))
