# EsTools: Exporter, Importer & Indexer for Elasticsearch

- [Usage](#usage)
  - [Docker](#docker)
  - [Script](#python-script)
  - [Executable](#executable)
- [Options](#options)
- [Exporter](#exporter)
- [Importer](#importer)
- [Indexer](#indexer)
  - [Using a Template](#using-a-template)
  - [Template Fields](#template-fields)

## Usage

EsTools can be used as a Python script, a dynamically linked executable or run inside a Docker container.

### Docker

Run the official Docker image as a container:

```
docker run --rm ospreyfms/estools
```

> If local image already exists, upgrade to the latest build using: `docker pull ospreyfms/estools`

### Python Script

Download the latest source code and install:

```
pip install -r ./requirements.txt
```

Then run as a script:

```
python3 ./src/main.py
```

### Executable

Build a dynamically linked executable (pyinstaller):

```
pip install -r ./requirements-dev.txt
./build.sh
```

Then run the executable:

```
./dist/estools
```

## Options

### Debug

The `-d` or `--debug` option can be used to enable debugging output in the log (`./estools.log`).

### File

The `-f` or `--file` option can be used when the output or input is a file. For input the file must exist. For output the file must not exist.

### Index

The `--index` option is used to set the export/import index name.

### Input

The `-i` or `--inp` option can be a single Elasticsearch host or a multiple Elasticsearch hosts, and can include the URL scheme and port number.

Examples of valid hosts:

- `127.0.0.1` or `es1.example.com` - host IP address or domain name
- `http://127.0.0.1` - host with URL scheme (the default is `http`)
- `127.0.0.1:9200` - host with port number
- `http://127.0.0.1:9200` - host with URL scheme and port number

Separate multiple hosts with commas:

- `192.168.1.101,192.168.1.102,192.168.1.103` - multiple hosts
- `192.168.1.101,http://192.168.1.102,192.168.1.103:9200` - multiple hosts can also include URL scheme and/or port number

### Output

The `-o` or `--out` option can be a single Elasticsearch host or a multiple Elasticsearch hosts. See _Examples of valid hosts_ in the [Input](#input) section.

### Size

The `--size` option is used to set the number of batch objects in each Elasticsearch operation. The default size is `10,000`.

### Sort

The `--sort` option is used to sort by field(s) when exporting from Elasticsearch.

Examples of valid sorts:

- `field` - sort by `field` in ascending order
- `field:desc` - sort by `field` in descending order (the default is `asc`)
- `field,field2:desc` - sort by `field` in ascending order and sort by `field2` in descending order

### Template

The `-t` or `--template` option is used to set the template file path. The template file must exist.

## Exporter

The exporter is used to export documents from an index.

Export from an index to a file:

```
estools --index INDEX -i "127.0.0.1" --file ./index.json
```

Export from an index to stdout:

```
estools --index INDEX -i "127.0.0.1" > ./index.json
```

Export from an index to stdout with gzip:

```
estools --index INDEX -i "127.0.0.1" | gzip > ./index.json
```

## Importer

The importer is used to import documents to an index.

Import from a file to an index:

```
estools --index INDEX --file ./index.json -o "127.0.0.1"
```

## Indexer

The indexer can be used to dynamically generate and index documents based on a template.

Index documents example:

```
estools --index INDEX --template ./template.json -o "127.0.0.1"
```

Generate documents to a file example:

```
estools --index INDEX --template ./template.json --file ./index.json
```

Generate documents to stdout and compress using gzip example:

```
estools --index INDEX --template ./template.json | gzip > ./index.json.gz
```

### Using a Template

Example template:

```
{
	"$total": 5,
	"$concat": {
		"id": "$id",
		"name": "$index"
	},
	"$loop": {
		"value": [1, 2, 3]
	},
	"$rand": {
		"randValue": ["a", "b", "c"]
	},
	"$timestamp": {
		"timestamp": "now"
	},
	"id": null,
	"name": "test-"
}
```

Example output using template above:

```
{"id": "1", "name": "test-0", "value": 1, "randValue": "c", "timestamp": 1670582540}
{"id": "2", "name": "test-1", "value": 2, "randValue": "b", "timestamp": 1670582540}
{"id": "3", "name": "test-2", "value": 3, "randValue": "b", "timestamp": 1670582540}
{"id": "4", "name": "test-3", "value": 1, "randValue": "c", "timestamp": 1670582540}
{"id": "5", "name": "test-4", "value": 2, "randValue": "c", "timestamp": 1670582540}
```

### Template Fields

Below are special fields used to dynamically generate document field values.

#### `$concat`

Concatenate the field value with:

- `$id` the current document index plus one
- `$index` the current document index

For example:

```
{
	"$concat": {
		"name": "$index"
	},
	"name": "test-"
}
```

Example output:

```
{"name": "test-0"}
{"name": "test-1"}
{"name": "test-2"}
{...}
```

To use the concatenate value as the field value set the field value to `null`, for example:

```
{
    "$concat": {
        "id": "$id"
    },
    "id": null
}
```

Example output:

```
{"id": "1"}
{"id": "2"}
{...}
```

#### `$loop`

Continually loop through the provided array and set the field value based on current loop index, for example:

```
{
    "$loop": {
        "value": [1, 2, 3]
    }
}
```

Example output:

```
{"value": 1}
{"value": 2}
{"value": 3}
{"value": 1}
{...}
```

A `$loop` also excepts a string value for loading values from a file, like this example `values.txt` file:

```
aaa
bbb
ccc
```

The example template:

```
{
    "$loop": {
        "value": "./values.txt"
    }
}
```

Example output:

```
{"value": "aaa"}
{"value": "bbb"}
{"value": "ccc"}
{"value": "aaa"}
{...}
```

#### `$rand`

Set the field value to a random value from an array of values, for example:

```
{
	"$rand": {
		"value": ["a", "b", "c"]
	}
}
```

Example output:

```
{"value": "b"}
{"value": "b"}
{"value": "c"}
{...}
```

A `$rand` also excepts a string value for loading values from a file, like this example `values.txt` file:

```
aaa
bbb
ccc
```

The example template:

```
{
    "$rand": {
        "value": "./values.txt"
    }
}
```

Example output:

```
{"value": "bbb"}
{"value": "aaa"}
{"value": "bbb"}
{...}
```

A `$rand` also excepts an object with the `low` and `high` fields that can be either integers or floating point numbers, for example:

```
{
	"$rand": {
		"value": { "low": 1.0, "high": 10.0 }
	}
}
```

Example output:

```
{"value": 4.59}
{"value": 6.14}
{"value": 9.28}
{...}
```

#### `$timestamp`

Set the field value to a POSIX timestamp. The value can be a string like `now` or an array like `["now", "1h", "2h"]`. When the value is an array the values will be selected based on the current loop index. All `$timestamp` values will reduce the amount of time from `now`.

Example `$timestamp` values:

- `now` - current timestamp
- `10s` - 10 seconds ago
- `30m` - 30 minutes ago
- `5h` - 5 hours ago
- `1d` - 1 day ago

Example template:

```
{
	"$timestamp": {
		"ts": ["now", "1s", "2s"]
	}

```

Example output:

```
{"ts": 1670580592}
{"ts": 1670580591}
{"ts": 1670580590}
{"ts": 1670580592}
{...}
```

A `$timestamp` also excepts an object with the `every` and `reduce` fields. The `every` field value determines how often (by amount of documents generated) to increment the `reduce` value by the `reduce` value. The `reduce` value is the amount of time to reduce from the previous value. Example template:

```
{
	"$timestamp": {
		"ts": { "every": 2, "reduce": "1s" }
	}
}
```

Example output:

```
{"ts": 1670580767}
{"ts": 1670580767}
{"ts": 1670580766}
{"ts": 1670580766}
{"ts": 1670580765}
{"ts": 1670580765}
{...}
```

#### `$total`

The total number of documents to generate. A single document will be generated when `$total` is not set. Example template:

```
{
	"$total": 3,
	"name": "test"
}
```

Example output:

```
{"name": "test"}
{"name": "test"}
{"name": "test"}
```

#### `$type`

Convert the field value to a `float`, `int` or `str`, for example:

```
{
	"$type": {
		"number": "float"
	},
	"number": 1
}
```

Example output:

```
{"number": 1.0}
```
