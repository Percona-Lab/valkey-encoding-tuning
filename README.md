# valkey-encoding-tuning

Valkey (and Redis) internally encodes hash keys as a [listpack](https://github.com/antirez/listpack/blob/master/listpack.md), which is very memory-efficient. But, if a field in the hash exceeded the `hash-max-listpack-value` (by default, 64 characters), then it will be encoded as a hashtable instead.

This tool will scan/analyze the whole Valkey dataset, to calculate the size statistics for Valkey/Redis datatypes (hashes, lists, sets, zsets) , helping the administrators to determine the optimal value for encoding hash objects.

## Requirements

- Go 1.20+ (or compatible version)
- Valkey server (local or remote)

## Installation

Clone the repository:

```bash
git clone https://github.com/Percona-Lab/valkey-encoding-tuning.git
cd valkey-encoding-tuning
````

Install dependencies:

```bash
go mod tidy
```

## Usage

Build the project:

```bash
make build
```

Run the tool:

```bash
./valkey-encoding-tuning [flags]
```

Or directly:

```bash
go run ./cmd/... [flags]
```

### Available arguments

- `address` Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster, default to "127.0.0.1:6379"
- `database` Comma-separated list of database to analyze, default to 0
- `field-pattern` Pattern (regex style) of the hash fields to be analyzed
- `hash-key-pattern` Pattern (glob style) of the HASH keys to be analyzed
- `list-key-pattern` Pattern (glob style) of the LIST keys to be analyzed
- `output-file` Output file name (JSON format)
- `password` Password of the Valkey user
- `print-output` Print output to stdout (default true)
- `set-key-pattern` Pattern (glob style) of the SET keys to be analyzed
- `username` Name of the Valkey user
- `zset-key-pattern` Pattern (glob style) of the SORTED SET keys to be analyzed

## Unit tests

To run the unit tests

```bash
make test
```

### Example

```bash
go run ./cmd/... \
  --address=127.0.0.1:6379 \
  --username=default \
  --password=hello-world
```

Sample output:

```markdown
# DB 0 Analysis
## Hash Datatype
### Node 127.0.0.1:6379
#### Config
- hash-max-listpack-value=64
#### Analysis
- hashtable keys found: 4493/10000 (44.93% of all hash keys)
- hash fields count: 40000
- largest hash field: item:8269.description (field value), size:485 
- avg field size: 38.00
- hash fields' size distribution:
+ P10: 4.00
+ P20: 4.00
+ P30: 4.00
+ P40: 6.00
+ P50: 11.00
+ P60: 11.00
+ P70: 11.00
+ P80: 38.00
+ P90: 102.75
+ P100: 485.00

## List Datatype
### Node 127.0.0.1:6379
#### Config
- list-max-listpack-size=-2
- list-compress-depth=0
#### Analysis
N/A (no keys found)

## Set Datatype
### Node 127.0.0.1:6379
#### Config
- set-max-listpack-value=64
- set-max-listpack-entries=128
#### Analysis
N/A (no keys found)

## Sorted Set Datatype
### Node 127.0.0.1:6379
#### Config
- zset-max-listpack-value=64
- zset-max-listpack-entries=128
#### Analysis
N/A (no keys found)
```
