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
# Hash Datatype Analysis
## Node 127.0.0.1:30001
### Config
- hash-max-listpack-value=64
### Analysis
- hashtable keys found: 1552/3328 (46.63% of all hash keys)
- hash fields count: 6656
- largest hash field: item:6720.description, size:482 
- avg field size: 13.00
- hash fields' size distribution:
+ Quartile 1 (P25): 5.00
+ Quartile 2 (P50): 19.66
+ Quartile 3 (P75): 51.00
+ Quartile 4 (P99): 419.00

## Node 127.0.0.1:30002
### Config
- hash-max-listpack-value=64
### Analysis
- hashtable keys found: 1504/3343 (44.99% of all hash keys)
- hash fields count: 6686
- largest hash field: item:1173.description, size:477 
- avg field size: 13.00
- hash fields' size distribution:
+ Quartile 1 (P25): 5.00
+ Quartile 2 (P50): 21.50
+ Quartile 3 (P75): 49.94
+ Quartile 4 (P99): 426.00

## Node 127.0.0.1:30003
### Config
- hash-max-listpack-value=64
### Analysis
- hashtable keys found: 1518/3329 (45.60% of all hash keys)
- hash fields count: 6658
- largest hash field: item:1291.description, size:466 
- avg field size: 13.00
- hash fields' size distribution:
+ Quartile 1 (P25): 5.00
+ Quartile 2 (P50): 19.89
+ Quartile 3 (P75): 50.84
+ Quartile 4 (P99): 420.00

## Node 
### Config
- hash-max-listpack-value=
### Analysis
- hashtable keys found: 4574/10000 (45.74% of all hash keys)
- hash fields count: 20000
- largest hash field: item:6720.description, size:482 
- avg field size: 13.00
- hash fields' size distribution:
+ Quartile 1 (P25): 5.00
+ Quartile 2 (P50): 19.88
+ Quartile 3 (P75): 50.54
+ Quartile 4 (P99): 422.33

# List Datatype Analysis
## Node 127.0.0.1:30001
### Config
- list-max-listpack-size=-2
- list-compress-depth=0
### Analysis
N/A (no keys found)

## Node 127.0.0.1:30002
### Config
- list-max-listpack-size=-2
- list-compress-depth=0
### Analysis
N/A (no keys found)

## Node 127.0.0.1:30003
### Config
- list-max-listpack-size=-2
- list-compress-depth=0
### Analysis
N/A (no keys found)

## Node 
### Config
- list-max-listpack-size=
- list-compress-depth=
### Analysis
N/A (no keys found)
```
