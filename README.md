# valkey-encoding-tuning
Valkey (and Redis) internally encodes hash keys as a [listpack](https://github.com/antirez/listpack/blob/master/listpack.md), which is very memory-efficient. But, if a field in the hash exceeded the `hash-max-listpack-value` (by default, 64 characters), then it will be encoded as a hashtable instead.

This tool will scan/analyze the whole Valkey dataset, to calculate the data size statistics for the hash keys, helping the administrators to determine the optimal value for encoding hash objects.

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

### Example

```bash
go run ./cmd/... \
  --address=127.0.0.1:6379 \
  --username=default \
  --password=hello-world
```
