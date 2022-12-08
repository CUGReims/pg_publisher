## About The Project

Using an interactive CLI, this publisher script will enable to
copy Postgresql views, tables, and schemas from one database to
another.

## Getting Started

### Prerequisites

Requires docker and docker-compose

### Installation

1. Clone the repo
```shell
git clone
make build
```

## Usage

```shell
make up
make generate_dummy_src_data # generates dummy data
make cli
```

## Demo

<img src="./intro.gif">


## Tests

```shell
make tests
```


## Creating a binary for both Windows and Linux

```shell
pyinstaller cli.py -F --onefile
cd dist
./cli.py
```
