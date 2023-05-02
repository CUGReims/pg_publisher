## About The Project

Using an interactive CLI, this publisher script will enable to
copy Postgresql views, tables, and schemas from one database to
another.

## Getting Started

### Prerequisites

To use in development:
- Requires docker and docker-compose

To use the standalone executable:
- you will need to have psql installed on your machine.
- Make sure that the `psql` commands are available in your `PATH`.

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


## Creation of standalone executable

### For Linux

```shell
docker-compose run --rm --user `id -u`:`id -g` tester sh -c "cd /src && pyinstaller --clean /src/cli.spec"

chmod +x reims_publisher/dist/cli

./reims_publisher/dist/cli
```

### For Windows

Comment psycopg2-binary in requirements.txt, then:

```
pip install pipwin
pipwin install psycopg2

cd reims_publisher
pip install -e .
pyinstaller --clean /src/cli.spe
```
