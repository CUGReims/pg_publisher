## About The Project

Using an interactive CLI, this publisher script will enable to
copy Postgresql views, tables, and schemas from one database to
another.

## Getting Started

### Prerequisites

To use in development:
- Requires docker

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

Create a git tag first then:

```shell
make package
```

Which will create the file named `pg_publisher-$(TAG)-linux-amd64.tar.gz`.

### For Windows

Comment `psycopg2-binary` in `requirements.txt`, then:

#### Windows

```bat
pip install pipwin
pipwin install psycopg2

cd pg_publisher
pip install -e .
pyinstaller --clean ./all.spec
```

Test executable:

```bat
.\dist\cli.exe
.\dist\cli_direct.exe
```

Now you can rename dist folder to something like `pg_publisher-v0.1.0-alpha2-windows-amd64`
and create a ZIP archive.


## Requirements (pip freeze)

    click==7.1.2
    pgtoolkit==0.23.0
    pkg_resources==0.0.0
    prompt-toolkit==3.0.38
    psycopg2-binary==2.9.3
    questionary==1.10.0
    pgpublisher-publisher==0.1.0
    typing_extensions==4.6.3
    wcwidth==0.2.6
