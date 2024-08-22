# target-iceberg

`target-iceberg` is a Singer target for Iceberg.

Build with the [Meltano Target SDK](https://sdk.meltano.com).



## Installation

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-iceberg.git@main
```


## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| credential | True     | None    | Rest catalog user credential |
| catalog_uri | True     | None    | Catalog URI, e.g. https://api.catalog.io/ws/ |
| warehouse | True     | None    | The name of the catalog where data will be written |
| catalog_type | True     | None    | rest or jdbc |
| namespace | True     | None    | The namespace where data will be written|
| add_record_metadata | False    | None    | Add metadata to records. |
| validate_records | False    |       1 | Whether to validate the schema of the incoming streams. |
| stream_maps | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |A full list of supported settings and capabilities for this
target is available by running:

```bash
target-iceberg --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.


## Usage

You can easily run `target-iceberg` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-iceberg --version
target-iceberg --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-iceberg --config /path/to/target-iceberg-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Start by setting up the local catalog environment:

```bash
docker compose up
```

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-iceberg` CLI interface directly using `poetry run`:

```bash
poetry run target-iceberg --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._


Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-iceberg
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-iceberg --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-iceberg
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
