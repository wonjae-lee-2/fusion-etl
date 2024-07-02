# fusion_etl

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `fusion_etl/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `fusion_etl_tests` directory and you can run tests using `pytest`:

```bash
pytest fusion_etl_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.

## Comment

### Generate `requirements.txt`

Use `--no-deps` to add only those packages explicitly listed. When you run `uv pip compile` on Windows, it adds several dependecies that cannot be installed on Linux.

```bash
uv pip compile setup.py --no-deps --extra dev -o .devcontainer/requirements.txt
```

### Launch Dagster webserver

When you launch a Dagster webserver inside a container, make sure to set the `-h` and `-p` option to allow connections from outside the container through the open port.

```bash
dagster dev -h 0.0.0.0 -p 3000
```

### MSRP HCR_BIRCPT_MVW

This table's RATE_DIV column has several 0E-8 which bulk insert cannot handle. You need to manually replace them with 0 before bulk insert.

### BI Publisher jobs

Some erp assets are split into one calendar year to reduce the number of rows returned. For those assets, you need to schedule reports with a job name that corresponds to the 'source' property in ERP_MAPPINGS. If not, the erp timestamp sensor cannot find the job.

### Password change

When you change the password, don't forget to update .env, azure profile and token as well as github secrets.
