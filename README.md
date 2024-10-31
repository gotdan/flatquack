# FlatQuack

**FlatQuack is an open source tool to convert healthcare data in FHIR format into flat CSV, Parquet, JSON, or database tables that are ready for analysis using off-the-shelf tools. Not sure what this means? Check out the overview below :).**

## Overview

As the availability of healthcare data in [FHIR format](https://hl7.org/fhir) increases, there is a growing interest in using this data for analytic purposes. Most analytic and machine learning use cases require the preparation of FHIR data using transformations and tabular projections from its original, deeply nested form and authoring and maintaining these transformations is not trivial. The [SQL-on-FHIR](https://sql-on-fhir.org) specification defines a compact language to describe these transformations called a [FHIR ViewDefinition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ViewDefinition.html).

FlatQuack is an open source tool that compiles these ViewDefinitions into SQL that can be executed with [DuckDB](https://duckdb.org). DuckDB is a lightweight, scalable, open source database engine that runs at blazing speed thanks to its columnar engine, which supports parallel execution and can process larger-than-memory workloads. DuckDB natively supports reading and writing data in common formats like JSON, Parquet and CSV and can even integrate directly with remote endpoints such as AWS S3 buckets and databases such as Postgres, MySql and SQLite. FlatQuack takes advantage of this capability with the ability to specify templates that shape the SQL produced by FlatQuack to different use cases, making it easy to adapt to the specific naming conventions, input file formats, and output file formats in your workflow. Since FlatQuack just outputs SQL, it can be run directly on a FHIR dataset for a specific project or integrated into an existing data pipeline that uses orchestration and transformation tools like [Apache Airflow](https://airflow.apache.org/) and [dbt](https://getdbt.com).

## Alpha Version

FlatQuack is alpha software - it is not complete and you may run into bugs when using it. Please see the [project roadmap](#roadmap), [contribution guidelines](#contributing), and [support](#support) sections below for more information.

## Setup

1. Install [bun](https://bun.sh) - Bun is an open source Javascript runtime similar to node.js
2. You may also want to install the latest version of [DuckDB](https://duckdb.org) if you plan to execute the generated SQL queries outside of the FlatQuack command line interface (recommended for large datasets)

Additional steps if you would like to run scripts, unit tests or edit the project's source code:

3. Clone this repository and switch to that directory
4. Run the `bun install` command to install FlatQuack dependencies

You can also run it using Docker container:

```sh
docker build -t flatquack .
docker run --rm  -v "$(pwd):/app" -it flatquack

root@<container_hash>:/app# bunx flatquack
# *** compiling example/example.vd.json ***
# ... rest of output ...
```

## Running FlatQuack

#### `bunx flatquack`

(or `bun run ./src/cli.js` if you installed FlatQuack locally using steps 3 and 4 above)

#### Command line arguments

| name | short | default value | description |
| --- |  --- |  --- |  --- |
| `--mode` | `-m` | `preview` | Action FlatQuack should take with generated SQL. See the [modes section](#modes---mode-parameter) below for details. |
| `--view-path` | `-v` | current directory | The absolute or relative path to your ViewDefinition JSON files. Note that the -view-pattern parameter describes which files within this path will be used. |
| `--view-pattern` | `-p` |  `**/*.vd.json` | [Glob pattern](https://bun.sh/docs/api/glob) to define which files are ViewDefinitions. |
| `--template` | `-t` | `@csv` | Path to [template](#templates---template-parameter) to use when generating SQL. May be the name of a [sample template](#sample-templates) or the path to a [custom template](#custom-templates) |
| `--schema-file` | `-s` | FHIR R4 Schema | Path to a FHIR schema generated using the script included at `./scripts/build-fhir-schema.js`. This can be used to execute ViewDefinitions against FHIR data from versions other than R4.  See the [Generating a FHIR Schema](#generating-a-fhir-schema) seciton below for details.|
| `--param` | | | `name=value` pair of user defined variables to be used when generating SQL with a [custom template](#custom-templates). This argument may be repeated. |
| `--verbose` | | false | Print debugging information to the console when running FlatQuack. |

#### Modes (--mode parameter)
| name | description |
| --- |  --- |
| `preview` (default) | Generate SQL and display it in the console. |
| `build` | Generate SQL and save it in the same directory as the source ViewDefinition. |
| `run` | Execute the SQL and print the time it took to run in the console. |
| `explore` | Execute the SQL and print the query output in the console as JSON. Large queries should use the `build` action and run the resulting SQL files [directly with DuckDB](https://duckdb.org/docs/api/cli/overview#non-interactive-usage). |



## Templates (--template parameter)

### Sample Templates
| name | input | output |
| --- | --- | --- |
| [`@csv`](./templates/csv.sql) (default) | NDJSON FHIR Bulk Data files with a `.ndjson` extension and the resource type in the name | Flat CSV files with a header row |
| [`@parquet`](./templates/parquet.sql) | NDJSON FHIR Bulk Data files with a `.ndjson` extension and the resource type in the name | Parquet files ready for additional processing |
| [`@ndjson`](./templates/ndjson.sql) | NDJSON FHIR Bulk Data files with a `.ndjson` extension | NDJSON file with one line per output row and just the abstracted data|
| [`@dbt_model`](./templates/dbt_model.sql) | DBT Source named `fhir_db` with tables named as FHIR resource types | SQL Select statement that returns a flat table |
| [`@dbt_prehook`](./templates/dbt_prehook.sql) | NA | DuckDB SQL macros to load before executing a query generated with FlatQuack |
| [`@explore`](./templates/explore.sql) (default for the `explore` mode) | NDJSON FHIR Bulk Data files with a `.ndjson` extension and the resource type in the name | Flattened table with up to 10 results |

### Custom Templates
FlatQuack uses a very simple template language that replaces specific variables when they're placed between double brackets with values from the current execution (e.g., `{{ fq_input_dir }}`). Variable names and values not in the list below may be passed into the template processor using the `--param` command line argument and will be replaced if they appear they the template. This argument may also be used to pass in values that override the values of the built-in variables. Variables not in the list below or passed in as arguments will not be removed by the template engine to support their use in other processing steps such as DBT pipelines.

| name | description |
| --- | --- |
| `fq_input_dir`| Defaults to current working directory |
| `fq_output_dir`| Defaults to current working directory |
| `fq_sql_transform_expression` | SQL transformation generated from the `select` element of the ViewDefinition |
| `fq_where_filter` | SQL generated from the `where` element of the ViewDefinition |
| `fq_sql_input_schema` | SQL schema generated from FHIR elements used in the `select` and `where` elements of the ViewDefinition |
| `fq_sql_flattening_cols` | SQL columns that create the output columns based on the `column` elements in the ViewDefinition |
| `fq_sql_flattening_tables` | SQL joins that create the output columns based on the `column` elements in the ViewDefinition  |
| `fq_vd_name` | The value in the `name` element of the ViewDefinition |
| `fq_vd_resource` | The value in the `resource` element of the ViewDefinition |
| `fq_sql_macros` | DuckDB SQL macros to load before executing a query generated with FlatQuack |

## Generating a FHIR Schema
The schema for FHIR R4 is included with FlatQuack, but you may want to execute ViewDefinition files against other FHIR versions as well. To do this you can generate schema files for those version and pass them in with the `--schema-file` command line argument.

To generate a schema:
1. Download and decompress the FHIR definitions in JSON format from `https://hl7.org/fhir/downloads.html` (or the corresponding URL for the FHIR version you want to use).
2. Run the script:
    ```bash
    bun ./scripts/build-fhir-schema.js {path to FHIR definitions} {output file path}
    ```
The script accepts two positional parameters:
- The path to the directory where the FHIR definition files in JSON format are located (e.g., `../fhir/R4`)
- The path for the schema file (e.g. `../schemas/fhir-schema-r4.json`)

## Roadmap

- [ ] Value Set support
- [ ] Unions with nested select elements
- [ ] Constants in ViewDefinitions
- [ ] Boundary functions in FHIR Path expressions
- [ ] Watch command line mode for exploratory queries

## Contributing

### Pull Requests
Pull requests are very welcome! Apart from minor bug fixes, please start by opening [an issue](/issues) to discuss your plans and make sure they're aligned with other work being done on the project.

### Running the tests
```bash
bun test
```

### Generating specification test output file
1. Ensure that the test files in the `./tests/spec-tests` directory are up to date
2. Run `bun ./scripts/build-test-output.js`.
3. Output file is written as `flatquack_test_output.json`

## Support

As an open source project, support is provided for FlatQuack on a best effort basis. Interested in commercial support, training, or sponsoring the addition of a feature to the app? Please get in touch - Dan (at) CentralSquareSolutions.com.