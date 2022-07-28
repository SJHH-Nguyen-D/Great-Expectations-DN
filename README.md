# About

There are two components to this project:

1. Great Expectations
2. PostgreSQL store for validation and expectation results


## Postgres DB startup

Run postgresql container after build with:

    docker run --name postgres --detach -p 5432:5432 postgres

NOTE: just make sure that the port is not already occupied by your other local running postgres service at the same port:

    sudo kill -9 (sudo lsof -t -i:5432)

Apparently, you can't connect to your PostgreSQl container with SQLAlchemy or any connection interface if you don't have a working connection string. By default, there is no password to logging into your database service container and that's a no-go. This is because it defaults to a `trust` setting for all types of connections, meaning that a password is not required to connect or log into the database.

This can be rectified by changing METHOD of authentication from your various connection types (local, host, etc) to either `password`, `md5`, or `scram-sha-256` in your `pg_hba.conf` file in your PostgreSQL container under `/var/lib/postgresql/data/`.

We can simply copy down the original `pg_hba.conf` from the container with `docker cp`, then edit to to have `md5`, `password`, or `scram-sha-256` whereever there is `trust`, and then `COPY` it into the container to under the right location (ie., `/var/lib/postgresql/data/pg_hba.conf`), and then reload it with either:

1) `pg_ctl reload` using the `pg_ctl` tool in the container (which doesn't seem to work for me)

OR 

2) 'sign in' with `psql` using the `postgres` user and then running the `SELECT pg_reload_conf();` command, and then quitting out with `\q`.

The commands are listed below as well.

if you want to use the default user, once you've shelled into the postgres container, you can run the below command to access the default `postgres` role:

    cp ./docker-entrypoint-initdb.d/pg_hba.conf /var/lib/postgresql/data/
    psql -h localhost -U postgres
    SELECT pg_reload_conf();
    \q

Then you can login with any of the roles that were defined in the `init.sql`, but this time you will have to use your password to login. This also allows for the PostgreSQL database service to be reachable through SQLAlchemy or whatever connection API you are using. 

<br>

## Great Expectations Anatomy

There are 4 steps to creating a Great Expectations Deployment

1. Initialize the Data Context
<hr>

Data Context is a folder (locally or specified elsewhere with the Data Context Config class) that is the entry point for GE to set its configurations to run everything.

In the root directory, run the below command to initialize the Great Expectations Data Context

    great_expectations init

Go through the prompts. After you've gone through the prompts, a directory called `great_expectations` will be created (Your data context) with various Great Expectations configurations and files. One of the main configuration files is called the `great_expectations/great_expectations.yml` file.

<br>

2. Connect to Data
<hr>

Using the Data Connector object, which provides an execution engine (for interacting with your data layer such as Pandas, PySpark, SQlAlchemy), and data connector (allows access to the data, be it a flat file or a relational database store)

Create a new `Data Source` with:

    great_expectations datasource new

Go through the prompts, identifying your preferred execution engine and the path to the directory where your data files are stored

From here, a Jupyter Notebook opens, in which you can edit the configurations highlighted in the notebook. 

You will want to update the `datasource_name` and `datasource_yaml` variables.

You can run all the cells which outputs an updated `great_expectations/great_expectations.yml` file.

Close and optionally delete the notebook under `great_expectations/uncommited/`.

<br>

3. Create Expectations

<hr>

Expectations are the core verifiable assertions about the nature of your data. A collection of Expectations is called an Expectation Suite.

Create an expectation suite by running:

    great_expectations suite new

Go through the prompts and select the configurations that fits your project use case.

A familar Jupyter Notebook opens up for which we can edit the configurations.

Some of the key configurations that you will need to change include:

    1. Uncommenting out the column names that you want to build
    2. Configuring the UserConfigurableProfiler

After you've done those, run the cells to create your boiler plate validations

This will open up a `Data Docs` UI tab in your browser indicating that your expectations have been created. Here we can look through the types of validations that your Expectation Suite tests for with your data.

If you want to create more customized expectations about your data, you can learn more about it [here](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview/).

<br>

4. Validate Data
<hr>


Once you run your selected Expectation Suite(s) against your Batch or Batch Request of data, you validate your data with a Checkpoint object, which produces (on disk or some hard store) validation results and metrics in your Data Docs, giving you a high level human readable understanding of the results of your Expectations against your batch. The results are stored in JSON, however you can further configure how and where this is stored for later consumption and analysis.

A Checkpoint runs an expectation suite against a batch of data (or batch request), which produces validation results, for which the checkpoint can be configured to perform additional actions such as persisting the validation results.

Create a Checkpoint by running:

    great_expectations checkpoint new <name_of_your_checkpoint>

A notebook is opened upon running the above.

Here you can edit the `data_asset_name` variable, which plugs into the `yaml_config` variable.

By running all the cells, a validation run is performed, thus building the `Data Docs` and the validation objects (along with any other actions you've configured the Checkpoint to perform). A `Data Docs` UI should open up with the results of the validation run of your expectation suite`

<br>

## NOTICE when using PySark as Execution Engine

You must run the `ATTENTION_RUN_TO_SET_ENV_VARS.sh` file to export the `JAVA_HOME` and `SPARK_HOME` env vars, otherwise generating an expectation suite using the PySpark execution engine results in some JAVA related error.
