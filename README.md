# Airflow Layout
`airflow_home` houses all the relevant files for Airflow. `export AIRFLOW_HOME='pwd'/airflow_home` is used to set the environment variable that gives Airflow a home. If this is not run on each relevant machine Airflow will not find the config file.

__ `dags` houses all of the Directed Acyclic Graphs (DAGs) that have been defined and will be scheduled and run.
    A DAG specifies the workflow for a set of tasks. In these files, the schedule, operators, order/dependency are defined.
    The `rawsync_dag` will use a PostgresOperator to connect to the Redshift and run the SQL query.

__ `sql` houses the various SQL queries that should be run. These files will be called by an Operator within the DAG.

__ `airflow.cfg` defines all the specifics for Airflow - everything from which executor to use (Sequential, Local, or Celery), to the Postgres meta-database connection, and the message broker connection and everything else is specified here.

__ `logs` the logs files are not tracked with git. This folder contains logs for DAG runs (DAGs that have been initalized and run) as well as the tasks (Operators that have been initialized and run) in separate folders and files by date.

________________________________________________________________________________

# Airflow Commands
`airflow webserver` starts the webserver that powers the UI (usually port 8080)

`airflow scheduler` starts the scheduler (and worker depending on the Executor that was selected)

`airflow worker` starts a worker that can be used with the CeleryExecutor

`airflow flower` starts a server to show the status of any workers (usually port 5555)

________________________________________________________________________________

# Setting Up Airflow

### Install Python

### Install Airflow and Airflow Packages
`pip install airflow[crypto,celery,postgres,redis]`

### Create project home and export ENV variable for AIRFLOW_HOME
```
mkdir airflow_home
export AIRFLOW_HOME=`pwd`airflow_home
airflow version
```

### Create needed directories
`mkdir dags`
`mkdir logs`

### Change the Executor to CeleryExecutor (Recommended for production)
`executor = CeleryExecutor `

### Point SQL Alchemy to Postgres (username, password, host, db_name)
`sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/motiv_airflow `

### Are DAGs paused by default at creation
`dags_are_paused_at_creation = True `

### Don’t load examples
`load_examples = False `

### Set the Broker URL (If you’re using CeleryExecutors)
`broker_url = redis://localhost:6379/0`

### Point Celery to Postgres
celery_result_backend = db+postgresql+psycopg2://airflow:airflow@localhost:5432/motiv_airflow

### Install postgres
`brew install postgresql` # For Mac, the command varies for different OS

### Connect to the database
`psql -d postgres # This will open a prompt`

### Operate on the database server

- `\l` # List all databases

- `\du` # List all users/roles

- `\dt` # Show all tables in database

- `\h` # List help information

- `\q` # Quit the prompt

### Create a meta db for airflow
`CREATE DATABASE motiv_airflow;`

### Create a user and grant privileges (run the commands below under superuser of postgres)
```
CREATE USER airflow WITH ENCRYPTED PASSWORD ‘airflow’;
GRANT ALL PRIVILEGES ON DATABASE motiv_airflow TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
```

### Make sure config has correct database name
`airflow initdb`

### Establish your own connections via the web UI; you can test your DB connections via the Ad Hoc Query (see here)
#### Go to the web UI: Admin -> Connection -> Create

- Connection ID: name it

- Connection Type: e.g., database/AWS

- Host: e.g., your database server name or address

- Scheme: e.g., your database

- Username: your user name

- Password: will be encrypted if airflow[crypto] is installed

- Extra: additional configuration in JSON, e.g., AWS credentials

### Encrypt your credentials
#### Generate a valid Fernet key and place it into airflow.cfg
`FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print FERNET_KEY")`

________________________________________________________________________________

# Troubleshooting
### `airflow scheduler` is not using the Executor that I specified in configuration
You may need to rerun `export AIRFLOW_HOME='pwd'/airflow_home`

### `airflow flower` is giving warnings/errors
The Flower UI has a reset button; if you started the worker before starting Flower you may need to reset.

### Errors surrounding the meta-database
Check the connection string for sql_alchemy_conn and celery_result_backend. You may need to prepend `db+`
