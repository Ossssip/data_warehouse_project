a project to study data warehousing

# Data and Tools
- the project uses motor racing data from [the Ergast database](https://ergast.com/mrd/)
- the data is stored in BigQuery
- data transformation is done using [dbt-core](https://github.com/dbt-labs/dbt-core)
- [Apache Airflow](https://github.com/apache/airflow) to schedule fetching of a new data and dbt transformations
- [Looker](https://lookerstudio.google.com) to visualise the data

## How to setup
create and activate a virtual enviroment for the project [venv](https://docs.python.org/3/library/venv.html) or [conda](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html), install dbt and airflow (note that airflow does not work under Windows): `pip install dbt-bigquery apache-airflow`
### set up BigQuery
 - [create a BigQuery project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) and a dataset inside
 - create [a service account and obtain the key file](https://holowczak.com/creating-a-service-account-and-key-file-for-google-bigquery/). This key is then used in dbt's profiles.yml and in the python script uploading frech data to BigQuery

### set up dbt
- If not cloning this repo, initialize the project: `dbt init`. The command creates the basic folder structure.
- edit ~/.dbt/profiles.yml (see profiles_example.yml): set there your project id, dataset, path to the authentication key file.

### set up Airflow
 - initialize airflow: `ariflow initdb`
 - in ~/airflow/airflow.cfg make sure that  `dags_folder` points to the airflow subfolder in this project
 - the airflow/dag.py script first runs 
