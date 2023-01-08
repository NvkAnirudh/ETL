from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt

# Dag Arguments
dag_arguments = {
    'owner': 'Anirudh',
    'start_date': dt.datetime(2022, 12, 5),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

# Dag Definition
dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = dt.timedelta(minutes=60),
    default_args = dag_arguments,
    description = 'Apache Airflow'
)

# Task Definition
# Unizipping
unzip_data = BashOperator(
    task_id = 'unzip',
    bash_command = 'tar zxvf ~/cs779_project/tolldata.tgz',
    dag = dag
)

# extraction from csv
extract_data_from_csv = BashOperator(
    task_id = 'extraction_from_csv',
    bash_command = 'cut -d"," -f1,1-5 ~/cs779_project/vehicle-data.csv > ~/cs779_project/csv_data.csv',
    dag = dag
)

# extraction from csv
extract_data_from_tsv = BashOperator(
    task_id = 'extraction_from_tsv',
    bash_command = 'cut -f6-7 ~/cs779_project/tollplaza-data.tsv > ~/cs779_project/tsv_data.csv',
    dag = dag
)

# extraction from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extraction_from_fixed_width',
    bash_command = 'awk \'{print $(NF-1)}\' ~/cs779_project/payment-data.txt > ~/cs779_project/fixed_width_data.csv',
    dag = dag
)

# Consolidation
consolidate_data = BashOperator(
    task_id = 'consolidation',
    bash_command = 'paste ~/cs779_project/csv_data.csv ~/cs779_project/tsv_data.csv ~/cs779_project/fixed_width_data.csv > ~/cs779_project/extracted_data.csv',
    dag = dag
)

# Transformation - converting the fourth field to uppercase in the consolidated data
transform_data = BashOperator(
    task_id = 'tranformation',
    bash_command = 'awk -F\, \'{$4=toupper($4)}1\' OFS=\, ~/cs779_project/extracted_data.csv > transformed_data.csv',
    dag = dag
)

# Transformation - converting the tab delimiter to comma delimiter
transform_data2 = BashOperator(
    task_id = 'tranformation_2',
    bash_command = 'tr "\t" "," < ~/cs779_project/extracted_data.csv > ~/cs779_project/transformed_data2.csv',
    dag = dag
)

# Loading the accumulated data into PostgreSQL
load_data = BashOperator(
    task_id = 'loading',
    bash_command = 'psql --username=postgres --host=localhost -c "\copy tolldata FROM \'~/cs779_project/extracted_data4.csv\' delimiter \',\' csv"',
    dag = dag
)

# Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data >> transform_data2 >> load_data


