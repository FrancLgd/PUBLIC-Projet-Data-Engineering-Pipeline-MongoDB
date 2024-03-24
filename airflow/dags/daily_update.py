import os
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

# Lire le fichier de config
dossier_courant = os.getcwd()
print("Le dossier courant est :", dossier_courant)

# Nom du DAG
DAG_NAME = os.path.basename(__file__).replace(".py", "")  # Le nom du DAG est le nom du fichier

# Arguments du DAG
default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

@dag(DAG_NAME, default_args=default_args, schedule_interval="30 2 * * *", start_date=days_ago(2))
def dag_update_6m_best_rated():

    """
    Ce DAG exécute le script de mise-à-jour quotidienne des jeux les mieux notés.
    """

    @task(provide_context=True)
    def get_run_command(**kwargs):

        # Obtenir la date du jour
        date_j = datetime.strptime(kwargs["date_j"], "%Y-%m-%d").strftime("%Y-%m-%d")

        return f"cd {dossier_courant} && python ./src/update_6m_best_rated.py --DATE_RUN {date_j}"
    
    run_script_task = BashOperator(
        task_id='run_script_task',
        bash_command=get_run_command(date_j="{{ ds }}")
    )
        
    run_script_task

# Instanciation du DAG
dag_instance = dag_update_6m_best_rated() 
