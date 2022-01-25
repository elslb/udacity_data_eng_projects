from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 truncate='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        """
        Inserts data into dimension tables.
        Truncation is performed first to clear the tables prior to loading the data.
        """
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        redshift_hook.run(f"INSERT INTO {self.table}{self.sql_query}")
        self.log.info(f"Task successfully executed: {self.task_id}")
