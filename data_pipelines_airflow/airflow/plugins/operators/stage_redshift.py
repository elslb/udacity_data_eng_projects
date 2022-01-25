from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ('s3_key',)
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION '{}'
    TIMEFORMAT as 'epochmillisecs'
    IGNOREHEADER {}
    DELIMITER {}
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 table='',
                 region='',
                 ignore_headers='',
                 delimiter='',
                 file_format='JSON',
                 json_path ='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.s3_bucket = s3_bucket,
        self.s3_key = s3_key,
        self.table = table,
        self.region = region,
        self.ignore_headers = ignore_headers,
        self.delimiter = delimiter,
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        """
        Copies data from designated S3 bucket(s) into target tables in the assigned Redshift cluster.
        """
        self.log.info('Initializing AWS hook')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Clearing destination Redshift table')
        redshift_hook.run(f'DELETE FROM {table}')
        
        self.log.info('Copying data from designated S3 bucket to Redshift')
        if self.file_format == 'json':
            file_proc = f"JSON '{json_path}'"
        elif self.file_format == 'csv':
            file_proc = f"IGNOREHEADER '{ignore_header} DELIMITER '{delimiter}'"
            
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{s3_bucket}{rendered_key}"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            file_proc
        )
        redshift_hook.run(formatted_sql)
        
        self.log.info(f'Successfully copied {self.table}')
            
            