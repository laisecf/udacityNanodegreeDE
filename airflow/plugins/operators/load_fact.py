import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = "",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        insert_sql = "INSERT INTO {} ({})".format(self.table, self.sql)
        self.log.info('Inserting data into fact table')
        redshift.run(insert_sql)
        
