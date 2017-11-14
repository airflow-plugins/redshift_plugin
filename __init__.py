from airflow.plugins_manager import AirflowPlugin
from redshift_plugin.operators.s3_to_redshift import S3ToRedshiftOperator


class S3ToRedshiftPlugin(AirflowPlugin):
    name = "S3ToRedshiftPlugin"
    operators = [S3ToRedshiftOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
