from airflow.plugins_manager import AirflowPlugin
from s3_to_redshift_operator.operators.s3_to_redshift import S3ToRedshiftOperator
from s3_to_redshift_operator.macros.redshift_auth import redshift_auth


class S3ToRedshiftPlugin(AirflowPlugin):
    name = "S3ToRedshiftPlugin"
    operators = [S3ToRedshiftOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = [redshift_auth]
    admin_views = []
    flask_blueprints = []
    menu_links = []
