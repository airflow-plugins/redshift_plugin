from airflow.utils.db import provide_session
from airflow.models import Connection


@provide_session
def get_conn(conn_id, session=None):
    conn = (
        session.query(Connection)
        .filter(Connection.conn_id == conn_id)
        .first())
    return conn


def redshift_auth(s3_conn_id):
    s3_conn = get_conn(s3_conn_id)
    aws_key = s3_conn.extra_dejson.get('aws_access_key_id')
    aws_secret = s3_conn.extra_dejson.get('aws_secret_access_key')
    return ("aws_access_key_id={0};aws_secret_access_key={1}"
            .format(aws_key, aws_secret))
