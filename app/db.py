import pathlib
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


BASE_DIR = pathlib.Path(__file__).parent
CLUSTER_BUNDLE = str(BASE_DIR / "ignored" / "connect.zip")


def get_cluster():
    cloud_config= {
        'secure_connect_bundle': 'connect.zip'
    }
    auth_provider = PlainTextAuthProvider('<<CLIENT ID>>', '<<CLIENT SECRET>>')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    
    
def get_session():
    cluster = get_cluster()
    return cluster.connect()


session = get_session()