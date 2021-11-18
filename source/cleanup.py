import configparser
import psycopg2
from cluster_connect import ClusterConnector


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # setup connection to Redshift cluster
    connector = ClusterConnector(config)
    print("Get cluster endpoint.")
    try:
        connector.get_cluster_endpoint_arn()

        # Delete Redshift cluster and IAM role
        connector.delete_cluster()
        connector.delete_iam_role()
    except:
        print("Cluster does not exist.")


if __name__ == "__main__":
    main()