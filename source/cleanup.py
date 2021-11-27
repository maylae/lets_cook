import configparser
import psycopg2
import os
import sys

from cluster_connect import ClusterConnector


def main():
    config = configparser.ConfigParser()
    config_file = os.path.join(sys.path[0], 'dwh.cfg')
    config.read(config_file)

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