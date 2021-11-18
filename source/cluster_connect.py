import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd

class ClusterConnector():
    """ Class for setting up a redshift cluster with all necessary resources and connecting to it """
    
    def __init__(self, config):
        self.KEY                    = config.get('AWS','KEY')
        self.SECRET                 = config.get('AWS','SECRET')

        self.DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
        self.DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
        self.DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

        self.DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        self.DWH_DB                 = config.get("DWH","DWH_DB")
        self.DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
        self.DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
        self.DWH_PORT               = config.get("DWH","DWH_PORT")

        self.DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
        
        self.setup_resources()    
    
    def setup_resources(self):
        """ Create the necessary resources and clients for working with Redshift """
        self.ec2 = boto3.resource('ec2',
                               region_name="us-west-2",
                               aws_access_key_id=self.KEY,
                               aws_secret_access_key=self.SECRET
                            )

        self.iam = boto3.client('iam',aws_access_key_id=self.KEY,
                             aws_secret_access_key=self.SECRET,
                             region_name='us-west-2'
                          )

        self.redshift = boto3.client('redshift',
                               region_name="us-west-2",
                               aws_access_key_id=self.KEY,
                               aws_secret_access_key=self.SECRET
                               )

    def create_iam_role(self):
        """ Create a new IAM role, that impersonates Redshift and """
        #1.1 Create the role
        try:
            print("1.1 Creating a new IAM Role") 
            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.DWH_IAM_ROLE_NAME,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )    
        except Exception as e:
            print(e)

        print("1.2 Get the IAM role ARN")
        self.roleArn = self.iam.get_role(RoleName=self.DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(self.roleArn)
        
    def create_cluster(self):
        """ Create the redshift cluster with the created IAM role """
        if not self.roleArn:
            print("Please create IAM role first.")
            return None
        try:
            response = self.redshift.create_cluster(        
                #HW
                ClusterType=self.DWH_CLUSTER_TYPE,
                NodeType=self.DWH_NODE_TYPE,
                NumberOfNodes=int(self.DWH_NUM_NODES),

                #Identifiers & Credentials
                DBName=self.DWH_DB,
                ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,
                MasterUsername=self.DWH_DB_USER,
                MasterUserPassword=self.DWH_DB_PASSWORD,
                
            )
        except Exception as e:
            print(e)
            
            
    def get_redshift_props(self):
        """ Get redshift cluster props """
        props = self.redshift.describe_clusters(ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        return props
    
    def get_cluster_endpoint_arn(self):
        """ Get cluster endpoint and IAMRoleArn. 
            Returns:
                - success (boolean): Returns True if cluster is available, else False.
        """
        try:
            props = self.get_redshift_props()
            if props['ClusterStatus'] == "available":
                self.DWH_ENDPOINT = props['Endpoint']['Address']
                self.DWH_ROLE_ARN = props['IamRoles'][0]['IamRoleArn']
                self.cluster_props = props
                print("DWH_ENDPOINT :: ", self.DWH_ENDPOINT)
                print("DWH_ROLE_ARN :: ", self.DWH_ROLE_ARN)
                return True
            else:
                return False
        except:
            return False
        
    def open_port(self):
        """ Open an incoming TCP port to access cluster endpoint """
        try:
            vpc = self.ec2.Vpc(id=self.cluster_props['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.DWH_PORT),
                ToPort=int(self.DWH_PORT)
            )
        except Exception as e:
            print(e)
            
    def delete_cluster(self):
        """ Delete the created Redshift cluster """
        self.redshift.delete_cluster( ClusterIdentifier=self.DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        
    def delete_iam_role(self):
        self.iam.delete_role(RoleName=self.DWH_IAM_ROLE_NAME)

