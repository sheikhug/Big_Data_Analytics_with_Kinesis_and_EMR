import * as cdk from 'aws-cdk-lib';
import {
    Stack,
    StackProps,
    Aws,
} from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as emr from 'aws-cdk-lib/aws-emr';
import { Construct } from 'constructs';

const MASTER_INSTANCE_TYPE = 'm5.xlarge';
const CORE_INSTANCE_TYPE = 'm5.xlarge';
const CORE_INSTANCE_COUNT = 2;
const MARKET = 'ON_DEMAND';
const CLUSTER_NAME = 'emr-pipeline-cluster';
const EMR_RELEASE = 'emr-6.10.0';
const SUBNET_ID = 'subnet-06f0daffbab90693d';

interface EMRClusterStackProps extends StackProps {
    s3LogBucket: string;
    s3ScriptBucket: string;
    vpcName: string;
}

export class EMRClusterStack extends Stack {
    constructor(scope: Construct, id: string, props: EMRClusterStackProps) {
        super(scope, id, props);

        const { s3LogBucket, s3ScriptBucket } = props;

        const scriptsLocation = `${s3ScriptBucket}/emr_pipeline/scripts`;

        // Enable reading scripts from the S3 bucket
        const readScriptsPolicy = new iam.PolicyStatement({
            effect: iam.Effect.ALLOW,
            actions: ['s3:GetObject'],
            resources: [`arn:aws:s3:::${scriptsLocation}/*`],
        });
        const readScriptsDocument = new iam.PolicyDocument({
            statements: [readScriptsPolicy],
        });

        // EMR service role
        const emrServiceRole = new iam.Role(this, 'EMRServiceRole', {
            assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AmazonElasticMapReduceRole'
                ),
            ],
            inlinePolicies: {
                readScriptsDocument: readScriptsDocument,
            },
        });

        // EMR job flow role
        const emrJobFlowRole = new iam.Role(this, 'EMRJobFlowRole', {
            assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
            managedPolicies: [
                iam.ManagedPolicy.fromAwsManagedPolicyName(
                    'service-role/AmazonElasticMapReduceforEC2Role'
                ),
            ],
        });

        // EMR job flow profile
        const emrJobFlowProfile = new iam.CfnInstanceProfile(
            this,
            'EMRJobFlowProfile',
            {
                roles: ['default_profile_role'],
                instanceProfileName: 'emrJobFlowProfile_',
            }
        );

        // Create EMR cluster
        const emrCluster = new emr.CfnCluster(this, 'EMRCluster', {
            instances: {
                coreInstanceGroup: {
                    instanceCount: CORE_INSTANCE_COUNT,
                    instanceType: CORE_INSTANCE_TYPE,
                    market: MARKET,
                },
                ec2SubnetId: SUBNET_ID,
                ec2KeyName: 'owolabiakintan',
                keepJobFlowAliveWhenNoSteps: true,
                masterInstanceGroup: {
                    instanceCount: 1,
                    instanceType: MASTER_INSTANCE_TYPE,
                    market: MARKET,
                },
            },
            steps: [
                {
                    actionOnFailure: 'CANCEL_AND_WAIT',
                    hadoopJarStep: {
                        jar: 'command-runner.jar',
                        args: ['hive', '-f', `s3://${scriptsLocation}/create_tables.hql`],
                    },
                    name: 'create_tables',
                },
                {
                    actionOnFailure: 'CANCEL_AND_WAIT',
                    hadoopJarStep: {
                        jar: 'command-runner.jar',
                        args: ['hive', '-f', `s3://${scriptsLocation}/transform_data.hql`],
                    },
                    name: 'transform_data',
                },
            ],
            jobFlowRole: 'EMR_EC2_DefaultRole',
            name: CLUSTER_NAME,
            applications: [
                {
                    name: 'Hive',
                },
            ],
            serviceRole: emrServiceRole.roleName,
            configurations: [],
            logUri: `s3://${s3LogBucket}/${Aws.REGION}/elasticmapreduce/`,
            releaseLabel: EMR_RELEASE,
            visibleToAllUsers: false,
        });
    }
}
