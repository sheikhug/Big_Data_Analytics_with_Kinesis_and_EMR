import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Stack, StackProps } from 'aws-cdk-lib';
import { KinesisStreamsToKinesisFirehoseToS3 } from '@aws-solutions-constructs/aws-kinesisstreams-kinesisfirehose-s3';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { aws_glue as glue2 } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_athena as athena } from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { aws_s3_deployment as s3deploy } from 'aws-cdk-lib';
import { aws_kinesisfirehose as firehose } from 'aws-cdk-lib';
import * as lambda_ from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';
import { aws_ec2 as ec2 } from 'aws-cdk-lib'
import * as emr from 'aws-cdk-lib/aws-emr';


const MASTER_INSTANCE_TYPE = 'm5.xlarge';
const CORE_INSTANCE_TYPE = 'm5.xlarge';
const CORE_INSTANCE_COUNT = 2;
const MARKET = 'ON_DEMAND';
const CLUSTER_NAME = 'emr-pipeline-cluster';
const EMR_RELEASE = 'emr-7.5.0';


export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

      const dataStream = new KinesisStreamsToKinesisFirehoseToS3(this, 'stream-firehose-s3', {
        kinesisStreamProps: {
          streamName: 'ingest-stream',
        },
        bucketProps: {
          bucketName: 'streambucket333444566700432',
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          autoDeleteObjects: true,
        },
        loggingBucketProps: {
          bucketName: 'streamlogging333444566700432',
          removalPolicy: cdk.RemovalPolicy.DESTROY,
          autoDeleteObjects: true,
        }
      });    

    const stream_name = dataStream.kinesisStream.streamName;
    const stream_bucket = dataStream.s3Bucket
    const stream_bucket_name = dataStream.s3Bucket?.bucketName || ''

    // dataStream.s3Bucket?.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY)
    // dataStream.s3LoggingBucket?.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY)
    dataStream.kinesisStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY)



    const glueDatabase = new glue.Database(this, 'GlueDatabase', {
      databaseName: 'glue_database',
      description: 'Glue database to store glue catalogue table definitions.',
      locationUri: 's3://' + dataStream.s3Bucket?.bucketName || ''
    });


    const glueRole = new iam.Role(this, 'GlueRole', 
      {
        assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      });

      glueRole.addToPolicy(new iam.PolicyStatement({
        actions: [
          's3:GetObject',
          's3:PutObject',
        ],
        // resources: ['*'],
        resources: [dataStream.s3Bucket?.bucketArn || ''],
      }));

      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceNotebookRole'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AWSGlueConsoleFullAccess'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AwsGlueSessionUserRestrictedNotebookServiceRole'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AWSGlueSchemaRegistryFullAccess'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AwsGlueSessionUserRestrictedServiceRole'));
      glueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'));



    const cfnCrawler = new glue2.CfnCrawler(this, 'ingest-batch', {
      role: glueRole.roleArn,
      targets: {
        s3Targets: [{
          path: dataStream.s3Bucket?.bucketName,
        }],
      },

      databaseName: glueDatabase.databaseName,
      schedule: {scheduleExpression: 'cron(0	*	*	*	?	*)'},
    });




    const athenaOutputBucket = new s3.Bucket(this, 'athenaoutputbucket', {
      bucketName: 'athenaoutputbucket333444566700432',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });


    

    const athenaWorkgroup = new athena.CfnWorkGroup(this, 'AthenaWorkGroup', {
      name: 'QuickSightWorkGroup',
      state: 'ENABLED',
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: athenaOutputBucket.s3UrlForObject(),
        },
      },
    });


    // Step 2: Create an IAM Role for the Lambda Function
    const lambdaRole = new iam.Role(this, 'AthenaLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonAthenaFullAccess'));
    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'));


    // Create a Lambda Function to Run Athena Queries
    const queryLambda = new lambda_.Function(this, 'AthenaQueryLambda', {

      runtime: lambda_.Runtime.PYTHON_3_9,
      code: lambda_.Code.fromInline(`
import boto3
import os

def handler(event, context):
  athena = boto3.client('athena')
  params = {
    'QueryString': f"select * from {os.environ['BUCKET_NAME']} where vendorid=2;",
    'QueryExecutionContext': {'Database': os.environ['DATABASE_NAME']},
    'ResultConfiguration': {'OutputLocation': os.environ['OUTPUT_LOCATION']}
  }
  result = athena.start_query_execution(**params)
  print('Query Execution ID:', result['QueryExecutionId'])
      `),
      handler: 'index.handler',
      role: lambdaRole,
      environment: {
        BUCKET_NAME: athenaOutputBucket.bucketName,
        DATABASE_NAME: glueDatabase.databaseName,
        OUTPUT_LOCATION: athenaOutputBucket.s3UrlForObject(),
      },
    });

    // Create an EventBridge Rule to Schedule the Lambda Function
    const queryscheduleRule = new events.Rule(this, 'AthenaScheduleRule', {
      schedule: events.Schedule.cron({ minute: '5' }), // Runs every 5 min.
    });

    // Add the Lambda function as the target for the EventBridge Rule
    queryscheduleRule.addTarget(new targets.LambdaFunction(queryLambda));





    const keyPair = new ec2.CfnKeyPair(this, 'ec2KeyPair', {
      keyName: 'keypairforEC2withEMR',
      keyType: 'rsa',
    });

    // import the default vpc
    const vpc = ec2.Vpc.fromLookup(this, 'DefaultVPC', {
      isDefault: true,
    });

    // get the sbunet id. 
    const subnetId = vpc.publicSubnets[0]?.subnetId;




    const s3ScriptBucket = new s3.Bucket(this, 's3scriptbucket', {
      bucketName: 's3scriptbucket333444566700432',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });
    
    const scriptDeploytment = new s3deploy.BucketDeployment(this, 'DeployFiles', {
      sources: [s3deploy.Source.asset('./lib/emr-scripts')], 
      destinationBucket: s3ScriptBucket,
    });    

    const ERMLogBucket = new s3.Bucket(this, 'emrlogputbucket', {
      bucketName: 'emrlogputbucket333444566700432',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });


    const scriptsLocation = `${s3ScriptBucket}`;

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

    stream_bucket?.grantReadWrite(emrJobFlowRole);  


    const emr_job_flow_profile = new iam.CfnInstanceProfile(this, "emr_job_flow_profile", 
      {
      roles: ["EMR_EC2_DefaultRole"],
      instanceProfileName: "emrJobFlowProfile_",
    });

    const emrClusterName = 'dynamic-trigger-emr-cluster';

    // Lambda to trigger EMR cluster
    const emrLaunchLambda = new lambda_.Function(this, 'EmrLaunchLambda', {
      runtime: lambda_.Runtime.PYTHON_3_9,
      code: lambda_.Code.fromInline(`
import boto3
import os

def handler(event, context):
    s3 = boto3.client('s3')
    emr = boto3.client('emr')
    bucket_name = os.environ['BUCKET_NAME']
    threshold = int(os.environ['THRESHOLD_BYTES'])
    
    # Calculate total size of the S3 bucket
    total_size = 0
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            total_size += sum(obj['Size'] for obj in page['Contents'])

    # Check if the threshold is met
    if total_size >= threshold:
        print(f'Threshold met: {total_size} bytes')
        
        # Launch EMR cluster using the pre-defined template
        cluster_id = emr.run_job_flow(
            Name=os.environ['CLUSTER_NAME'],
            LogUri=f"s3://{bucket_name}/emr-logs/",
            JobFlowRole=os.environ['EMR_JOB_FLOW_ROLE'],
            ServiceRole=os.environ['EMR_SERVICE_ROLE'],
            Instances={
                "Ec2SubnetId": os.environ['SUBNET_ID'],
            }
        )['JobFlowId']
        
        print(f"EMR Cluster launched with ID: {cluster_id}")
    else:
        print(f'Threshold not met: {total_size} bytes')
      `),
      handler: 'index.handler',
      environment: {
        BUCKET_NAME: s3ScriptBucket.bucketName,
        THRESHOLD_BYTES: (1 * 1024 * 1024).toString(), // 100 MB
        CLUSTER_NAME: emrClusterName,
        SUBNET_ID: subnetId,
        EMR_SERVICE_ROLE: emrServiceRole.roleName,
        EMR_JOB_FLOW_ROLE: emrJobFlowRole.roleName,
      },
    });

    stream_bucket?.grantRead(emrLaunchLambda);
    // emrServiceRole.grant(emrLaunchLambda, 'elasticmapreduce:RunJobFlow');
    
    const lambdaeventRule = new events.Rule(this, 'PeriodicTrigger', {
      // schedule: events.Schedule.rate(cdk.Duration.minutes(5)),
      schedule: events.Schedule.cron({ minute: '5' }), // Runs every 5 min.
    });

    lambdaeventRule.addTarget(new targets.LambdaFunction(emrLaunchLambda));

    // Create EMR cluster
    const emrCluster = new emr.CfnCluster(this, emrClusterName, {
        instances: {
            coreInstanceGroup: {
                instanceCount: CORE_INSTANCE_COUNT,
                instanceType: CORE_INSTANCE_TYPE,
                market: MARKET,
            },
            ec2SubnetId: subnetId,
            ec2KeyName: keyPair.keyName,
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
              args: [
                'spark-submit', 
                `s3://${scriptsLocation}/create_tables.py`,
                '--arg1', stream_bucket_name,
                '--arg2', s3ScriptBucket.bucketName,
                '--arg3', 'analysed_data',
              ],
            },
            name: 'create_tables',
          },
        ],

        name: CLUSTER_NAME,
        applications: [{ name: 'Spark' }],        
        serviceRole: emrServiceRole.roleName,
        // jobFlowRole: emrJobFlowRole.roleName,
        jobFlowRole: 'EMR_EC2_DefaultRole',
        configurations: [],
        logUri: `s3://${ERMLogBucket}/elasticmapreduce/`,
        releaseLabel: EMR_RELEASE,
        visibleToAllUsers: false,

      });




    // Outputs
    new cdk.CfnOutput(this, 'Kinesis-data-stream-name', {value: stream_name});
    new cdk.CfnOutput(this, 'Data_stream_bucket',       {value: dataStream.s3Bucket?.bucketName || ''});
    new cdk.CfnOutput(this, 'Glue_database_Name',       {value: glueDatabase.databaseName});
    new cdk.CfnOutput(this, 'AthenaResultsBucketName',  {value: athenaOutputBucket.bucketName,});










    






    //     // Add a bucket policy for QuickSight to access the S3 bucket
    //     const quickSightArn = `arn:aws:quicksight:${this.region}:${this.account}:namespace/my-custom-namespace`;
    //     const quickSightRoleArn = `arn:aws:iam::${this.account}:role/service-role/aws-quicksight-service-role-v0`;

    //     const quickSightServiceRole = new iam.Role(this, 'QuickSightServiceRole', {
    //       assumedBy: new iam.ServicePrincipal('quicksight.amazonaws.com'),
    //       managedPolicies: [
    //         iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSQuickSightAthenaAccess'),
    //       ],
    //     });


    //     athenaOutputBucket.addToResourcePolicy(
    //       new iam.PolicyStatement({
    //         effect: iam.Effect.ALLOW,
    //         principals: [new iam.ServicePrincipal('quicksight.amazonaws.com')],
    //         actions: ['s3:GetObject', 's3:ListBucket'],
    //         resources: [athenaOutputBucket.bucketArn, `${athenaOutputBucket.bucketArn}/*`],
    //       })
    //     );




    //     // Create QuickSight Athena Data Source
    //     const athenaDataSource = new quicksight.CfnDataSource(this, 'AthenaDataSource', {
    //       awsAccountId: this.account,
    //       dataSourceId: 'AthenaDataSource',
    //       name: 'AthenaDataSource',
    //       type: 'ATHENA',
    //       dataSourceParameters: {
    //         athenaParameters: {
    //           workGroup: athenaWorkgroup.name,
    //         },
    //       },
    //       permissions: [
    //         {
    //           principal: quickSightServiceRole.roleArn,
    //           actions: ['quicksight:DescribeDataSource', 'quicksight:UpdateDataSourcePermissions'],
    //         },
    //       ],
    //     });


    //     // Lambda function to automate QuickSight dataset creation (Python runtime)
    //     const quickSightDatasetLambda = new lambda_.Function(this, 'QuickSightDatasetLambda', {
    //       runtime: lambda_.Runtime.PYTHON_3_9,
    //       code: lambda_.Code.fromInline(`
    // import boto3
    // import os


    // def convert_glue_type_to_typescript(glue_type):
    //     type_mapping = {
    //         'string': 'STRING',
    //         'int': 'INTEGER',
    //         'bigint': 'INTEGER',
    //         'float': 'DECIMAL',
    //         'double': 'DECIMAL',
    //         'boolean': 'BOOLEAN',
    //         'date': 'DATETIME',
    //         'timestamp': 'DATETIME'
    //     }
    //     return type_mapping.get(glue_type, 'STRING')  # Default to STRING if type is unknown


    // def get_quicksight_input_columns(database_name, table_name):
    //     glue_client = boto3.client('glue')
    //     response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
    //     columns = response['Table']['StorageDescriptor']['Columns']
    //     return [
    //         {'Name': col['Name'], 'Type': convert_glue_type_to_typescript(col['Type'])}
    //         for col in columns
    //     ]

    // # Example usage
    // database_name = 'example-database'
    // table_name = 'example-table'
    // columns = get_quicksight_input_columns(os.environ['DATABASE_NAME'], os.environ['TABLE_NAME'])

    // print(columns)


    // def handler(event, context):
    //     quicksight = boto3.client('quicksight')

    //     params = {
    //         'AwsAccountId': os.environ['ACCOUNT_ID'],
    //         'DataSetId': 'AthenaDataSet',
    //         'Name': 'AthenaDataSet',
    //         'ImportMode': 'SPICE',
    //         'PhysicalTableMap': {
    //             'PhysicalTableId': {
    //                 'S3Source': {
    //                     'DataSourceArn': os.environ['DATASOURCE_ARN'],
    //                     'InputColumns': columns,
    //                     'UploadSettings': {
    //                         'Format': 'CSV',
    //                         'StartFromRow': 1,
    //                         'ContainsHeader': True
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     response = quicksight.create_data_set(**params)
    //     print('Dataset created:', response)

    //       `),
    //       handler: 'index.handler',
    //       environment: {
    //         ACCOUNT_ID: this.account,
    //         DATABASE_NAME: glueDatabase.databaseName,
    //         TABLE_NAME: stream_bucket_name,
    //         // AWS_REGION: this.region,
    //         DATASOURCE_ARN: `arn:aws:quicksight:${this.region}:${this.account}:datasource/AthenaDataSource`,
    //       },
    //     });

    //     quickSightDatasetLambda.addToRolePolicy(
    //       new iam.PolicyStatement({
    //         effect: iam.Effect.ALLOW,
    //         actions: ['quicksight:CreateDataSet', 'quicksight:UpdateDataSet', 'glue:GetTable'],
    //         resources: ['*'],
    //       })
    //     );




  }
}












