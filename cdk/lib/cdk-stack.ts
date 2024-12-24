import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Stack, StackProps } from 'aws-cdk-lib';
import { KinesisStreamsToKinesisFirehoseToS3 } from '@aws-solutions-constructs/aws-kinesisstreams-kinesisfirehose-s3';
import * as glue from '@aws-cdk/aws-glue-alpha';
import { aws_glue as glue2 } from 'aws-cdk-lib';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_athena as athena } from 'aws-cdk-lib';
// import * as athena from 'aws-cdk-lib/aws-athena';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { aws_kinesisfirehose as firehose } from 'aws-cdk-lib';
import * as lambda_ from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as quicksight from 'aws-cdk-lib/aws-quicksight';



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
    // const stream_bucket = dataStream.s3Bucket
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
          // connectionName: 'connectionName',
          // dlqEventQueueArn: 'dlqEventQueueArn',
          // eventQueueArn: 'eventQueueArn',
          // exclusions: ['exclusions'],
          path: dataStream.s3Bucket?.bucketName,
          // sampleSize: 123,
        }],
      },

      // the properties below are optional
      // classifiers: ['classifiers'],
      // configuration: 'configuration',
      // crawlerSecurityConfiguration: 'crawlerSecurityConfiguration',
      databaseName: glueDatabase.databaseName,
      // description: 'description',
      // lakeFormationConfiguration: {
      //   accountId: 'accountId',
      //   useLakeFormationCredentials: false,
      // },
      // name: 'name',
      // recrawlPolicy: {
      //   recrawlBehavior: 'recrawlBehavior',
      // },
      schedule: {
        scheduleExpression: 'cron(0	*	*	*	?	*)',
      },
      // schemaChangePolicy: {
      //   deleteBehavior: 'deleteBehavior',
      //   updateBehavior: 'updateBehavior',
      // },
      // tablePrefix: 'tablePrefix',
      // tags: tags,
    });




    const athenaOutputBucket = new s3.Bucket(this, 'Bucket', {
      bucketName: 'athenaoutputbucket333444566700432',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      // blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      // encryption: s3.BucketEncryption.S3_MANAGED,
      // enforceSSL: true,
      // versioned: true,
    });


    
  // // Add Athena CDK 
  //  const athenaWorkGroup = new athena.CfnWorkGroup(this, 'AthenaWorkGroup', {
  //     name: 'AthenaWorkGroup',
  //     workGroupConfiguration: {
  //       resultConfiguration: {
  //         outputLocation: 's3://' + athenaOutputBucket.bucketName + '/athena_output',
  //       },
  //     },
  //   });

    const athenaWorkgroup = new athena.CfnWorkGroup(this, 'AthenaWorkGroup', {
      name: 'QuickSightWorkGroup',
      state: 'ENABLED',
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: athenaOutputBucket.s3UrlForObject(),
        },
      },
    });


    // const query1 = new athena.CfnNamedQuery(this, 'QueryNo1', {
    //   database: glueDatabase.databaseName,
    //   queryString: 'select * from bigdataanalytics_streamfirehoses3kinesisfirehoseto_nthc1zgmfkdq \
    //                 where vendorid=2; ',
    //   description: 'Query1 in CDK',
    //   name: 'QueryNo1',
    //   workGroup: athenaWorkGroup.name,
    // });
   

    // query1.addDependency(athenaWorkGroup);



    // Step 2: Create an IAM Role for the Lambda Function
    const lambdaRole = new iam.Role(this, 'AthenaLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'));
    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonAthenaFullAccess'));
    lambdaRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'));


    // Step 3: Create a Lambda Function to Run Athena Queries
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

    // Step 4: Create an EventBridge Rule to Schedule the Lambda Function
    const scheduleRule = new events.Rule(this, 'AthenaScheduleRule', {
      // schedule: events.Schedule.cron({ minute: '0', hour: '0' }), // Runs daily at midnight
      schedule: events.Schedule.rate(cdk.Duration.minutes(5)), 
    });

    // Add the Lambda function as the target for the EventBridge Rule
    scheduleRule.addTarget(new targets.LambdaFunction(queryLambda));




//     // Step 2: Add a bucket policy for QuickSight to access the S3 bucket
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




//     // Step : Create QuickSight Athena Data Source
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


//     // Step 4: Lambda function to automate QuickSight dataset creation (Python runtime)
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




















    // Outputs
    new cdk.CfnOutput(this, 'Kinesis-data-stream-name', {
      value: stream_name
    });

    new cdk.CfnOutput(this, 'Data_stream_bucket', {
      value: dataStream.s3Bucket?.bucketName || ''
    });


    new cdk.CfnOutput(this, 'Glue_database_Name', {
      value: glueDatabase.databaseName
    });


    new cdk.CfnOutput(this, 'AthenaResultsBucketName', {
      value: athenaOutputBucket.bucketName,
    });





  }
}
