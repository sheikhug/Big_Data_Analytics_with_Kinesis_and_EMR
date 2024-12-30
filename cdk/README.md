# Big Data Analytics Pipeline with AWS CDK

![alt text](Big-Data-Analytics-Architecture.drawio.png)

This project implements a comprehensive big data analytics pipeline using AWS services and the AWS Cloud Development Kit (CDK).

The pipeline ingests data through Amazon Kinesis, stores it in Amazon S3, processes it using AWS Glue and Amazon EMR, and enables analysis through Amazon Athena and AWS QuickSight. The infrastructure is defined as code using AWS CDK, allowing for easy deployment and management.

## Repository Structure

```
.
├── bin
│   └── cdk.ts                 # Entry point for CDK application
├── lib
│   ├── cdk-stack.ts           # Main CDK stack definition
│   └── emr-scripts
│       └── emr-script.py      # EMR processing script
├── test
│   └── cdk.test.ts            # Test file for CDK stack
├── cdk.context.json           # CDK context file
├── cdk.json                   # CDK configuration file
├── jest.config.js             # Jest configuration for testing
├── package.json               # Node.js package configuration
├── tsconfig.json              # TypeScript configuration
└── README.md                  # This file
```

## Usage Instructions

### Prerequisites

- Node.js (v14.x or later)
- AWS CLI configured with appropriate credentials
- AWS CDK CLI (v2.x)

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd <repository-name>
   ```

2. Install dependencies:
   ```
   npm install
   ```

3. Bootstrap your AWS environment (if not already done):
   ```
   cdk bootstrap
   ```

### Deployment

To deploy the stack:

```
cdk deploy
```

This command will synthesize the CloudFormation template and deploy the stack to your default AWS account and region.

### Configuration

The main configuration for the stack is in `lib/cdk-stack.ts`. Key components include:

- Kinesis Data Stream and Firehose for data ingestion
- S3 buckets for raw and processed data storage
- Glue database and crawler for data cataloging
- EMR cluster for data processing
- Athena workgroup for SQL queries
- Lambda functions for automation and scheduling

Modify the `lib/cdk-stack.ts` file to adjust the configuration as needed.

### EMR Processing

The EMR cluster uses a Python script (`lib/emr-scripts/emr-script.py`) for data processing. This script:

1. Reads raw data from an S3 bucket
2. Performs basic analysis (average trip duration, average passenger count, trips by vendor)
3. Saves results back to S3

To modify the EMR processing logic, edit the `emr-script.py` file.

### Athena Queries

A Lambda function is set up to run Athena queries on a schedule. The query is defined in the Lambda function code within `lib/cdk-stack.ts`. Modify this code to change the query or its schedule.

## Data Flow

1. Data is ingested through a Kinesis Data Stream.
2. Kinesis Firehose delivers the data to an S3 bucket.
3. A Glue crawler catalogs the data in the S3 bucket.
4. EMR cluster processes the data using the provided Python script.
5. Processed data is stored back in S3.
6. Athena queries can be run against the processed data.
7. (Optional) QuickSight can be used for visualization.

```
[Kinesis Stream] -> [Kinesis Firehose] -> [S3 Raw Data]
                                              |
                                          [Glue Crawler]
                                              |
                                          [Glue Catalog]
                                              |
                                          [EMR Cluster] -> [S3 Processed Data]
                                                                  |
                                                              [Athena] -> [QuickSight]
```

## Infrastructure

The infrastructure is defined using AWS CDK in TypeScript. Key resources include:

- **Kinesis**: `KinesisStreamsToKinesisFirehoseToS3` construct for data ingestion
- **S3**: Multiple buckets for raw data, processed data, Athena query results, and EMR scripts
- **Glue**: Database, crawler, and associated IAM roles
- **EMR**: Cluster definition with instance types and configurations
- **Athena**: Workgroup and associated S3 bucket for query results
- **Lambda**: Functions for running Athena queries and launching EMR clusters
- **IAM**: Various roles and policies for different components
- **EventBridge**: Rule for scheduling Athena queries

## Troubleshooting

- **Deployment Failures**: Check the CloudFormation console for detailed error messages. Ensure you have the necessary permissions to create all resources.
- **EMR Job Failures**: Check the EMR console and CloudWatch logs for the specific step that failed. Verify S3 permissions and the EMR script logic.
- **Athena Query Issues**: Ensure the Glue crawler has run successfully and the data is properly cataloged. Check the Athena query logs in CloudWatch.

## Performance Optimization

- Monitor EMR cluster utilization and adjust instance types or counts as needed.
- Use appropriate Kinesis shard counts based on your data ingestion rate.
- Optimize Athena queries by partitioning data and using appropriate file formats (e.g., Parquet).
- Use QuickSight caching for frequently accessed dashboards.

For more detailed performance tuning, use AWS CloudWatch metrics and logs to identify bottlenecks in your data pipeline.