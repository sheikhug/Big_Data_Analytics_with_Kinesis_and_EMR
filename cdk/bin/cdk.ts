#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { CdkStack } from '../lib/cdk-stack';


const app = new cdk.App();
const cdk_stack =  new CdkStack(app, 'BigDataAnalytics', {
  env: { account: '024848486969', region: 'us-east-1' },
});


