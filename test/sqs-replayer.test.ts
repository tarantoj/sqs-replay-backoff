import { join } from 'path';
import {
  aws_lambda,
  aws_sqs,
  Duration,
  Stack,
} from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, test } from 'vitest';
import { SqsReplayer } from '../src/sqs-replayer';

describe('SqsReplayer', () => {
  const fixtureCode = () =>
    aws_lambda.Code.fromAsset(join(__dirname, 'fixtures', 'bootstrap-dir'));

  test('creates a Rust lambda triggered by the replay queue', () => {
    const stack = new Stack();
    const sourceQueue = new aws_sqs.Queue(stack, 'SourceQueue');
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    new SqsReplayer(stack, 'Replayer', {
      sourceQueue,
      replayQueue,
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);

    template.hasResourceProperties('AWS::Lambda::Function', {
      Runtime: 'provided.al2023',
      Architectures: ['arm64'],
      Handler: 'bootstrap',
      MemorySize: 512,
      Timeout: 10,
      Environment: {
        Variables: {
          QUEUE_URL: {
            Ref: stack.getLogicalId(sourceQueue.node.defaultChild as aws_sqs.CfnQueue),
          },
          RUST_LOG: 'info',
        },
      },
    });

    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      EventSourceArn: {
        'Fn::GetAtt': [
          stack.getLogicalId(replayQueue.node.defaultChild as aws_sqs.CfnQueue),
          'Arn',
        ],
      },
    });
  });

  test('grants the lambda permission to send messages to the source queue', () => {
    const stack = new Stack();
    const sourceQueue = new aws_sqs.Queue(stack, 'SourceQueue');
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    new SqsReplayer(stack, 'Replayer', {
      sourceQueue,
      replayQueue,
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          {
            Action: Match.arrayWith(['sqs:SendMessage']),
            Effect: 'Allow',
            Resource: {
              'Fn::GetAtt': [
                stack.getLogicalId(sourceQueue.node.defaultChild as aws_sqs.CfnQueue),
                'Arn',
              ],
            },
          },
        ]),
      },
    });
  });

  test('sets replay tuning environment variables when provided', () => {
    const stack = new Stack();
    const sourceQueue = new aws_sqs.Queue(stack, 'SourceQueue');
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    new SqsReplayer(stack, 'Replayer', {
      sourceQueue,
      replayQueue,
      maxAttempts: 3,
      backoffRate: Duration.seconds(60),
      maximumDelay: Duration.minutes(5),
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::Lambda::Function', {
      Environment: {
        Variables: {
          QUEUE_URL: {
            Ref: stack.getLogicalId(sourceQueue.node.defaultChild as aws_sqs.CfnQueue),
          },
          RUST_LOG: 'info',
          MAX_ATTEMPTS: '3',
          BACKOFF_RATE: '60',
          MAXIMUM_DELAY: '300',
        },
      },
    });
  });
});