import { join } from 'path';
import { App, aws_lambda, aws_sqs, Duration, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, test } from 'vitest';
import { SqsReplayer } from '../src/sqs-replayer';

const env = { account: '123456789012', region: 'us-east-1' };
const fixtureCode = () => aws_lambda.Code.fromAsset(join(__dirname, 'fixtures', 'bootstrap-dir'));

describe('SqsReplayer', () => {
  test('shares a single lambda across replayers', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
    const source1 = new aws_sqs.Queue(stack, 'Source1');
    const replay1 = new aws_sqs.Queue(stack, 'Replay1');
    const source2 = new aws_sqs.Queue(stack, 'Source2');
    const replay2 = new aws_sqs.Queue(stack, 'Replay2');

    new SqsReplayer(stack, 'Replayer1', {
      sourceQueue: source1,
      replayQueue: replay1,
      code: fixtureCode(),
    });
    new SqsReplayer(stack, 'Replayer2', {
      sourceQueue: source2,
      replayQueue: replay2,
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);
    template.resourceCountIs('AWS::Lambda::Function', 1);
    template.resourceCountIs('AWS::Lambda::EventSourceMapping', 2);
    template.resourceCountIs('AWS::SSM::Parameter', 2);
  });

  test('shares a single lambda across stacks in an app', () => {
    const app = new App();
    const stack1 = new Stack(app, 'Stack1', { env });
    const stack2 = new Stack(app, 'Stack2', { env });

    const source1 = new aws_sqs.Queue(stack1, 'Source1');
    const replay1 = new aws_sqs.Queue(stack1, 'Replay1');
    const source2 = new aws_sqs.Queue(stack2, 'Source2');
    const replay2 = new aws_sqs.Queue(stack2, 'Replay2');

    new SqsReplayer(stack1, 'Replayer1', {
      sourceQueue: source1,
      replayQueue: replay1,
      code: fixtureCode(),
    });
    new SqsReplayer(stack2, 'Replayer2', {
      sourceQueue: source2,
      replayQueue: replay2,
      code: fixtureCode(),
    });

    const template1 = Template.fromStack(stack1);
    const template2 = Template.fromStack(stack2);

    expect(Object.keys(template1.findResources('AWS::Lambda::Function'))).toHaveLength(1);
    expect(Object.keys(template2.findResources('AWS::Lambda::Function'))).toHaveLength(0);
    template1.resourceCountIs('AWS::Lambda::EventSourceMapping', 2);
    template2.resourceCountIs('AWS::Lambda::EventSourceMapping', 0);
    template1.resourceCountIs('AWS::SSM::Parameter', 1);
    template2.resourceCountIs('AWS::SSM::Parameter', 1);
  });

  test('creates a Rust lambda configured for the SSM config store', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
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
      MemorySize: 256,
      Timeout: 10,
      TracingConfig: { Mode: 'Active' },
      Environment: {
        Variables: {
          SSM_PARAMETER_PATH: '/sqs-replay/queues/',
          RUST_LOG: 'info',
        },
      },
    });
  });

  test('registers the replay queue config in SSM', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
    const sourceQueue = new aws_sqs.Queue(stack, 'SourceQueue');
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    new SqsReplayer(stack, 'Replayer', {
      sourceQueue,
      replayQueue,
      maxAttempts: 3,
      backoffRate: Duration.seconds(60),
      maximumDelay: Duration.minutes(5),
      useJitter: true,
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);
    template.hasResourceProperties('AWS::SSM::Parameter', {
      Type: 'String',
      Name: Match.stringLikeRegexp('^/sqs-replay/queues/'),
    });

    const parameters = Object.values(template.findResources('AWS::SSM::Parameter'));
    expect(parameters).toHaveLength(1);
    const value = joinedString(parameters[0].Properties.Value);
    expect(value).toContain('replayQueueArn');
    expect(value).toContain('destinationQueueUrl');
    expect(value).toContain('maxAttempts');
    expect(value).toContain('backoffRate');
    expect(value).toContain('maximumDelay');
    expect(value).toContain('useJitter');
  });

  test('rejects fifo source queues', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
    const fifoSource = new aws_sqs.Queue(stack, 'FifoSource', { fifo: true });
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    expect(
      () =>
        new SqsReplayer(stack, 'Replayer', {
          sourceQueue: fifoSource,
          replayQueue,
          code: fixtureCode(),
        }),
    ).toThrow(/does not support FIFO queues/);
  });

  test('rejects out-of-range tuning options', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
    const sourceQueue = new aws_sqs.Queue(stack, 'SourceQueue');
    const replayQueue = new aws_sqs.Queue(stack, 'ReplayQueue');

    expect(
      () =>
        new SqsReplayer(stack, 'ReplayerMaxDelay', {
          sourceQueue,
          replayQueue,
          maximumDelay: Duration.minutes(30),
          code: fixtureCode(),
        }),
    ).toThrow(/maximumDelay must be between 1 second and 15 minutes/);
    expect(
      () =>
        new SqsReplayer(stack, 'ReplayerMaxAttempts', {
          sourceQueue,
          replayQueue,
          maxAttempts: 0,
          code: fixtureCode(),
        }),
    ).toThrow(/maxAttempts must be at least 1/);
    expect(
      () =>
        new SqsReplayer(stack, 'ReplayerBatchSize', {
          sourceQueue,
          replayQueue,
          batchSize: 0,
          code: fixtureCode(),
        }),
    ).toThrow(/batchSize must be between 1 and 10000/);
  });

  test('grants the lambda ssm read and send to the source queue', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });
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
            Action: 'ssm:GetParametersByPath',
            Effect: 'Allow',
            Resource: {
              'Fn::Join': ['', Match.arrayWith(['arn:', { Ref: 'AWS::Partition' }, ':ssm:us-east-1:123456789012:parameter/sqs-replay/queues/*'])],
            },
          },
          {
            Action: Match.arrayWith(['sqs:SendMessage']),
            Effect: 'Allow',
            Resource: {
              'Fn::GetAtt': [stack.getLogicalId(sourceQueue.node.defaultChild as aws_sqs.CfnQueue), 'Arn'],
            },
          },
        ]),
      },
    });
  });
});

/** Joins the string fragments of a synthesized `Fn::Join` value. */
const joinedString = (value: unknown): string => {
  if (typeof value === 'string') {
    return value;
  }
  const parts = (value as { 'Fn::Join'?: [string, unknown[]] })?.['Fn::Join'];
  if (Array.isArray(parts)) {
    return parts[1].map((part) => (typeof part === 'string' ? part : '')).join('');
  }
  return '';
};
