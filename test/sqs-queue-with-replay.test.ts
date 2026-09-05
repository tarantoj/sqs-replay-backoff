import { join } from 'path';
import { App, aws_lambda, Duration, Stack } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { describe, expect, test } from 'vitest';
import { SqsQueueWithReplay } from '../src/sqs-queue-with-replay';

const env = { account: '123456789012', region: 'us-east-1' };
const fixtureCode = () =>
  aws_lambda.Code.fromAsset(join(__dirname, 'fixtures', 'bootstrap-dir'));

describe('SqsQueueWithReplay', () => {
  test('creates the queue, replay queue, and DLQ redrive chain', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });

    new SqsQueueWithReplay(stack, 'QueueWithReplay', { code: fixtureCode() });

    const template = Template.fromStack(stack);

    template.resourceCountIs('AWS::SQS::Queue', 3);
    template.resourceCountIs('AWS::Lambda::Function', 1);
    template.resourceCountIs('AWS::Lambda::EventSourceMapping', 1);
    template.resourceCountIs('AWS::SSM::Parameter', 1);

    template.hasResourceProperties('AWS::SQS::Queue', {
      RedrivePolicy: {
        deadLetterTargetArn: {
          'Fn::GetAtt': [Match.anyValue(), 'Arn'],
        },
        maxReceiveCount: 5,
      },
    });
  });

  test('honours fifo, visibility timeout, and max receive count', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });

    new SqsQueueWithReplay(stack, 'QueueWithReplay', {
      fifo: true,
      visibilityTimeout: Duration.seconds(60),
      maxReceiveCount: 2,
      code: fixtureCode(),
    });

    const template = Template.fromStack(stack);

    const queues = Object.values(template.findResources('AWS::SQS::Queue'));
    for (const queue of queues) {
      expect(queue.Properties.FifoQueue).toBe(true);
    }

    const queuesWithRedrive = queues.filter(
      (queue) => queue.Properties.RedrivePolicy,
    );
    expect(queuesWithRedrive).toHaveLength(2);
    for (const queue of queuesWithRedrive) {
      expect(queue.Properties.VisibilityTimeout).toBe(60);
      expect(queue.Properties.RedrivePolicy.maxReceiveCount).toBe(2);
    }
  });

  test('exposes the queue, replay queue, DLQ, and replayer', () => {
    const app = new App();
    const stack = new Stack(app, 'TestStack', { env });

    const queueWithReplay = new SqsQueueWithReplay(stack, 'QueueWithReplay', {
      code: fixtureCode(),
    });

    expect(queueWithReplay.queue.node.id).toBe('Queue');
    expect(queueWithReplay.replayQueue.node.id).toBe('ReplayQueue');
    expect(queueWithReplay.deadLetterQueue.node.id).toBe('DeadLetterQueue');
    expect(queueWithReplay.replayer).toBeDefined();
  });
});