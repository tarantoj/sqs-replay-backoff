# @tarantoj/sqs-replay-backoff

An [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/home.html) construct library that replays messages back to an SQS queue with exponential backoff. Messages that repeatedly fail processing are driven through a `Queue -> ReplayQueue -> DeadLetterQueue` chain and re-injected into the source queue with an increasing delay, up to a configurable attempt limit, after which they are finally dead-lettered.

The replayer is a [Rust](https://www.rust-lang.org/) Lambda (custom runtime) that is cross-compiled and shipped prebuilt inside this package, so consumers do not need a Rust toolchain or Docker.

## Constructs

### `SqsReplayer`

Wraps the Rust replayer Lambda. It is triggered by a `replayQueue` (typically the DLQ of the queue you want to protect), re-sends each message back to a `sourceQueue` with a delay computed as `min(maximumDelay, backoffRate * 2^attempt)`, and reports a batch item failure once `maxAttempts` is exceeded.

```ts
import { aws_sqs, Duration } from "aws-cdk-lib";
import { SqsReplayer } from "@tarantoj/sqs-replay-backoff";

const sourceQueue = new aws_sqs.Queue(this, "SourceQueue");
const replayQueue = new aws_sqs.Queue(this, "ReplayQueue");

new SqsReplayer(this, "Replayer", {
  sourceQueue,
  replayQueue,
  maxAttempts: 5,          // default 5
  backoffRate: Duration.seconds(30),  // default 30 seconds
  maximumDelay: Duration.minutes(15), // default 15 minutes
});
```

### `SqsQueueWithReplay`

Convenience construct that creates the full queue topology (`Queue -> ReplayQueue -> DeadLetterQueue` with redrive policies) and wires up the `SqsReplayer`.

```ts
import { Duration } from "aws-cdk-lib";
import { SqsQueueWithReplay } from "@tarantoj/sqs-replay-backoff";

const { queue, replayQueue, deadLetterQueue, replayer } =
  new SqsQueueWithReplay(this, "QueueWithReplay", {
    fifo: false,                       // FIFO support
    visibilityTimeout: Duration.seconds(18), // default 18 seconds
    maxReceiveCount: 5,                // default 5
    maxAttempts: 5,
  });
```

### How it works

1. Messages failing to be consumed are moved to the `ReplayQueue` by the source queue's redrive policy.
2. The replayer Lambda (triggered by `ReplayQueue` with `batchSize: 1` and `reportBatchItemFailures`) re-sends each message to the `sourceQueue` with an exponential delay tracked by the `sqs-dlq-replay-num` message attribute.
3. Once a message has been replayed `maxAttempts` times it is reported as a failed batch item, so it is redriven from `ReplayQueue` into the final `DeadLetterQueue`.

## Development

The development environment is managed by [devenv](https://devenv.sh/).

```sh
devenv shell           # enter the dev shell (node, npm, cargo, cargo-zigbuild, ...)
npx projen             # synthesize all projen-managed files
npm run build          # bundles the Rust lambda, compiles the jsii library, runs tests
cd lambda && cargo test
```

See [AGENTS.md](./AGENTS.md) for repository conventions.

## License

Apache-2.0