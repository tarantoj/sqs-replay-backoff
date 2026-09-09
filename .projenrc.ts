import { awscdk, github, javascript } from 'projen';

const rustSetupSteps: github.workflows.JobStep[] = [
  {
    uses: 'dtolnay/rust-toolchain@stable',
    with: { targets: 'aarch64-unknown-linux-gnu' },
  },
  { uses: 'goto-bus-stop/setup-zig@v2' },
  { name: 'Install cargo-zigbuild', run: 'cargo install cargo-zigbuild' },
  { name: 'Install cargo-audit', run: 'cargo install cargo-audit' },
  {
    name: 'Audit Rust dependencies',
    // `cargo audit` takes `--file`, not `--manifest-path`. The SDK service
    // crates are declared with `default-features = false` (keeping only the
    // `default-https-client`/`rt-tokio` features), so the legacy rustls
    // 0.21/webpki 0.101 and h2 0.3 TLS stacks are never in the lockfile; the
    // HTTPS connector uses the rustls 0.23/aws-lc stack only.
    run: 'cargo audit --file lambda/Cargo.lock',
  },
];

const project = new awscdk.AwsCdkConstructLibrary({
  author: 'James Taranto',
  authorAddress: 'taranto.james@gmail.com',
  cdkVersion: '2.268.0',
  constructsVersion: '10.8.1',
  jsiiVersion: '6.0.12',
  typescriptVersion: '~5.9.3',
  defaultReleaseBranch: 'main',
  name: '@tarantoj/sqs-replay-backoff',
  description: 'AWS CDK construct that replays SQS messages back to a source queue with exponential backoff.',
  projenrcTs: true,
  releaseToNpm: true,
  prerelease: 'beta',
  npmDistTag: 'beta',
  npmTrustedPublishing: true,
  workflowNodeVersion: '24.x',
  repositoryUrl: 'https://github.com/tarantoj/sqs-replay-backoff.git',
  packageManager: javascript.NodePackageManager.NPM,
  jest: false,
  sampleCode: false,
  lambdaAutoDiscover: false,
  edgeLambdaAutoDiscover: false,
  singletonLambdaAutoDiscover: false,
  lambdaExtensionAutoDiscover: false,
  integrationTestAutoDiscover: false,
  docgen: false,
  prettier: true,
  prettierOptions: {
    settings: {
      printWidth: 150,
      singleQuote: true,
      trailingComma: javascript.TrailingComma.ALL,
    },
  },
  gitignore: ['.devenv*', 'devenv.local.nix', 'devenv.local.yaml', '.direnv', '.pre-commit-config.yaml', '/lambda/target/', '/assets/'],
  // Dependabot owns npm + cargo + github-actions updates (patch/minor only,
  // 7-day cooldown, auto-merged by mergify).
  // The nightly `upgrade-main` workflow is disabled (`depsUpgrade: false`) since
  // projen forbids it alongside dependabot; bump projen itself manually via
  // `npm i -D projen@latest && npx projen`. Dependabot ignores `projen` by default.
  depsUpgrade: false,
  dependabot: false,
  githubOptions: {
    mergifyOptions: {
      rules: [
        {
          name: 'Auto-merge dependabot patch/minor on green build',
          conditions: ['author=dependabot[bot]', '-label~=(do-not-merge)', 'status-success=build', 'status-success=package-js'],
          actions: { queue: { name: 'default' } },
        },
      ],
    },
  },
  // Rust toolchain for cross-compiling the bundled lambda in CI
  buildWorkflowOptions: { preBuildSteps: rustSetupSteps },
  releaseWorkflowSetupSteps: rustSetupSteps,
  npmIgnoreOptions: {
    ignorePatterns: [
      '/lambda/',
      '/AGENTS.md',
      '.devenv*',
      'devenv.local.nix',
      'devenv.local.yaml',
      '.direnv',
      '/coverage/',
      '/devenv.nix',
      '/devenv.yaml',
      '/devenv.lock',
      '/scripts/',
      '/vitest.config.mts',
      '/test-reports/',
      'junit.xml',
    ],
  },
  deps: [],
  devDeps: ['vitest'],
});

const bundleLambda = project.addTask('bundle:lambda', {
  steps: [{ exec: 'bash scripts/check-lambda.sh' }, { exec: 'bash scripts/bundle-lambda.sh' }],
});
const testTask = project.tasks.tryFind('test');
testTask?.prependExec('vitest run');
testTask?.prependSpawn(bundleLambda);

for (const pattern of ['/lib/', '/dist/', '/assets/', '/lambda/target/', 'coverage', '**/tsconfig.json']) {
  project.prettier?.addIgnorePattern(pattern);
}

const prettierFiles = ['src', 'test', 'projenrc', '.projenrc.ts'];
project.addTask('prettier:check', {
  steps: [{ exec: `prettier --check ${prettierFiles.join(' ')}` }],
});
project.addTask('prettier:write', {
  steps: [{ exec: `prettier --write ${prettierFiles.join(' ')}` }],
});
testTask?.prependSpawn(project.tasks.tryFind('prettier:check')!);

// npm updates via projen's Dependabot component (weekly, lockfile-only, 7-day
// cooldown), then patched raw: ignore semver-major so only patch/minor PRs are
// raised, and add the cargo ecosystem for the Rust lambda in /lambda plus the
// github-actions ecosystem for workflow pins. Raw mutation is needed because
// DependabotIgnore in this projen version has no `update-types` field, the
// component only synthesizes the npm entry, and the cargo/actions entries need
// the same cooldown in kebab-case.
const dependabot = project.github!.addDependabot({
  scheduleInterval: github.DependabotScheduleInterval.WEEKLY,
  cooldown: { defaultDays: 7 },
});
const majorIgnore = { 'dependency-name': '*', 'update-types': ['version-update:semver-major'] };
const cooldown = { 'default-days': 7 };
for (const update of dependabot.config.updates) {
  update.ignore = [majorIgnore];
}
dependabot.config.updates.push(
  {
    'package-ecosystem': 'cargo',
    directory: '/lambda',
    schedule: { interval: 'weekly' },
    cooldown,
    ignore: [majorIgnore],
  },
  {
    'package-ecosystem': 'github-actions',
    directory: '/',
    schedule: { interval: 'weekly' },
    cooldown,
    ignore: [majorIgnore],
  },
);

project.synth();
