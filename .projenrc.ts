import { awscdk, github, javascript } from 'projen';

const rustSetupSteps: github.workflows.JobStep[] = [
  {
    uses: 'dtolnay/rust-toolchain@stable',
    with: { targets: 'aarch64-unknown-linux-gnu' },
  },
  { uses: 'goto-bus-stop/setup-zig@v2' },
  { name: 'Install cargo-zigbuild', run: 'cargo install cargo-zigbuild' },
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
  projenrcTs: true,
  releaseToNpm: true,
  prerelease: 'beta',
  npmDistTag: 'beta',
  npmTrustedPublishing: true,
  workflowNodeVersion: '22.x',
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
  // Rust toolchain for cross-compiling the bundled lambda in CI
  buildWorkflowOptions: { preBuildSteps: rustSetupSteps },
  releaseWorkflowSetupSteps: rustSetupSteps,
  npmIgnoreOptions: {
    ignorePatterns: [
      '/lambda/',
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
  steps: [{ exec: 'bash scripts/bundle-lambda.sh' }],
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

project.synth();
