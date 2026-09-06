{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "sqs-replay-backoff";

  # https://devenv.sh/packages/
  packages = [ pkgs.git pkgs.zig pkgs.cargo-zigbuild pkgs.cargo-lambda pkgs.cargo-audit ];

  # https://devenv.sh/languages/
  languages.javascript.enable = true;
  languages.javascript.npm.enable = true;
  languages.javascript.npm.install.enable = true;
  languages.rust.enable = true;
  languages.rust.toolchainFile = ./lambda/rust-toolchain.toml;

  # https://devenv.sh/scripts/
  scripts.versioncheck.exec = ''
    node --version
    npm --version
    cargo --version
    rustc --version
  '';

  scripts.projen.exec = "npx projen $@";

  # https://devenv.sh/processes/
  # processes.dev.exec = "${lib.getExe pkgs.watchexec} -n -- ls -la";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/basics/
  enterShell = ''
    versioncheck
    echo "CDK construct library: run 'npx projen' to synthesize"
    echo "Rust lambda crate: cd lambda && cargo build"
  '';

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    node --version | grep -E "v[0-9]+"
    cargo --version | grep -E "cargo [0-9]+"
  '';

  # https://devenv.sh/git-hooks/
  # git-hooks.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
