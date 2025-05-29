docker run --rm \
  -v "$PWD":/app \
  -v "$PWD/target":/home/zaino/target \
  -v "$PWD/docker_cargo/git":/home/zaino/.cargo/git \
  -v "$PWD/docker_cargo/registry":/home/zaino/.cargo/registry \
  zaino-ci:local

