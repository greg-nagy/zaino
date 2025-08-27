# Testing
### Dependencies
1) [Zebrad](https://github.com/ZcashFoundation/zebra.git)
2) [Lightwalletd](https://github.com/zcash/lightwalletd.git)
3) [Zcashd, Zcash-Cli](https://github.com/zcash/zcash)

### Tests
1) Symlink or copy compiled `zebrad`, `zcashd` and `zcash-cli` binaries to `zaino/test_binaries/bins/*`

**Client Rpc Tests** _For the client rpc tests to pass a Zaino release binary must be built.
WARNING: these tests do not use the binary built by cargo nextest._
Note: Recently the newest GCC version on Arch has broken a build script in the `rocksdb` dependency. A workaround is:
`export CXXFLAGS="$CXXFLAGS -include cstdint"`

2) Build release binary `cargo build --release`
3) Run `cargo nextest run`

## Cargo Make
`cargo make help`
will print a help output.
`Makefile.toml` holds a configuration file.
