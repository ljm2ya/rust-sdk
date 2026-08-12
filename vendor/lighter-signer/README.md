# lighter-signer

Prebuilt Lighter transaction-signer shared library, taken verbatim from the
official Python SDK (which uses the exact same binary via `ctypes`):

- Source repo (Go): https://github.com/elliottech/lighter-go
- Binary origin: https://github.com/elliottech/lighter-python/tree/main/lighter/signers
- File: `lighter-signer-linux-amd64.so`
- sha256: `e18ff6bad3b49c4fc17c2a9bbf0fba8430f6be84bcc4abbe35a2e2f4641963f9`

Lighter transactions are signed with a Schnorr-style scheme over a custom
curve with a Poseidon hash (zk-circuit friendly), for which no independent
Rust implementation exists; the vendor's canonical signer is this Go
`c-shared` library. `rusk-sdk`'s `lighter` module loads it at runtime via
`libloading`.

Other platforms: download the matching binary from the `lighter-python`
repo above (darwin-arm64/amd64, linux-arm64, windows-amd64) and point
`LIGHTER_SIGNER_LIB` at it, or rebuild from `lighter-go` with its justfile
(`go build -buildmode=c-shared ./sharedlib`).

`lighter-signer.h` is the cgo-generated header documenting the exported
functions and ABI structs mirrored in `src/lighter.rs`.
