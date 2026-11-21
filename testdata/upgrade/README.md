# Pre-extraction Docker identities

`original.json` records outputs from Pulse commit
`6c20e087383ac7c333f562aa373c873bd5423db6`, before adapting its code here.
All leases, endpoints and image identities in these fixtures are synthetic.

I generated these expectations in an isolated module containing verbatim copies
of the original native structs, Docker engine, executor Process function and
adapter translation loop. The small source-only harness omits unrelated controller
code; its matching function is copied from Pulse's pinned c2j module. No candidate
implementation generates the expected bytes during tests.

The three inputs cover omitted and present optional fields, concrete and wildcard
bases, legacy canonical OCI, non-rounded requested quantities, metadata/default
map encoding, lease whitespace, accounting, cache identity/labels and old cursors.
Tests compare complete JSON bytes and lowercase SHA-256 values, then recognize an
original worker without creating or starting another one. Existing engine tests
also retain layout-1/layout-2 and older limits/cache attachment compatibility.

To deliberately refresh from a clean checkout of the exact original revision:

```sh
python3 testdata/upgrade/generate.py /path/to/pinned/pulse
```

This requires Go 1.26 and network/module-cache access. Review any fixture change
against the original source; never refresh to make changed serialization pass.
The copied code is covered by the repository's Apache-2.0 LICENSE and NOTICE.
