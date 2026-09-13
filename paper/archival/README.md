# v0.4.1 archival status

Checked 13 September 2026.

Software Heritage completed a full visit of the public repository. The archived
`refs/tags/v0.4.1` release resolves to the same annotated tag and commit as the
local release:

- Release: `swh:1:rel:a2e89f64f97deb08f9d8702ede3829acc719e92f`
- Commit: `swh:1:rev:d35161cd74e6d8f6f54fd8d564470794184b56a7`
- Snapshot: `swh:1:snp:c62a9c8b80850d0231d12dcfd7c2027472eba60d`

The [release archive](https://archive.softwareheritage.org/swh:1:rel:a2e89f64f97deb08f9d8702ede3829acc719e92f)
and [completed save request](https://archive.softwareheritage.org/api/1/origin/save/2473580/)
are public. The verified API fields are recorded in
`software-heritage-v0.4.1.json`.

## Archive policy

Software Heritage is the permanent archive for this release. No Zenodo deposit
is planned, and no archive DOI is claimed. Cite the verified release SWHID above.

An exact-tag source ZIP is also available locally at
`out/archive-v0.4.1/bun_panda-0.4.1.zip`. Recreate it with:

```sh
mkdir -p out/archive-v0.4.1
git archive --format=zip --prefix=bun_panda-0.4.1/ \
  --output=out/archive-v0.4.1/bun_panda-0.4.1.zip v0.4.1
shasum -a 256 out/archive-v0.4.1/bun_panda-0.4.1.zip
```

Expected SHA-256:
`8ddd793418a8cbec9eb79e73474c4b5cb74241e0ee02289cba9696500cbd40b2`.
The ZIP contains the frozen release, excluding later tests, fixes, and paper revisions.

## Independent reproduction pending

The [tester handoff](../INDEPENDENT-REPRODUCTION-HANDOFF.md) fixes the public
release and expected report. An author-run or agent-run check does not satisfy
independent reproduction. No tester has been contacted by this work.
