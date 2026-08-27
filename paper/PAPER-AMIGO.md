# Paper Amigo project

The current manuscript PDF is attached to Paper Amigo project
`ab09cc57-095b-4342-a8a8-48e6da9c60b8`.

- Local PDF: `paper/build/main.pdf`
- Uploaded SHA-256: `13efaafe966e8a830d11fae2bbc8e32504d4b6e266b4dea4e6ad24ad707f7bac`
- Uploaded file: `main.pdf`, 165,395 bytes
- Public file URL: `https://yzy16yxaqa.ufs.sh/f/QMVxAv8DZmlK12Jxo8dU2VP5jYOJXfGdlrKugwZeWCSIv0zp`

After every successful PDF build, run:

```sh
bun run paper:amigo:sync
```

The command lists the project and compares the local and remote SHA-256 values.
It stops without uploading when they match. When they differ, it replaces the
existing file, verifies the new remote hash, and updates
`paper/paper-amigo-project.json`. It never creates another project.

Use `bun run paper:amigo:check` when you only want a read-only status check.
