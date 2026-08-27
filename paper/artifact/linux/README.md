# x86-64 Linux replication

The devcontainer is the no-cost compatibility route. Open the repository in a
four-core GitHub Codespace, verify that `uname -m` prints `x86_64`, and run:

```sh
bash paper/artifact/linux/run-linux-suite.sh
```

The script refuses to label a non-x86 host as the Linux replication. It saves
CPU, kernel, virtualization, memory, runtime, commit, working-tree, and Linux
steal-time records next to the raw benchmark files.

The fixed-memory study uses the same image and disables swap by setting Docker
memory and memory-swap to the same value:

```sh
bash paper/artifact/linux/run-cgroup-study.sh
```

Docker keeps each cell long enough for the host script to record its exit code
and OOM status. The container records cgroup v2 memory limits, peak usage, and
memory events when it exits normally. An OOM-killed cell remains in the host
inspection JSON.

The image pins Bun 1.4.0 by multi-architecture OCI digest and pandas 3.0.5. Run
three fresh non-burstable x86 cloud VMs for the submitted timing evidence. A
single Codespace establishes x86 portability but is not independent hardware
replication.
