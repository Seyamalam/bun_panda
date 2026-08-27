# x86-64 Linux replication without owned hardware

Checked against provider documentation on 26 August 2026.

## Recommendation

A cloud VM is acceptable for the second architecture in this study. Describe it precisely as an "x86-64 Linux cloud-VM replication," not as a bare-metal replication. The guest operating system, runtime, dependency versions, CPU count, memory limit, workload order, and benchmark protocol can all be fixed. The physical host, frequency control, and competing tenants usually cannot.

Use two stages:

1. Run the complete artifact in a 4-core GitHub Codespace if `uname -m` reports `x86_64`. This is the cheapest way to find portability failures and obtain a preliminary x86 result.
2. For the result reported as performance evidence, rent three fresh, non-burstable, non-Spot instances of one fixed x86 type. `c7i.xlarge` on EC2 is a reasonable default. Run the full study once per instance, then report per-instance results and an interval that treats VM instance as the highest resampling level. Keep region, availability zone, image, kernel, instance type, and benchmark commit fixed.

The second stage need only run for a few benchmark windows. There is no methodological reason to keep the VM running afterward. Check the provider's live calculator before provisioning because rates vary by region and can change.

## Available routes

| Route | What the provider promises | Suitable use | Main problem |
|---|---|---|---|
| GitHub Codespaces | Every codespace runs on a separate VM. Personal GitHub Free accounts currently include 120 core-hours and 15 GB-month of storage; Pro includes 180 core-hours and 20 GB-month. Compute consumption equals active wall time multiplied by the selected core count. [GitHub billing documentation](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces) | First x86 Linux replication and artifact debugging | GitHub's machine API publishes OS, vCPU count, memory, and storage, but not host CPU architecture or model. Verify `x86_64` at runtime and record the CPU model. Do not imply that GitHub guarantees that model. [Codespaces machine API](https://docs.github.com/en/rest/codespaces/machines) |
| AWS EC2 C7i | C7i uses Intel x86-64 Sapphire Rapids processors and the Nitro hypervisor. C7i supports ordinary VMs, bare-metal sizes, and Dedicated Hosts. [EC2 compute-optimized specifications](https://docs.aws.amazon.com/ec2/latest/instancetypes/co.html) | Recommended short paid performance run | Default tenancy shares physical hardware with other accounts. Dedicated Instances isolate accounts at host level, but provide no host affinity and may move after stop/start. [EC2 tenancy documentation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/dedicated-instance.html) |
| Google Compute Engine E2 free tier | The free tier currently includes one non-preemptible `e2-micro` in specified US regions. [Google Cloud Free Program](https://docs.cloud.google.com/free/docs/free-cloud-features) | Installation and semantic smoke test | `e2-micro` time-shares a physical core and bursts. E2 can use Intel Broadwell or AMD Milan, and Google does not permit selecting a minimum CPU platform for E2. It is a poor timing platform. [E2 shared-core behavior](https://docs.cloud.google.com/compute/docs/general-purpose-machines), [CPU selection limits](https://docs.cloud.google.com/compute/docs/instances/specify-min-cpu-platform) |
| Oracle Cloud Always Free E2.1.Micro | Up to two AMD `VM.Standard.E2.1.Micro` instances are currently Always Free. Each has 1 GB RAM and one eighth of an OCPU with access to extra CPU resources. Oracle may reclaim idle instances, and capacity can be unavailable. [OCI Always Free documentation](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm) | x86 compatibility check | The fractional, burst-capable CPU and 1 GB memory make it unsuitable for stable timing or capacity comparison. |
| Azure free burstable VM | Azure's free-account page currently lists 750 hours of several burstable VM types for the first 12 months. The exact eligible x86 type depends on the offer and account. [Azure free account](https://azure.microsoft.com/en-us/pricing/purchase-options/azure-account) | Compatibility check if an account already exists | B-series performance depends on banked CPU credits and falls back to a baseline after credits are exhausted. [Azure CPU-credit model](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/b-series-cpu-credit-model) |
| Azure Fsv2 | Fsv2 is non-burstable compute-optimized x86-64. `Standard_F2s_v2` starts at two vCPUs and 4 GiB. [Fsv2 specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/compute-optimized/fsv2-series) | Paid alternative to EC2 C7i | A region can supply any of several listed Intel generations, so record the actual CPU and do not pool unlike models as one machine. |

AWS's current program gives new accounts credits that may cover eligible EC2 use, but eligibility depends on account age and plan. Treat credits as a possible payment method, not as part of the experimental design. [AWS Free Tier announcement](https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/)

## Minimum protocol for a defensible VM run

Before every run, save:

```sh
date --iso-8601=seconds
uname -a
uname -m
lscpu --json
cat /etc/os-release
systemd-detect-virt
bun --version
git rev-parse HEAD
cat /proc/meminfo
cat /proc/stat
```

Then apply these controls:

- Reject the run unless `uname -m` is `x86_64`.
- Use one immutable VM image or a fully pinned provisioning script. Store the image identifier and package lockfile.
- Use a fixed non-burstable instance type. Do not use Spot, preemptible, shared-core, or credit-based types for the reported timings.
- Stop unrelated services. Pin the benchmark process to the same vCPU set with `taskset`. Keep benchmark parallelism constant.
- Run the existing randomized fresh-process design. Do not replace it with one long warm process.
- Measure three independently created VM instances. The bootstrap hierarchy should be VM, process, then iteration. Publish each VM's median as well as the pooled interval. A single Codespace is a portability result, not independent replication.
- Capture `/proc/stat` before and after every cell. Linux defines its `steal` field as involuntary wait in a virtualized environment. Flag cells with nonzero or unusually high steal time and show a sensitivity analysis with those cells excluded. [Linux `/proc/stat` documentation](https://www.kernel.org/doc/html/latest/filesystems/proc.html#miscellaneous-kernel-statistics-in-proc-stat)
- Run the cgroup-v2 memory study on the same image. Report the configured `memory.max`, peak `memory.current`, out-of-memory events, input size, and completion status. Do not compare macOS RSS directly with the cgroup result.
- Save raw output, environment capture, provisioning file, and checksums in the artifact. Record failed and discarded runs with reasons rather than deleting them silently.

## What this resolves

This design resolves the missing x86-64 Linux replication and supplies the environment needed for the cgroup memory experiment. It does not create a second independent research team, and it does not eliminate cloud-host variance. The paper should retain the Apple M5 Pro and x86 VM results as separate strata. A cross-machine median ratio would confound architecture, processor generation, operating system, and virtualization, so compare backend behavior within each machine and discuss cross-platform direction rather than claiming a pure ARM-versus-x86 effect.
