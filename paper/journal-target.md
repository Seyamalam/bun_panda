# Target journal decision

Checked on 26 August 2026.

## Recommendation

Target **The VLDB Journal** and use its subscription publication route.

This is the cleanest match between the paper and a Springer Nature Q1 journal. The journal covers database-system technology, information-system architecture, and data-management applications. It also states that submissions with a theory component must include a systems component. That requirement fits a paper built around an implemented TypeScript and WebAssembly dataframe engine, adaptive dispatch, semantic conformance, and reproducible systems measurements. See the journal's [official aims and scope](https://link.springer.com/journal/778/aims-and-scope).

The current ranking evidence is **SJR 2025 Q1** in both Hardware and Architecture and Information Systems, with SJR 1.258. The detailed record reproduces the SCImago Journal & Country Rank 2025 dataset and identifies the journal as an active Springer Nature title in Scopus. See the [2025 SJR record](https://journalsbase.com/journals/vldb-journal) and the directory's [data-provenance statement](https://journalsbase.com/about.php). Springer currently reports a 2025 Journal Impact Factor of 5.3, but its public page does not state a JCR quartile. The paper and cover letter should therefore say **SJR Q1**, not the unqualified phrase "Q1 journal" and not "JCR Q1." See the [official journal page](https://link.springer.com/journal/778).

The journal is hybrid. Springer offers an optional paid open-access route, currently USD 3,190, and a subscription route for which **no APC applies**. Choose the subscription route. This makes publication free to the author, but the version of record will normally remain behind a subscription paywall. See the journal's [official publishing options](https://link.springer.com/journal/778/how-to-publish-with-us).

The choice is ambitious. A package description with microbenchmarks will not clear this journal's bar. The final paper must present a defensible data-management systems contribution and show where the design changes the performance, portability, or semantics of dataframe execution.

## Candidate comparison

Quartiles below are SJR 2025 quartiles. SJR assigns a quartile separately in each subject category, so the named categories matter. JCR and SJR can disagree because they use different databases and category boundaries.

| Journal | Current Q1 evidence | Scope fit for this paper | Hybrid and free route | Decision |
| --- | --- | --- | --- | --- |
| **The VLDB Journal** | SJR 1.258, Q1 in Hardware and Architecture and Information Systems. [2025 ranking](https://journalsbase.com/journals/vldb-journal) | Direct fit for database-system technology, data-management architecture, and implemented systems. [Official scope](https://link.springer.com/journal/778/aims-and-scope) | Hybrid. The subscription route has no APC. [Official policy](https://link.springer.com/journal/778/how-to-publish-with-us) | **Primary target.** Highest technical bar, but the strongest intellectual match. |
| **World Wide Web** | SJR 0.980, Q1 in Computer Networks and Communications, Hardware and Architecture, and Software. [2025 ranking](https://researchjournalrank.com/journal/world-wide-web) | Strong only if browser execution is central. Its scope explicitly includes APIs, Web data management, performance evaluation, server and client technologies, and testing. [Official scope](https://link.springer.com/journal/11280/aims-and-scope) | Hybrid. The subscription route has no APC. [Official policy](https://link.springer.com/journal/11280/how-to-publish-with-us) | Best backup if the three-engine worker study becomes the main argument. The current paper still centers dataframe execution rather than Web systems. |
| **Frontiers of Computer Science** | SJR 1.199, Q1 in Computer Science miscellaneous and Theoretical Computer Science. [2025 ranking](https://researchjournalrank.com/journal/frontiers-of-computer-science) | Broad but plausible. The official scope includes architecture, software, information systems, networks, and emerging multidisciplinary work. [Official scope](https://link.springer.com/journal/11704/aims-and-scope) | Hybrid. The subscription route has no APC. [Official policy](https://link.springer.com/journal/11704/how-to-publish-with-us) | Sensible broad-scope fallback if VLDB reviewers judge the database novelty too narrow. |
| **Empirical Software Engineering** | SJR 0.920, Q1 in Software. [2025 ranking](https://journalsbase.com/journals/empirical-software-engineering) | Plausible after a substantial reframing. It welcomes controlled and replicated studies, data-intensive studies, and infrastructure for empirical research. [Official scope](https://link.springer.com/journal/10664/aims-and-scope) | Hybrid. The subscription route has no APC. [Official policy](https://link.springer.com/journal/10664/how-to-publish-with-us) | Use only if the research questions concern software-engineering practice, reproducibility, and technology evaluation. A dataframe system alone is not an empirical software-engineering question. |
| **Cluster Computing** | SJR 1.014, Q1 in Computer Networks and Communications and Software. [2025 ranking](https://researchjournalrank.com/journal/cluster-computing) | Conditional. Its core is parallel and distributed computing across clusters, clouds, grids, and data centres. [Official scope](https://link.springer.com/journal/10586/aims-and-scope) | Hybrid. The subscription route has no APC. [Official policy](https://link.springer.com/journal/10586/how-to-publish-with-us) | Do not submit the present single-process study. Consider it only after adding credible parallel or distributed execution. |

## Why The VLDB Journal wins

The paper's strongest contribution is neither generic software engineering nor supercomputing. It is the design and evaluation of a dataframe execution system. The mixed TypeScript and WebAssembly execution path, calibration policy, semantic oracle, typed-column reuse, and browser-safe kernel all concern data representation and execution. That story belongs most naturally in a data-management venue.

The alternatives each require a larger change in identity:

- World Wide Web needs the browser to become the primary environment, not a small exported kernel.
- Empirical Software Engineering needs research questions about software-engineering practice and technology evaluation.
- Cluster Computing needs parallel or distributed execution.
- Frontiers of Computer Science can take a broad systems paper, but it gives the manuscript a less precise scholarly audience.

VLDB is therefore the right first target if the remaining experiments establish a genuine system result. If they do not, Frontiers of Computer Science is the honest fallback.

## Changes before submission and reviewer-risk items

The following list separates necessary manuscript and release work from
experiments that would strengthen the case. The present evidence decision is to
report macOS performance only. Items 3 and 4 therefore remain reviewer risks,
not completed or promised results.

1. State the systems novelty in one testable sentence. The contribution should be the adaptive mixed-backend execution design and its semantic contract, not the existence of another dataframe API.
2. Compare against pandas, Danfo.js, Polars, and DuckDB-Wasm on semantically matched workloads. Separate data conversion, initialization, and operation time.
3. If resources become available, add x86-64 Linux replication from multiple fresh cloud VMs and report it as a separate platform stratum. Otherwise retain the explicit one-host limitation.
4. If making a capacity claim, measure peak memory and maximum successful scale under fixed cgroup limits with swap disabled. The current manuscript does not make that claim.
5. Complete the browser study in Chromium, Firefox, and WebKit. Report cold loading and warm execution separately.
6. Repair or sharply delimit the remaining pandas mismatches. The paper must distinguish implemented behavior from pandas-compatible behavior operation by operation.
7. Add fixed public-data and query workloads alongside synthetic cases. TPC-H-derived relational workloads are a natural fit for a database venue.
8. Archive the exact evaluated commit, raw measurements, environment manifests, and scripts. Obtain one independent reproduction before submission.
9. Rewrite the related-work section around dataframe execution, query engines, WebAssembly data systems, adaptive execution, and semantic compatibility. The paper should build on database-systems literature, as the journal's scope requires.
10. Use the journal's current submission instructions at the time of submission. Rankings, charges, templates, and licensing terms can change.

## Publication wording to use

Use this description in planning documents and the cover letter:

> The VLDB Journal is a Springer Nature hybrid journal and is ranked SJR Q1 in the 2025 Hardware and Architecture and Information Systems categories. We intend to use the subscription publication route, for which Springer states that no APC applies.

Do not describe the subscription route as open access. It is free to publish, not free for every reader. If immediate open access later becomes mandatory, check institutional agreements or funding before selecting the paid option.
