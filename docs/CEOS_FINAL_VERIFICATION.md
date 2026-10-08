# Sentinel final connection verification — 8 October 2026

Sentinel remains the independent detector and sound owner. CryptoEarnOS consumes only its bounded read-only feed. This verifies the selected local connection; it does not establish a trading edge or uninterrupted operating reliability.

## Final source and CI checks

The final isolated source suite passed **206 tests in 9.18 seconds**, including six actual Windows launch-expression cases and two explicit source-reselection tests. Ruff lint and format (47 Python files), compilation, dependency integrity and all PowerShell launcher parsing passed. The copied source had no operating configuration, environment file or records; only a copied pinned interpreter and its virtual-environment metadata were added for the six bootstrap tests. Source-only evidence is local/ignored at the Durable workspace's `.qa/sentinel-final-ci-suite.log`.

The earlier source-only run passed 198 tests and skipped those six launch cases because it lacked the interpreter. Three initial deployment-asset copy omissions were corrected before that pass. The original bootstrap-test SHA-256 was `d27c420c3e479f82fb2301094df1096226728ff9ae86a54674351e126184e3b3`; after formatting and the explicit reselection argument it is `18d846f8462f93f9919a0be16be5c620cf7a1f51509b889b098c90ec9dc9dbf1`. Two sandbox-restricted retries stalled and were stopped by exact test-process identity; the final full isolated run used local Windows test permissions.

Independent review identified caller-directory package shadowing. The managed helpers now launch isolated Python with the selected absolute source path. Actual PowerShell expressions reject hostile caller directories, `PYTHONHOME`, `PYTHONPATH` and user-site settings. Ruff changes were limited to formatting, import order, an unused variable and explicit exception suppression; the older reliability implementation was preserved. AST comparison confirmed unchanged behavior for formatting in the app, soak monitor and their existing tests.

## Selected-source refresh

The first selected revision `0af7d7eaaa5ad21e063be3bda82f98e938eb405925cb9ea4045d3eaee26fdac5` and launcher manifest `a0609a5d396251448ca428bff1a68c858cdf68857dad9d1fb5997dd07f0474e9` are historical. At 06:38 UTC the reviewed selection became:

| Evidence | Selected value |
|---|---|
| Runtime source SHA-256 | `24a687107d0f4bfe7cbaa5227ce289543d6286b8f3155cca12021baafaad8685` |
| Launcher manifest SHA-256 | `9eaa00d6bcea7413d3c9aad8a66174991d27bc0c02b33019cefde11d925afa30` |
| Authenticated instance | `a7d27f9e-9cb0-499f-8a48-0ffbfdbae488` |

Setup requires matching previous and reviewed new hashes for replacement, serializes with the control helper, and atomically preserves the existing DPAPI envelope. Disposable tests proved that missing, stale or incorrect selections and a failed file replacement preserve the original authority. No automatic source promotion was added.

The real transition first verified the old listener by HMAC and exact process ownership, made an owner-only rollback copy, preserved configuration/environment bytes and the encrypted credential, and stopped only the verified old Python listener. The registered helper started the new isolated instance; the existing guardian remained authoritative. A conservative initial process guard refused the older launch expression before any mutation; inspection of that verified process supplied the exact permitted match. Two subsequent authenticated metadata observations confirmed detector progress, ready status, fresh acknowledged Binance/Bybit/OKX feeds and enabled local sound. Evidence and private rollback material remain ignored at `.qa/sentinel-selected-refresh-20261008/`.

## Remaining observations

Earlier isolated checks proved native cold start/reuse, browser-independent detector progress, duplicate-guardian exclusion, wrong-listener rejection and local sound dispatch success. External notifiers remained disabled. Physical sleep/resume, actual Windows login/reboot, human audibility and long-duration reliability are still unobserved; a responding HTTP endpoint alone does not prove continuous monitoring. No operating alert bodies or credential values were printed, and no trading, spending or external notifications were performed. GitHub's cross-platform matrix and Docker-build result must be checked before merge.
