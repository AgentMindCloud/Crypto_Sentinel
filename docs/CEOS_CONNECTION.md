# Independent Sentinel connection

Sentinel owns market detection, local sound, its database and feed recovery. CryptoEarnOS imports a read-only alarm feed and can open the registered Sentinel workspace. Closing either dashboard leaves the detector running while Windows is awake.

## Selected local setup — 8 October 2026

- Primary: `http://127.0.0.1:8787/`, application 0.2.0.
- Selected package source revision: `24a687107d0f4bfe7cbaa5227ce289543d6286b8f3155cca12021baafaad8685` (06:38 UTC refresh).
- The private connector is configured. Its separate read token is encrypted with Windows CurrentUser DPAPI under `data/ceos-connection/`; the directory grants only the current owner full access. Dashboard/ingest credentials and operating settings were preserved.
- `Crypto Sentinel Managed Alarm.lnk` starts one native guardian at login. It verifies a saved source/helper manifest, reuses the authenticated primary, and retries unavailable starts with 5–60-second backoff. It does not start Docker or a shadow/soak process. An occupied unverified port is preserved and reported unavailable.
- Local sound is enabled. Metadata inspection confirmed ntfy, Telegram and webhook delivery are disabled. The integration did not change these settings or send external messages.

In CryptoEarnOS, use **Markets / Research → Open Sentinel**. That owner-only action supports only this registered application. It never transfers dashboard authority or trading permission. The credential-free dashboard URL preserves Sentinel's own login boundary.

## Maintainer commands

Run these from this Sentinel checkout; do not print the `connector` action's output. That action is reserved for the local backend's captured secret loader.

```powershell
.\scripts\Setup-Integration.ps1 -ReviewedSourceRevision 24a687107d0f4bfe7cbaa5227ce289543d6286b8f3155cca12021baafaad8685 -EnableStartup
.\scripts\Manage-Integration.ps1 -Action status
.\scripts\Manage-Integration.ps1 -Action open
```

Setup is idempotent for the selected source and refuses competing legacy startup owners. The startup guardian uses a named mutex; the control helper serializes starts. Source changes require a verified new selection. A broken optional connection disables integration without stopping the independent detector. Source state is not silently promoted by a CEOS rebuild.

For an explicitly reviewed upgrade, also pass `-ExpectedPreviousRevision` with the exact currently selected hash. Setup verifies both revisions under the same lifecycle mutex and atomically preserves the encrypted credential while updating the selection. It does not restart an existing detector. The maintainer must first verify its authenticated identity and process ownership, then restart that exact instance and confirm the new revision. A missing or mismatched previous selection is refused. Keep a private rollback copy; never clear the connector or operating records to upgrade.

Before sending its read credential, the connector proves the listener knows that credential through a random-nonce HMAC challenge. The feed and metadata routes reject foreign Host/Origin, redirects, write-token reuse and unbounded responses. Metadata status never opens the alert database. The owner backend's PowerShell child receives only a native Windows environment, a fixed executable extension list and System32 PATH; backend credentials are not inherited.

## Evidence and limits

- 70 focused Python checks passed, covering existing dashboard/config/app behavior plus DPAPI reuse, source mismatch, metadata-only identity and detector progress without a browser.
- 13 focused TypeScript checks passed, including the actual Python HTTP endpoint, wrong-listener rejection before credential transmission, action allowlisting and secret redaction. CryptoEarnOS typecheck passed.
- A disposable Windows profile proved native cold start, authenticated instance reuse and detector progress with the browser closed. Public Binance, Bybit and OKX payloads were fresh. Only browser dispatch was replaced in that copied test helper; all its notifiers were disabled.
- A separate isolated API sound test returned 201 and persisted `queued → attempt → success` for one local warning. This proves dispatch reported success; human audibility was not observed.
- The selected operating primary, native guardian and CEOS managed status call were verified. One guardian remained after a duplicate launch exited cleanly. No operating alert bodies or credential values were inspected.
- A tampered helper failed a disposable manifest check. Actual Windows reboot/login, physical sleep/resume, long-duration uptime and human acoustic audibility remain final-campaign observations. A responding HTTP endpoint alone never proves current market monitoring.

Final activation receipt is local/ignored at the Durable workspace's `.qa/sentinel-managed/final-activation-receipt.json`, SHA-256 `15a6386c645b4be91b2974c308206b700bba2f2b1dbfe7cc279d31e35e6d0ac4`. It records only source/status, startup, guardian and hash metadata. No production trading or financial readiness is implied.

That initial activation is superseded by the reviewed refresh in [CEOS_FINAL_VERIFICATION.md](CEOS_FINAL_VERIFICATION.md): 206 tests passed, configuration and encrypted authority were preserved, and the restarted detector passed identity and progress checks. Physical sleep/reboot and long-duration observations remain pending.
