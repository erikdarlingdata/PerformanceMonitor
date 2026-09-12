# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- changelog-layout:begin -->
This file is an **index**. From 3.0.0 on, each released entry is compacted to its title and
the issues it references, and the full prose lives in one file per minor version under
[`docs/changelog/`](docs/changelog/). GitHub will not render a blob of the size the whole
history in one file had reached, so the prose moved rather than got shorter.

`[Unreleased]` keeps its full prose here, because this is where it is written. At the release
cut it is archived and compacted like every other version:

    python3 tools/changelog/changelog_archive.py split

Releases before 3.0.0 are not archived: those entries carry no prose to move.
<!-- changelog-layout:end -->

## [Unreleased]

### Added

- **A mute rule that has outlived its reason now reports itself** ([#3306]) - a mute is a deliberate blind spot in a monitoring tool, and nothing surfaced that one existed, how old it was, or whether it would ever expire; the only way to find one was to already suspect it and call `get_mute_rules`. So a rule created to stop a false-positive flood keeps suppressing the alert after the fix ships, and the symptom is silence rather than a number going the wrong way - the Retention Held pattern one surface over, and harder to notice for exactly that reason. A new fleet-level service self-alert reports a rule that is enabled, has no expiry, and was created longer ago than the longest expiry the mute dialog itself offers, judged on that conjunction rather than on permanence alone, which the dialog offers on purpose. Severity follows blast radius: a rule that constrains nothing suppresses EVERY alert on the store, so the fleet reads healthy for want of anything being reported, and that reads Critical. Nothing is un-muted automatically - a silent expiry floods the delivery channel the rule was usually created to protect - so the alert states the rules, their ages and their reasons and leaves the decision with the operator, while the triage link drills straight into the rule list and the week of alert history a muted alert is still recorded in. No blanket mute can suppress it: a rule that constrains nothing matches every alert including this one, so honouring such a mute would let the condition suppress the only report of its own subject. It re-states itself once a day rather than on the shared alert cooldown, which on the shipped defaults would have posted every fifteen minutes about a fact that changes on a scale of days.
- **User-authored custom alert rules** ([#3285]) - a threshold alert on any compose-catalog measure, the companion to the custom notebooks: choose a source, measure, aggregate and window, a comparison with Warning/Critical thresholds or a range (within or outside a band, with an optional Critical tier), N-sample hysteresis and a scope of all servers, a chosen few, or a fleet tag (and its sub-tags), and the service evaluates it on its own sweep and delivers through the existing plumbing (cooldown, mute rules, Summary/per-event, email and webhooks) - for SQL Server and PostgreSQL metrics alike. Rules are authored over MCP (`create_custom_alert_rule` / `update` / `delete` / `get` / `list` / `validate_custom_alert_rule`, plus `test_custom_alert_rule` to see a rule's current value and would-fire verdict on each in-scope server without delivering, and `list_custom_alert_templates` for a starter set covering common PostgreSQL and SQL Server signals such as autovacuum backlog, replication lag, connection saturation and blocking); the compose catalog gained the PostgreSQL measures for all 20 PG collector tables, gated so a measure only offers on the server types that collect it, which makes PostgreSQL targets both alertable and chartable. Notifications show the rule's name rather than an internal id; a rule that no longer compiles or can never fire (scoped to no monitored server, or an always-NULL measure) is surfaced as one aggregated service self-health alert; and disabling or deleting a rule force-resolves any open incident. This release ships the evaluation engine, the MCP authoring surface, and a visual web editor in the viewer (the Scalar compose picker with inline validation, a live per-server would-fire preview, start-from-template, and a "create alert from this panel" action); the rule list shows when each rule last fired.

### Changed

- **A server's deadlock health band reads a RATE, not a raw count** ([#3368]) - `ServerHealthClassifier.DeadlockSeverity` was `count > 0 ? Critical : Healthy`, and `OverallMetricSeverity` short-circuits on the first Critical, so one resolved deadlock anywhere in the window banded a server Critical whatever else it looked like - making the band's most common cause the one condition that had already resolved itself. It bands deadlocks per HOUR now, normalised over the window the count covers, so one pair of thresholds means the same condition on a 1-hour read and a 24-hour one. Measured on a 43-server production OLTP fleet, the count band read 13.4% of one-hour windows Critical and 87.9% of 24-hour windows Critical - the same servers and the same code, a 6.5x swing from window length alone. Defaults are 5 per hour (Warning) and 20 per hour (Critical), derived from 14 days of collected deadlocks across those servers rather than chosen as round numbers: 99.1% of server-hours hold at most two, the routine mode tops out at 15 in an hour, and nothing at all was observed between 16 and 89 - so the Critical tier sits inside a measured empty interval. Both are store-backed knobs on `config_alert_settings` (V120), reported and accepted by `get_alert_settings` / `update_alert_settings` under a new `health_bands` group and editable in the Viewer's Settings window, clamped to 1-1000 per hour - the floor keeps the knob a rate, so no reachable setting restores the count band. The `health_bands` group is deliberately separate from `deadlocks`: that one governs whether an alert is DELIVERED, these decide what a card reads and how `get_fleet_overview` counts bands, and nesting a band tier under the alert would invite tuning one and expecting the other to move. A window too short to normalise is not banded on a rate at all - a non-zero count reads Warning and a zero count reads Unknown, so an absent denominator can never read as healthy. A target with no deadlock source reports Unknown for the severity AND null for the rate, both derived from the same reading, so a PostgreSQL card cannot publish a per-hour figure its own severity denies. Both cards publish the rate beside the count and name it in the "needs attention" reason.

### Fixed

- **`get_collection_log` can answer the slow-run question it exists for, and says what window it actually reached** ([#3287]) - the read accepted `hours_back` and then made it irrelevant: newest-first with a 200-row cap means a 24-hour request on a busy fleet is satisfied by roughly the last 25 seconds of activity, and the payload reported the span REQUESTED beside `truncated = true` with nothing saying where the data actually stopped. It also accepted `collector_name` and `min_duration_ms` by dropping them, returning a complete-looking unfiltered page with no error - the same shape as the coverage census in [#3278], one level up: a limited page that cannot be told apart from an answer. Both filters now go into the SQL ahead of the cap, so `run_count` and `truncated` describe the MATCHING rows rather than the unfiltered window, and supplying `min_duration_ms` also ranks the page slowest-first, because a duration floor under newest-first ordering keeps the recent matches while the slow runs being hunted are precisely the ones that are not recent. The payload carries `oldest_returned_collection_time` and `newest_returned_collection_time`, which bound the PAGE rather than claiming a window floor, plus the `order` it used and the filters it applied - and the two stamps mean different things under the two orderings, which the tool description now states: under newest-first the page is a contiguous slice so the oldest stamp IS the reach, while under a duration floor it is a cost-ranked sample drawn from the whole window that says nothing about reach. A filtered read with no matches no longer borrows the quiet-window sentence, and the never-collected probe stays AHEAD of that branch so a fault outranks a miss - telling a server that has never collected to "drop the filters and see what the window holds" sends it widening a window that will never fill. `truncated` is also now observed rather than inferred: comparing the row count to the cap could not tell a window holding exactly `limit` runs from one holding more, so the read over-fetches by one and reports what it saw. A floor that is not a real number is refused rather than accepted - every comparison against NaN is false and `+Infinity` is not negative, so both passed a sign test, matched no run, and came back as an empty duration-ranked page naming a floor that was never a number; an oversized literal such as `1e400` overflows to `+Infinity` rather than failing to parse, which is the likeliest way one actually arrives. Zero remains a real value: it admits every row and is how a caller asks for the whole window ranked by cost. Both SKUs, and `/api/read/get_collection_log` passes both filters through.
- **A failed mute-rule reload no longer drops every mute in force** ([#3354]) - both mute-rule stores swallowed a failed read and returned an empty set, so `MuteRuleService.LoadAsync` could not tell "this store holds no rules" from "I could not read them" and replaced its cache with the empty answer. Because that reload runs on every control-plane config change and not only at startup, one transient store failure un-muted every rule an operator had deployed, delivered the flood those rules existed to stop, and left no artefact at all on Lite. Both stores now propagate the fault and the reload keeps the rules it already holds; the first load on an empty or unmigrated store still starts clean. Both SKUs log the event and count it on the swallowed-alerting-read surface, and `Stale Mute Rules` can no longer report all-clear off a read that failed.
- **Webhook payload timestamps come from one clock read per firing** ([#3355]) - `WebhookAlertService` reads its clock once per fan-out and threads that instant to the Teams, Slack, generic and PagerDuty builders, which take it as an optional parameter. A fan-out crossing a second boundary no longer posts the four channels a second apart, and the Teams and Slack cards' own "Time (UTC)" and "Time (Local)" facts are two renderings of one instant rather than two separate reads.
- **The Retention Held thresholds are settable, so the alert an operator most needs to tune is no longer the one alert that cannot be** ([#3297]) - `RetentionHoldWarnRatio` and `RetentionHoldCriticalRatio` were compile-time constants, and the #3296 reporter received an hourly CRITICAL naming `Retention Held` against `Monitor Store` with nothing in Settings matching either phrase. V119 puts both on `config.config_alert_settings` as `retention_hold_warn_ratio` / `retention_hold_critical_ratio`, read live through the same by-reference seam as the [#2136] cadence knob, reported and accepted by `get_alert_settings` / `update_alert_settings` under `self_alerts`, and editable in the Viewer's Settings window - so a change takes effect on the next sweep with no restart. Both clamp to [2.0, 100.0] and the floor is the shipped warning default, so the knobs RAISE the tiers and cannot lower them: retention drops whole CHUNKS and the retention job runs on a schedule, so a tier legitimately holds its horizon plus up to a chunk plus up to one job interval while working perfectly - measured at **1.4x** on a healthy production store under 4-day horizons, not the ~1.25x the old constant's own example implied. So anything at or below ~1.5x fires on a store that is working correctly, and the band above that up to 2.0x is margin nobody has measured; 2.0x is the only figure with evidence behind it, and it has to be safe for the coarsest tier (4-day horizon, 1-day chunks) rather than the average one, because one global ratio serves tiers whose chunk-to-horizon ratios differ by an order of magnitude. The ceiling keeps the knob a threshold rather than an undisclosed off switch - `create_mute_rule` silences an alert scoped, expiring and listed by `get_mute_rules`. A critical tier set BELOW the warning tier is accepted rather than corrected: firing is gated on warn and severity on critical, so every fire is simply Critical, where a `GREATEST` on read would accept the operator's value and then use a different one. Every retention-hold decision AND every threshold the alert states back to the operator now goes through the seams, because a bare constant in the message would name a threshold the engine is not using while the decision looked right. Also fixes `AlertSettingsRow.ValueEquals`, which had never compared [#2136]'s cadence knob or [#2349]'s four file-growth gates - so a store whose only customization lived in one of them read as DEFAULT and had its whole row overwritten by the viewer's on the one-time control-plane migrate-in, held now by a reflection pin over the row's settable properties rather than by a list. `docs/retention-hold-runbook.md` documents the three ways in and why the floor is where it is.
- **`Stale Mute Rules` now honours a mute rule that NAMES it and ignores one that only reaches it incidentally, so the one alert an operator could not answer has an off switch** ([#3348]) - [#3343] shipped the condition with `honorMuteRules: false`, and self-alert conditions have no enable/disable in `config_alert_settings` either, so the only ways to stop it were to delete the mute rule it was reporting or to not run the build. The reasoning it shipped with was right and is preserved: a rule that constrains nothing matches EVERY alert including this one, so honouring the shared mute seam naively would let a blanket mute suppress the only report of its own subject. The seam is still never consulted for this metric, and that is load-bearing rather than incidental - `_isAlertMuted` answers **one boolean over every rule** and cannot say WHICH rule answered, so an affirmative answer is as easily a fleet-wide silence as a decision about this alert. The condition instead scans the rule list it already receives for one whose `metric_name` names it, which needs **no new seam and no interface change**. **The explicit/incidental split is the matcher's own answer, not an assertion about it**: `metric_name` is compared with exact `OrdinalIgnoreCase` full-string equality - only the four `*_pattern` dimensions are substring matches, and there is no glob, prefix or regex form of a metric constraint anywhere in the seam - so a rule either spells the metric out or does not constrain metrics at all, and the issue's "does a pattern that happens to match count as explicit" question does not arise rather than being decided. `MuteRule.NamesMetric` is that one comparison and `MatchesAt` routes its metric arm through it, so the two cannot drift. **Self-suppression is impossible by construction**: the only input to the decision is a rule that names the metric, and a blanket rule names nothing, so a blanket mute cannot reach the decision whatever else it silences. The narrowing is strictly one-directional - an explicitly-naming rule still has to pass the FULL matcher against this alert's real context - so honouring one can never suppress more than the ordinary seam would. Which shapes can reach a fleet-level condition was checked rather than assumed: `AlertMuteContext.ServerName` is a non-nullable string with **no null server key**, and a fleet self-alert passes the synthetic `"Monitor Store"` sentinel, so a rule scoped to that sentinel DOES suppress while one aimed at a monitored server cannot - both pinned, because the negative is only a real constraint if the positive holds. `MuteRule` grew the clock-taking `MatchesAt`/`IsExpiredAt` forms so the rule's expiry is read on the evaluator's injected clock rather than `DateTime.UtcNow`; without them the verdict would have depended on the wall-clock date the suite ran on, which fails intermittently rather than consistently. Deliberately not an overload - two `Matches` overloads make three existing `<see cref="Matches"/>` targets ambiguous. `FireAsync`'s `honorMuteRules` flag becomes a nullable `muted` verdict: null (the default, every sibling) asks the seam, and the one condition that cannot use a single boolean passes its own answer. **The daily re-fire cadence stays, on re-grounded reasoning** - it was chosen BECAUSE the alert was unsuppressible, a premise this change falsifies, so the comment would otherwise have been a stale load-bearing claim. Two reasons independent of suppressibility hold it: the subject is a creation date against a seven-day bound, identical on every sweep, and a five-minute cadence would make a permanent mute the ONLY survivable configuration - manufacturing the exact blind spot the condition exists to report. **A muted alert is still recorded**, so the history row lands every re-fire naming the rule that silenced the channels, `get_mute_rules` lists it with its reason, and it can expire; the one honest consequence is that an explicit mute which itself goes stale silences the report that includes itself, which is the informed operator's decision rather than an unaware one. The condition is untouched - the subject, the Critical severity for a rule that constrains nothing, and the refusal to auto-expire are all unchanged. **No rung**: `metric_name` already exists on `config_mute_rules` and the matching is in-process. Schema stays **118**.
- **Per-event alert emails attach their own incident's graph** ([#3330]) - `AlertContext.AttachmentXml` was one string for a whole alert while the cards carrying it are per-incident, so an alert with N distinct deadlock fingerprints sent N emails that all attached the first graph in the window. The forensic XML now rides on `AlertIncident`, keyed by the same #1140 fingerprint the incident already has: the per-event splitter gives each message its own, and #3324's delivery filter selects alongside the incidents it keeps. A blocking chain seen only by the DMV-snapshot fallback has no report to attach and now carries none rather than a neighbouring chain's. Email-only; the four webhook channels are byte-identical.
- **`delivery.cooldown_minutes` is reachable from the control plane** ([#3314]) - the per-fingerprint throttle on every Slack, Teams, PagerDuty, generic-webhook and email delivery was stored as `config_notification.email_cooldown_minutes`, reported by no MCP tool and accepted by none, so on a headless deployment with no SMTP configured the only path to the number governing channel volume was the WPF Settings window. `get_alert_settings` now reports it and `update_alert_settings` accepts it under a channel-neutral name beside `delivery.mode` / `delivery.per_event_max`, with the stored spelling kept as a write-only alias so existing configs and the Lite Settings window are unaffected. Both tool descriptions now distinguish it from the top-level `cooldown_minutes`, which gates the engine's fire decision rather than the post. Because the value lives on a second singleton config row, `update_alert_settings` now writes one statement per table inside one transaction, and the read/write round-trip invariant is held per table. The `mcp` role gains UPDATE on exactly that one column of `config_notification`; the SMTP password, the webhook URLs and the PagerDuty routing key stay unreadable and unwritable, and even the non-secret sibling `smtp_host` is not writable. The 120-minute ceiling is unchanged: the cooldown is one global number applied to every fingerprint on every server, so silencing one recurring signature belongs to `create_mute_rule`, which is scoped, expires, is listed by `get_mute_rules`, and still logs the alert.
- **High CPU now requires the condition to persist** ([#3282]) - the built-in High CPU alerts on both engines fired on a single sample over the threshold and resolved on the next sample under it, so a momentary spike was indistinguishable from sustained saturation; measured fire/resolve pairs on a 42-server fleet ran 42-147 seconds apart. Both evaluators now go through the shared `AlertPersistenceGate`: three consecutive breaching CPU samples to fire, two consecutive clearing samples to resolve, counted per collected SAMPLE rather than per alert sweep so a re-read of one reading cannot fill the streak. The per-server state is persisted on both SKUs, so a restart neither re-announces an already-open incident nor loses a partly-built streak, and a CPU reading that stops arriving now freezes the gate instead of announcing a recovery nobody measured.
- **A mute rule created or deleted outside the Viewer did not take effect, for an unbounded time, and every surface said it had** ([#3315]) - `config.config_mute_rules` carried no `config_version` bump trigger, and the alert engine consults `MuteRuleService`'s in-memory cache, which is reloaded only when that beacon changes. So `create_mute_rule` returned `created`, the row really landed, and matching alerts kept being delivered until an unrelated config write or a service restart happened to force a reload - with `get_mute_rules` listing the rule the whole time, because it reads the table and not the cache. `delete_mute_rule` had the same gap in the more dangerous direction: an operator un-mutes, believes alerting is restored, and the stale cache keeps suppressing. Storage V117 adds `trg_bump_mute_rules`, the fifth instance of V17's statement-level beacon trigger and sharing its function verbatim, so ANY write to the table makes the service reload its mute cache on the next sweep - `AFTER INSERT OR UPDATE OR DELETE`, so disabling a rule counts as well as removing it. The `mcp` role needs no new grant: the column-level `UPDATE (config_version, updated_at)` on `config_service` provisioned for the `config_alert_settings` trigger is exactly what this SECURITY INVOKER trigger uses, verified against a live store in both directions. The viewer's connect-time probe gains a `pg_trigger` sentinel for the rung rather than an `information_schema.triggers` one, which lists only triggers on relations the current role owns or holds a non-SELECT privilege on and so would report the rung absent to the read-only `viewer` seat on a store that is fully migrated.
- **Webhook and email alerts no longer re-render incidents that are still inside their own cooldown window** ([#3313]) - a batch posted when any one of its incident fingerprints was outside its #1154 cooldown window and then rendered every incident it carried, so a single new fingerprint re-delivered every co-resident fingerprint that had already gone out minutes earlier. Teams, Slack, PagerDuty, the generic webhook and email now render only the incidents actually outside their own window, with a one-line footer stating how many others are still open. The alert history row continues to record every incident, so nothing the MCP reader, the triage page, the Viewer's detail pane or the mute pre-fill reads has changed. Per-event delivery already sent one incident per message and is unchanged. PagerDuty's `dedup_key` now anchors on the first DELIVERABLE incident, which corrects a correlation defect: a batch of one in-window and one fresh incident previously triggered under the in-window incident's key and folded into its existing alert, so the genuinely new incident never surfaced there at all.
- **A Collector Cost Regression self-alert now has to be worth reporting, not merely real** ([#3316]) - the predicate floored the average daily TOTAL cost and then compared cost PER RUN, and a total-cost floor is cleared by volume, so it never constrained the quantity the ratio tested: a collector averaging 3 ms per run cleared the 1,000 ms/day floor on run count alone and was then judged by a 2x ratio on that 3 ms. Those firings were truthful rather than noisy - both sides of the ratio are means over many runs, so rounding averages out - and still unactionable, the measured case doubling 3.0 to 6.1 ms per run across 50 runs to add 0.16 s of collection time a day. A third gate now asks what the regression COSTS, the per-run rise times the volume it is paid on, which is unit-consistent with the ratio and unlike a minimum per-run baseline still reports a 3 ms collector that runs often enough for the rise to matter. The floor is derived from a measured fleet sample where real regressions added 21.8-22.6 s a day and unactionable ones 0.16-3.6 s, and the alert now states the added cost that justified it.
- **Analysis-finding alerts no longer render their Diagnosis facts twice on every delivery channel** ([#3302]) - the finding's prose detail restates its own structured context under different labels, so the delivery gate's equality test could not see the duplication: a restatement is not a copy. The producer now DECLARES whether its prose restates its context, and the analysis path declares that it does, so the facts go out once - while a producer whose prose is genuinely its own, such as a remediation string the context does not carry, still delivers it. That declaration is a required member rather than a defaulted one, so a new producer cannot omit it silently, and where it is defaulted it defaults to DELIVERING: a redundant paragraph is cosmetic, while a withheld one discards an operator's only copy of the remedy. `config_alert_log.detail_text` is unchanged, so the MCP reader, the triage page, the Viewer's detail pane and the mute pre-fill all read what they always did.
- **A deadlock or blocked-process report whose statement sat inside a stored procedure now names the procedure** ([#3307]) - SQL Server emits an unresolved `Proc [Database Id = N Object Id = M]` placeholder in `inputbuf`, and neither collector attempted to resolve it, so the alert named the object the deadlock fought over but not the code that lost. Both collectors now resolve it to `database.schema.object` with one batched lookup per collection cycle - three-part, matching how `Involved Objects` already qualifies names - and keep the raw placeholder when the object has been dropped or the monitoring login cannot see its database, so a failed lookup degrades to today's output rather than to a half-built name. The parse and the lookup are C#-side: the XML shredding the resolution hangs off already happens locally, and the monitored server is asked only for an object name.
- **The installer and the portable launcher are signed, and a release-time guard refuses to publish an unsigned executable anywhere else** ([#3288]) - the portable launcher stub, `Update.exe` and `Setup.exe` shipped unsigned on both products while the app payload was signed, so the file a user double-clicks in the portable zip was a 399 kB unsigned launcher rather than the signed 182 kB app - which is exactly what an "unknown publisher" policy blocks, and what forces the elevation that breaks Entra MFA in #3196. Velopack creates those three during packing, so no pre-pack round reaches them, and SignPath's open-source policy requires origin verification through a trusted build system - a signing request is accepted only through the GitHub Action, which cannot be invoked from inside a running `vpk pack`. So the standalone assets are signed by an Action step AFTER packing: `Setup.exe`, and the root launcher and `Update.exe` inside `Portable.zip`. Neither carries a recorded hash - `releases.<channel>.json` records a SHA256 and a Size for the two `.nupkg` files and nothing else - so rewriting them invalidates nothing, and the portable zip is rebuilt member by member with every unchanged entry's local header, name and raw compressed bytes copied verbatim, verified against a snapshot of the original before it replaces it. **Two executables are still unsigned, and only inside the `.nupkg`**: `lib/app/<App>_ExecutionStub.exe` and `lib/app/Squirrel.exe`, because that package's bytes are what the updater verifies and what the delta patches against. Their deployed copies in `Portable.zip` ARE signed; the copies `Setup.exe` lays down are not, so an installed user's shortcut target and updater remain unsigned. The release guard walks every published `.exe` including those inside the zips and packages, exempts exactly those two by member path AND container so the same names anywhere else still fail, reports each exemption's reason, and treats an empty census as a failure rather than a pass.
- **PostgreSQL instance CPU bands on Aurora Serverless v2 capacity headroom, not on percent-of-allocated** ([#3281]) - `os.cpuUtilization.total.avg` is percent of the capacity CURRENTLY ALLOCATED, and a serverless instance class re-sizes that allocation continuously - so a 1-vCPU instance reads exactly 100% whenever one core is busy for a minute, which is the routine trigger for scaling up rather than a saturation incident. Measured on a production instance at such a minute: 4 of 12 configured ACUs in use, 33% of the ceiling, while CPU read 100% with idle at 0.0. `RdsCpuIngestor` now collects `os.general.acuUtilization.avg`, `os.general.serverlessDatabaseCapacity.avg` and `os.general.maxConfiguredAcu.avg` on the Performance Insights call it already made, with a single retry on the CPU metric alone if the service rejects the request shape - so an instance class with no ACU concept loses the capacity columns rather than its CPU reading. The fleet card ladder and the High CPU alert both band on percent of the CONFIGURED ceiling, and where no capacity reading exists the band is Unknown rather than Healthy and the alert does not fire. The raw CPU figure stays collected and displayed - it answers "was a core pinned" - labelled with what it is a fraction of, and `get_pg_cpu_utilization` no longer describes the reading as capacity-independent.
- **`get_pg_index_bloat` now carries its own coverage denominator, so a limited page cannot read as a coverage claim** ([#3278]) - the read sorts answerless rows first by design, so any limit below the answerless population returned 100% of them, and nothing distinguished "the collector estimated nothing" from "it covers plenty, sorted behind more numerous skips". It now returns a population-level census from a separate query no row limit can reach - candidate btree indexes, trusted against suppressed, and each suppression reason - with BYTES beside every count, because the row and byte rankings of those reasons disagree: one bucket measured second-largest by rows and last by footprint, at 0.0004% of it. The candidate total is derived from its parts, so the published total cannot disagree with the breakdown explaining it. Shipped on the MCP tool, the WPF storage panel and the web dashboard through one classifier.

## [3.7.0] - 2026-09-10

Full entries: [docs/changelog/3.7.md](docs/changelog/3.7.md)

### Added

- **The member-range scan now reports ranges that stop short of their own content, which it previously read as healthy** ([#3230])
- **The clock-frame census now sees a renderer reached through a one-hop wrapper** ([#3207])
- **`.github/dependabot.yml` now tells whoever reviews a `Microsoft.Data.SqlClient` or `Azure.Identity` bump to check whether `ExcludeBrokerCredential`'s default or the broker's packaging changed** ([#3219])
- **Lite now records which Azure credential `DefaultAzureCredential` selected for an Existing Sign-In (`az login`) connection** ([#3224], [#3219])
- **A guard over the boundary three clock-frame defects shipped through** ([#3208])
- **Added a sixth authentication mode, Azure — Existing Sign-In (`az login`), for Azure SQL Database where Microsoft Entra MFA cannot work** ([#3214], [#3196])
- **A collector's PostgreSQL extension dependency is declared rather than described** ([#3187])
- **A refresh-ceiling staleness finding, reported separately from where the reading sits against its slot** ([#3182])
- **A TimescaleDB compression run is watched against the clearance its own minute of the hour has before the next continuous-aggregate refresh starts** ([#3112])
- **Remediation credential seam and journal actor** ([#2138])
- **A collector definition can now record what it measured on its own `collection_log` row** ([#3161])
- **Every alerting-side store read that fails now records how long it ran before it faulted** ([#3099])
- **Every `<see cref>` target in the repository is now resolved, and the ones that resolve to nothing are inventoried** ([#3086], [#3025])
- **`TsqlConventionGuardTests` enforces the checkable part of `CONTRIBUTING.md`'s T-SQL Style list, which nothing had enforced: PR #3078 introduced `COUNT(*)` and `COUNT(DISTINCT …)` into a new collector query and passed the Linux build, the PostgreSQL tests, every whole-tree guard, the command-deadline family, `review` and `verify`** ([#3081])
- **A collector stalled mid-read now gets a server-wide wait sample taken out of band** ([#2880])
- **`get_pg_plan_capture_readiness`, and the PostgreSQL read-coverage ratchet reaches zero** ([#3070])
- **A monitored PostgreSQL target's `lc_messages` is now a named precondition rather than an invisible one** ([#3061])
- **Pinned the PostgreSQL panel registry against both the XAML that renders its panels and the load path that fills them, resolving every one of the 27 registry collectors to its panels through the two names its loader already carries** ([#3062])
- **Watch the heaviest hourly continuous-aggregate refresh's live runtime against the refresh slot it has to fit inside, on the existing hourly job-health sweep** ([#3044])
- **A guard that no XAML layout grid assigns a `Grid.Row` or `Grid.Column` at or past its own definitions** ([#3049])
- **The monitoring store now reads its OWN PostgreSQL server log, as a per-class census rather than as lines** ([#3021])
- **The alerting subsystem's own store reads failed silently** ([#3013])
- **The retention-horizon convergence's “no-op on every later start” claim is now asserted rather than only stated** ([#2960], [#1958])
- **A new command-timeout pin can no longer arrive with its own private copy of the deadline judgement, or quietly stop using the shared one** ([#2972], [#2938], [#2940], [#2966])
- **A new data-moving migration rung can no longer land without re-deriving the advisory-lock wait budget** ([#2894], [#2888], [#2936])
- **The per-database plan/text fetch split is now ten columns on the run's row, so the phase that owns a deferred fetch can be aggregated instead of scraped per server** ([#2860], [#2811], [#2859], [#2864], [#2902], [#2851])
- **The Azure per-database branch now emits a `connect:`/`open:`/`drain:` phase split, the last collection path that reported one blended number** ([#2855], [#2164], [#2312], [#2851], [#2854], [#2864], [#2811], [#2819], [#2859], [#2860])
- **A collector losing cycles to the wall-clock budget no longer reports `HEALTHY` with `errors: 0`** ([#2804], [#2803], [#2779], [#2784])
- **An abandoned collection cycle now records what it was DOING, not merely that it stopped** ([#2864], [#2673], [#2859])
- **The server-scoped phase split is now a set of columns, so it can be aggregated and read without an SSM session** ([#2859], [#2851], [#2811], [#2796])
- **Server-scoped collectors now emit the `open:`/`drain:` phase split, so the largest collector on the fleet can be attributed** ([#2851], [#2811], [#2816], [#2796])
- **The sweep-body detach policy (#2700/#2717) is now pinned, including the invariant that makes it safe** ([#2840], [#2841])
- **`query_store` plan and text fetch phases now name the STORE half separately from the TARGET half** ([#2811], [#2312])
- **OIDC sign-in for the web dashboard: token validation and claim-to-role mapping** ([#2550])
- **The auto force-plan bot's detection half arrives, and it cannot write to anything** ([#2138])
- **`timeBucket` + `topN` compose into a top-N time series** ([#2734], [#2737], [#1687], [#2733])
- **A notebook panel cell can pin its own time window** ([#2735], [#1606], [#2733])
- **Custom-view validation rejects unknown keys instead of silently ignoring them** ([#2733])
- **`get_query_store_duration_trend` stops ranking the raw Query Store slab per call** ([#2736], [#1849], [#1665], [#1759])
- **Query Store gets total-CPU and total-duration compose measures** ([#2732])
- **Alert deliveries carry a linked triage page** ([#2710])
- **Postgres targets get the Poison Wait alert** ([#2711])
- **PostgreSQL/Aurora targets get a High CPU alert, backed by a new instance-CPU collector** ([#2719])
- **Fleet overview stops false-flagging active servers as Offline** ([#2699])
- **`pg_wait_stats` stops re-shipping unchanged wait events** ([#2695], [#2691])
- **Collection health distinguishes a collector that has gone dark from one that is actively failing** ([#2694])
- **PostgreSQL wraparound Warning signals autovacuum losing the race, not routine proximity** ([#2693], [#2689])
- **`pg_deadlocks` now works on Aurora and RDS** ([#2692], [#2538])
- **`pg_statement_stats` stops re-shipping idle statements** ([#2691])
- **query_store retires the runaway detector for a flat candidate-plan ceiling** ([#2690], [#2683], [#2685])
- **`plan_correction` binds on compatibility-level-100 databases** ([#2688])
- **`plan_correction` stages its recommendations before touching Query Store** ([#2687], [#2673], [#2680], [#2686])
- **The live Query Store fetch stops joining a view it never reads** ([#2680])
- **The tool now monitors its OWN cost on the servers it watches** ([#2674])

### Changed

- **A census count stated over a population the summary does not republish can no longer be satisfied by assertion** ([#3200])
- **A sizing constant cannot be tightened on a population too thin for a maximum over it to bound anything** ([#3193])
- **Web dashboard custom views now record WHO changed them** ([#2550])
- **Assert collection size with `Assert.Single` and `Assert.Empty` in the source-scanning test pins, clearing all 24 `xUnit2013` analyzer warnings** ([#3132])
- **Lite: log verbosity becomes a setting, and per-database collection timing moves below the default** ([#3104])
- **The fleet card's collection flag is named for what it measures** ([#3098])
- **Darling: per-cycle collector timing telemetry moves to `Debug`** ([#3102])
- **Consolidated the 34 private `ReadRepoFile` helpers in `Darling.Tests` onto one shared `RepoFile` authority, in the `Compile Include` shape #2913 sanctioned, reconciling eight different path-resolution semantics onto one** ([#3090])
- **Rewrote the 33 abbreviated `int` data types across 7 collector and service query strings to `integer`, and moved `CONTRIBUTING.md`'s data-type abbreviation rule from unguarded to enforced in `TsqlConventionGuardTests`** ([#3087])
- **The cross-app guard's C# matcher stops at three syntactic shapes, and that boundary is now a decision with a census behind it** ([#3082], [#3063], [#3067], [#3076])
- **The cross-app guard asks MSBuild for a project's items instead of evaluating MSBuild by hand** ([#3074], [#3065])
- **Recorded the real provenance of `HeaviestHourlyRefreshObservedCeilingSeconds` — the single status-verified run behind it, the ten-run post-boundary distribution (194–594 s, mean 338 s), the transition-run ambiguity, the rule that keeps the unstatused self-metrics series out of it, and why the value is unchanged in both directions** ([#3069])
- **`CrossAppGuardCiGateTests` now sees a cross-app read spelled as a bare directory name** ([#3076])
- **Cross-app guard coverage now sees a SKU's test tree, not just its app directory**
- **The cross-app CI-gate guard now reads project files as well as C# source, so a cross-app source read expressed as an MSBuild `Compile Include` is seen rather than silently missed** ([#3063])
- **Kept `FleetIdentifierScrubTests`' commentary to what maintaining the guard requires, and restated its ordinal ceiling as a property of the shape being matched rather than an inference from past renames** ([#3018])
- **The migration rung bound turns out to bind, and the residual it leaves is a rung SHAPE rather than a missing mechanism** ([#2894], [#2935])
- **A migration rung can no longer trap its own cancellation** ([#2894])
- **#2894's own account of what breaks is corrected** ([#2894], [#2936])
- **`procedure_stats` renders execution-plan XML on one cycle in four instead of on every cycle, which buys us-east-1 roughly 30 s of collection cadence at no CPU cost** ([#2862], [#2843], [#1767], [#2849])
- **The identifier guard now sweeps for a server named by short name and ordinal, and records what it cannot sweep for** ([#2490])
- **The references #2900 left unpinned now use synthetic slugs too** ([#2490])
- **Fixture display names and short-name comment references now use synthetic slugs too** ([#2490])
- **Darling's slice-repair collapse keeps staging the FULL projection, because on PostgreSQL the width is what makes the plan safe** ([#2876], [#2771], [#1907])
- **Lite's server-scoped watermark read stays UNBOUNDED, and the asymmetry with its per-database twin is now documented as deliberate** ([#2800], [#2795], [#2344])

### Fixed

- **Fleet cards now report a PostgreSQL/Aurora target's instance CPU** ([#3267])
- **A server card no longer reports a metric as Healthy when nothing measured it** ([#3271])
- **Azure master-connected size collection no longer dies the moment sys.resource_stats has sibling rows** ([#3262])
- **The Query Store tab's first/last-execution columns had no row-level value pin — the coverage shape that let the #3207/#3221 clock-frame defects ship** ([#3253])
- **A test pinned the Query Store regression row's last-execution display as the raw server clock, which after the #3207 clock-frame work fails on any non-UTC machine and verifies nothing on CI's UTC runners** ([#3248])
- **The PostgreSQL first-target runbook stopped describing a nine-collector product** ([#3247], [#2566], [#2538], [#2692], [#2530], [#3102], [#2625], [#3239])
- **The Darling README's PostgreSQL inventory stopped at 12 of 27 collectors, and now cannot silently do that again** ([#3261], [#3072], [#2603], [#2566], [#2658], [#2661], [#2719], [#2625])
- **MCP `list_servers` no longer names the wrong engine in its version key** ([#3245])
- **The viewer's test-connect request now names the engine it wants probed, and the probe reply's engine facts are pinned as contract** ([#3244])
- **A PostgreSQL target's registry row no longer claims a SQL Server major of 0** ([#3243])
- **An absent optional PostgreSQL extension no longer reads as a permissions failure** ([#3240])
- **The PERMISSIONS hint on the self-hosted PostgreSQL log readers no longer prescribes a role that cannot fix them** ([#3239], [#2566])
- **A full solution Rebuild is warning-clean again** ([#3241])
- **The onboarding probe no longer claims msdb access on Azure SQL Database targets** ([#3237])
- **`upgrade-darling.ps1`'s post-upgrade checklist no longer reads a healthy server count as a failure** ([#3238])
- **The de-skew's accuracy claim is now scoped where a column can outlive a DST transition** ([#3231])
- **Desktop timestamp columns were rendered in the wrong clock frame in both SKUs, in opposite directions, with each SKU correct exactly where the other was wrong** ([#3207], [#3221])
- **Four doc comments stated a column's clock frame incorrectly, one of them appealing to a sibling tab as precedent and one asserting a cross-SKU parity that did not hold** ([#3207])
- **Composed panel queries now assert that every compiled statement binds exactly the parameters its plan predicts and cites every one of them** ([#3211])
- **`ServiceCommandDeadlines.SerialLoopSeconds` keeps its value of 5 and loses the chain bound that justified it** ([#3204])
- **The SQL-text pins over the object-stats, PVS and plan-correction reads are now insensitive to alias qualification and whitespace** ([#3217])
- **`CONTRIBUTING.md`'s build instructions name the deprecated Dashboard and CLI Installer projects at their real paths under `deprecated/`, matching what `README.md` already said** ([#3226])
- **Sixteen fields across five MCP tools returned the monitored server's local wall clock, unmarked, beside naive-UTC fields on the same JSON object** ([#3206])
- **The product version is declared in exactly one place** ([#3222])
- **`StoreSqlClockDisciplineTests` scanned store SQL one string literal at a time, so a predicate assembled by concatenation was split before the discriminator could see it** ([#3223])
- **`DarlingPgReadSqlParsesLiveTests` had a floor of 10 against a population of 49, and one shipped read was outside it** ([#3223])
- **The Query Store liveness touch guard is one constant, six hours wide** ([#3189])
- **`get_default_trace_events` returned `event_time` in the monitored server's local wall clock** ([#3198])
- **Composed-panel event annotations from `default_trace_events` were both selected and plotted in the monitored server's local clock** ([#3198])
- **Adding an Entra MFA server could fail with a Windows broker error that was recorded nowhere** ([#3196])
- **The store disk-pressure check stopped walking the whole store every five minutes to decorate an alert message** ([#3199])
- **`CompressionPhaseGuardMinutes` is a DECLARED four-minute width rather than the light-refresh ceiling rounded up** ([#3188])
- **The two refresh-ceiling constants state one estimator and one closure rule, word for word, and name themselves PREFIX MAXIMA rather than maxima over closed populations** ([#3188])
- **`query_store`'s `sql_duration_ms` was mostly store time, under a column documented as the monitored server's** ([#3192])
- **A PostgreSQL target's permissions are documented as what they actually gate: one grant, two extension install kinds, and the six collectors that need one** ([#3184])
- **The hourly refresh band separates the aggregates whose runs are long enough to collide, inside the minutes it already had** ([#3185])
- **The refresh-slot log-level test's routine-band comment states what the case establishes, instead of narrating a repaired failure** ([#3181])
- **The TimescaleDB hourly refresh grid is re-derived so contention between refresh policies cannot depend on where a view sits in a list** ([#3174], [#3168], [#3166])
- **`HeaviestHourlyRefreshObservedCeilingSeconds` is re-derived from a per-run census, 594 s becomes 896 s, and the watch-line ordering the #3044 signal rests on inverts** ([#3166])
- **Darling: the TimescaleDB job-execution-logging GUC now heals on existing stores instead of only fresh ones** ([#3175])
- **`pg_index_bloat`'s per-index ceiling and cycle budget are now sized against a MEASURED block rate, and it came in at half the assumed one** ([#3164], [#2997], [#3153], [#2673])
- **Alert history states which channel a row is about** ([#3169])
- **The PostgreSQL runbook lists `pg_index_bloat` in its cadence and MCP tool tables** ([#3165])
- **`pg_index_bloat` rotates which indexes it measures instead of re-measuring the largest one every cycle, so the `skipped_reason` it stamps on the rest states a deferral a later cycle actually honours** ([#3153])
- **The measured index-bloat read keeps the latest MEASUREMENT of each index in the window rather than the latest row** ([#3153])
- **`pg_index_bloat`'s per-database rotation cursors are retired when their database stops being enumerated, so they no longer accumulate for the life of the server** ([#3153])
- **`pg_index_bloat` and `pg_index_usage_stats` each state their own index population and name the other's** ([#3158])
- **`get_collection_health`'s `output_finding` no longer explains every zero-row collector as an event collector at rest** ([#3160])
- **PostgreSQL column statistics now say WHICH cause produced an empty result** ([#3154])
- **`pg_write_stats` now collects the checkpointer and background-writer counters on Aurora PostgreSQL** ([#3156])
- **The web dashboard's composer tells a read-only seat that its account is read-only** ([#2550])
- **Every locked-mode restore in CI can fail its step now, and the merge gate can finally see a lock-file mismatch** ([#3143])
- **The Darling viewer's fleet sidebar labelled a PostgreSQL target "SQL Server v0", and the MCP's `list_servers` published the same string** ([#3145])
- **The crude second-opinion parse in `CrossAppGuardCiGateTests` no longer loses project items to an apostrophe** ([#3140], [#3138])
- **The `tempdb Space` alert said "used" while measuring reserved space** ([#3144])
- **The store COPY's start phase — `BeginBinaryImportAsync` awaiting `CopyInResponse`, which is where a store-side lock wait stalls — ran on Npgsql's undocumented 30 s default rather than the collection-sweep deadline, because `NpgsqlBinaryImporter.Timeout` bounds only the row loop below it and the connection's `CommandTimeout` is read-only** ([#3139])
- **The refresh slot envelope is stated against a measured maximum, and `get_store_metrics` says a daily point is a last snapshot** ([#3119])
- **A UI-thread timeout was deciding `Lite.Tests` outcomes in a host with no UI thread** ([#3134])
- **`TimescaleSupport.cs` described the routine refresh band as a universal over a population that grows every hour, in three places** ([#3133])
- **The repo-file reader census in `Darling.Tests` could not see a qualified call to either reader, so its exact-set equality passed with an undeclared LF-reading file in the tree** ([#3129])
- **The gated-live PostgreSQL job no longer reports green having tested nothing when a shared library changes** ([#3116])
- **A PostgreSQL command timeout no longer decides WHICH MACHINE's deadline fired from SQLSTATE `57014` alone** ([#3118])
- **`get_pg_index_bloat` no longer fails outright on an empty index.** ([#3121])
- **`pg_index_bloat`'s size ceiling and work budgets are enforced by selecting the relation `pgstatindex` is applied to, rather than by quals on a `LEFT JOIN LATERAL` to it** ([#3109])
- **Extended #3095's COPY phase axis to the three AWS-API store ingestors (`RdsPlanIngestor`, `RdsCpuIngestor`, `RdsDeadlockIngestor`), so every binary COPY the service performs reports which phase a fault came out of, pinned by a scan asserting both that every COPY site stamps and that no other site does** ([#3111])
- **A collector store-write timeout on a server whose target is PostgreSQL no longer receives the target-read remedy ("shrinking the work") or the monitored target's database name** ([#3111])
- **`RefreshSlotWarningSeconds`' doc comment rejected the 480 s alternative on a miscount of the readings quoted beside it** ([#3107])
- **A collector's store write that fails on a transport fault in the COPY's start phase is re-attempted once on a fresh store connection instead of costing the cycle** ([#3099])
- **A COPY start-phase store-write failure is now distinguishable from a data-phase one, by a caller and not only in a log** ([#3095])
- **`HeaviestHourlyRefreshObservedCeilingSeconds` is re-derived from the clean post-narrowing distribution and now records its own derivation method** ([#3101])
- **Fixed the T-SQL convention guard naming the wrong member in its offender list: a type being constructed, a local variable, or a bare C# keyword at 48 of the 160 T-SQL literals it reads** ([#3094])
- **Hardened the PostgreSQL panel-ownership pin to read its two C# sources through the shared source walker, and to assert the loader-body scan it stands on** ([#3088])
- **Corrected and pinned the counted claims in `Darling/README.md` and `README.md`** ([#3072])
- **Store Job Over Cadence no longer tells an operator to change what the store collects** ([#3060], [#3044])
- **Source-scanning pins now read the shared C# walk instead of dropping lines by comment prefix, which read the body of a block comment as code because this codebase puts no asterisk on the continuation lines** ([#3052])
- **Managed store: pin `lc_messages = 'C'` in `postgresql.conf` (v10 marker-guarded block) so PostgreSQL writes its severity labels untranslated** ([#3053])
- **The PostgreSQL Activity tab's Query Shapes sub-tab assigned a grid row it never declared, so the captured-plans note and grid shared one cell; the Storage tab's predicate panel was numbered two rows low and drew on top of the index-bloat pair** ([#3049])
- **`pg_wait_sampling` and `pg_predicate_stats` were filled only by the Activity tab's load path while rendering on Waits and Storage, so both panels were empty until Activity had been visited — an empty wait-sampling panel reads as a server that offers only the Aurora instrument, and an empty predicate panel hides the index candidates the hypothetical-index action is driven from** ([#3050])
- **The AG page's rollup numbers now say what they count, and the id derivation is shared rather than copied** ([#3045], [#3038], [#3031])
- **The PostgreSQL deadlock panel rendered on the server tab's Vacuum tab while only the Activity tab's load path filled it, so it was empty until Activity had been visited — indistinguishable from a server with no deadlocks** ([#3048])
- **The `pg_deadlocks` schedule comment claimed a cadence match with `pg_plan_capture`; they run five and sixty minutes apart** ([#3047])
- **The WPF Overview's fleet deadlock total now reports how much of the fleet it covers** ([#3042])
- **The live-cleanup ratchet read only the top directory, excluded on a substring where it meant a call, and justified its exemption with a reason that was false** ([#3036], [#3014], [#1776], [#1902])
- **Compression policies are pinned to a phase grid clear of the refresh slots instead of drifting through them** ([#3035], [#3012], [#1778], [#1586])
- **Web dashboard: each fleet rollup tile's number is now programmatically associated with its label and its coverage sub-line via `aria-describedby`, rather than by reading-order proximity alone** ([#3031])
- **Deadlock capture read nothing at all from a managed PostgreSQL target's log, and reported the window as read and empty** ([#3030])
- **The viewer's read deadline is priced against the pool the seat actually has, and the two fleet-wide fan-outs no longer start more reads than it has permits** ([#3016])
- **`get_collection_health` reported what each collector spent and never what it bought: every statistic on a row was a cost figure and the rows count lived on `get_collector_cost`, a different tool over a separate hourly, fleet-wide series** ([#3017])
- **Corrected `PgDeadlockLogParser`'s account of what a truncated deadlock report costs** ([#3009])
- **The hourly continuous-aggregate refreshes re-materialize a day instead of three, and start on distinct minutes of the hour** ([#3012], [#1778], [#1937])
- **The live-cleanup ratchet decided which files to scan, which teardowns were exempt, whether a teardown was converted, and where a teardown's block ended by matching substrings against raw source, so a doc comment could make any of those decisions** ([#3014])
- **Viewer fan-out width declarations are now censused by shape rather than by their `Task.WhenAll`, so a fan-out that fires its reads unawaited is covered** ([#3019])
- **`get_fleet_overview`'s `total_deadlocks` now reports how much of the fleet it covers** ([#3027])
- **The RDS/Performance Insights ingest note no longer describes a source the cycle never opened** ([#3027])
- **Darling's whole-tree guards now run on changes the product-area path filters do not reach** ([#3026])
- **Collector runs that end in a classified fault now record the run's real elapsed time in `collection_log` instead of three literal zeros, so a permission denial, a lock-timeout yield and a missing XE session are no longer stored as instantaneous** ([#3022])
- **`get_collection_health` no longer presents `last_error` as though it were current** ([#3010])
- **Aurora/RDS PostgreSQL log ingestion no longer loses a window of deadlock reports or `auto_explain` plans when a collection cycle fails after fetching it** ([#3008])
- **A retention command was inheriting Npgsql's 30 s default, and the pins that exist to say so could not see it** ([#2874])
- **The retention-convergence live test no longer reads TimescaleDB's scheduler running the armed policy as a convergence bug** ([#2937], [#1889], [#2143], [#2818], [#1680])
- **The command plane, the two command handlers that read the store, the Query Store backfill and the config reload beacon now carry explicit deadlines - and two of them had an enclosing budget that never bounded them** ([#2874], [#2148], [#2795], [#2344], [#2796], [#2888], [#2901])
- **Lite's shared-surface loads are now ordered, so the earlier of two overlapping reads can no longer land last** ([#2933], [#2929])
- **#2874's three straggler regimes now carry FIVE derived deadlines, and the `hypopg_reset()` one goes UP where the others go down** ([#2874], [#2888], [#2901], [#2826], [#2882], [#2940], [#2819], [#2786], [#2928])
- **The collection-sweep deadline's floor no longer credits `CommandTimeout` with bounding a phase it cannot reach** ([#2874], [#2819])
- **A comment that quotes the deadline can no longer certify an untimed collection-sweep site, and the fixture that was supposed to witness that no longer passes for the wrong reason** ([#2874])
- **The collection-sweep census can now see a construction that carries its namespace, and it records which of its sites address the store rather than a monitored target** ([#2874], [#2928], [#2901])
- **A deadline written only inside a comment no longer reads as a real one on the MCP and web read surface** ([#2874])
- **The Query Store backfill's per-server gate now stays closed while an abandoned slice is still running, and releases on every outcome path** ([#2165], [#2148], [#2874])
- **The viewer scoped-paint pin reads the shared brace walk, so no copy of it is left to drift** ([#2927], [#2929])
- **A `CommandTimeout` spelled in a COMMENT satisfied `AlertPassCommandTimeoutTests`' deadline check, so an untimed alert-pass command read clean** ([#2942], [#2874])
- **The control-plane reload watermark now records the version that was actually applied**
- **The Lite MCP CPU and Default Trace reads now window in the offset of the server they were asked about** ([#2967], [#1262], [#2511])
- **The latest-CPU reads no longer scan a server's whole retained history to return one row** ([#2964])
- **A future copy of the latest-CPU read can no longer drift back to the old shape unseen** ([#2973])
- **`/api/ping` answered `"ok"` while collection had never started; it now reports the collector's real state** ([#2953], [#2936], [#2117])
- **The last two command-timeout pins stopped certifying a site from a NEIGHBOUR's deadline** ([#2972], [#2938])
- **A control-plane reload whose store read failed no longer discards the operator's change** ([#2951], [#2936])
- **Role provisioning no longer writes a `statement_timeout` it failed to read, and no longer swallows cancellation to do it** ([#2952], [#2931], [#2918])
- **Every store command on the startup path now carries an explicit deadline, and this is the one regime in the sweep whose value goes UP** ([#2874], [#1772])
- **`memory_pressure_events.sample_time` was written in the monitored server's LOCAL wall clock while both of its readers assume naive UTC, so the memory-pressure series sat one UTC offset away from every other lane** ([#2932], [#1262])
- **The horizon convergence is now pinned to name `config` and nothing else, so it cannot arm a retention policy** ([#1937], [#1680], [#1877])
- **A second migrator no longer dies undiagnosed waiting on the migration advisory lock, and no longer dies at all when it had nothing to apply** ([#2894], [#2888], [#2920], [#2874])
- **The alert pass's CPU read now carries the deadline the rest of the pass already had, and the guard that declared the pass clean can no longer miss a site by living in the wrong file** ([#2874], [#2826], [#2882], [#2928], [#2786])
- **Collection Health counts an abandoned cycle from the row, so a window that predates [#2803] no longer reports clean** ([#2926], [#2804])
- **A transient failure of any of the three collection-blocking startup steps no longer ends collection for the life of the process** ([#2936], [#2935], [#2038])
- **A viewer surface that carries every scope's answer no longer paints an answer read for a scope the operator has since left** ([#2924], [#2901], [#2907], [#2910])
- **The collection sweep's store reads and writes now carry an explicit deadline - including the binary COPY, a construction shape this sweep could not see** ([#2874], [#2810], [#2871], [#2882], [#2888], [#2901], [#2819], [#1581], [#2170], [#2822], [#1772], [#2913])
- **`ViewerFleetTimerGuardTests` no longer passes when the fleet-timer fan-out moves BELOW the tab early-return, which is the regression [#2907] named** ([#2923], [#2913])
- **The source-walking idiom behind five test pins no longer blanks interpolated-string holes, so a call written inside an interpolation is visible to the scans built on it** ([#2913])
- **The compose `statement_timeout` now reaches the live roles on a reload, instead of only on a service restart** ([#2918], [#2917])
- **A control-plane reload no longer drops the compose `statement_timeout` knob on the floor** ([#2917], [#2357], [#2918])
- **The per-database phase split is now printed on the fault path, which is the one case it was added to explain** ([#2896], [#2855], [#2811], [#2851], [#1875])
- **The viewer's two fleet timers can no longer stack store reads, and the Overview no longer double-fires one of them** ([#2907], [#1566], [#2901])
- **Deferred plan and text fetch debt is no longer paid by whichever collector visits the database next** ([#2902], [#2312], [#2847], [#2841], [#2860], [#2898])
- **The server-scoped watermark read no longer runs for a collector whose own cycle throws the answer away** ([#2797], [#2795], [#2796], [#2860])
- **The IL-scan idiom behind five reachability pins is now a real instruction decoder, and can see generic callees** ([#2898], [#2890])
- **Every command on the Darling service's MCP and web read surface now carries an explicit deadline** ([#2874], [#2871], [#2736], [#2826], [#2918])
- **Every command in the Darling viewer now carries an explicit deadline** ([#2874], [#2882], [#2888], [#1566], [#2871])
- **Every command in the Darling storage layer now carries an explicit deadline** ([#2874])
- **The analysis pass's last two commands now carry an explicit deadline, and the pin can finally see the shape that hid them** ([#2874], [#2871], [#2810])
- **The retention live tests now carry their evidence into the failure message** ([#2818], [#1564])
- **A healthy server mid-sweep is no longer painted with the red Offline overlay** ([#2794], [#2792], [#1562], [#2473])
- **Abandon-path forensics no longer record a session id of 0** ([#2884], [#2673])
- **Every read in the alert evaluation pass now carries an explicit deadline** ([#2874], [#2826], [#2810], [#2871])
- **A held Ctrl+V during a busy clipboard no longer spawns several 'Pasted Plan' tabs from one keypress** ([#2870], [#2837])
- **Retention held by the rollup-coverage gate is now visible, instead of reading as a healthy job** ([#2813], [#2809], [#2136])
- **Every command in the analysis pass now carries an explicit deadline, not just the fact collector's** ([#2871], [#2810], [#2820], [#2826], [#2827], [#2344])
- **The slice-repair survey no longer holds one read lock across its entire multi-phase read, and can now actually be cancelled** ([#2761], [#2748], [#2465], [#2770])
- **The one-time Query Store slice repair no longer runs out of memory on a large store** ([#2771], [#2770], [#1912])
- **The Plan Viewer's inner tab handler is now subscribed once, not re-subscribed on every reopen** ([#2828])
- **The plan-regression analysis read had no command deadline, so it inherited one nobody chose** ([#2810], [#2827], [#2826], [#2809], [#2344])
- **Managed-store Postgres sizing now re-derives when the HOST changes, not only when the formula does** ([#2845], [#1559])
- **Four enumerated-path phase stamps were skipped whenever the phase they timed threw** ([#2854])
- **`Collector Cost Regression` compared the day's TOTAL cost, so a cadence recovery read as a regression** ([#2846], [#2768])
- **PR CI now runs `Lite.Tests` when only Darling changes.** ([#2839])
- **Plan Viewer clipboard retry no longer blocks the UI thread while the clipboard is busy** ([#2837], [#2833])
- **Plan Viewer paste no longer crashes the app when the clipboard can't be opened** ([#2833])
- **The plan-regression query, ~90% of all store statement cancellations, is no longer sorted globally** ([#2827])
- **A fact collector that could not run is no longer indistinguishable from one that found nothing** ([#2826])
- **FinOps "View Stored Plan" no longer crashes the whole Darling Viewer** ([#2825])
- **The `query_store` fetch split lines now report the probe's INPUT size, not just the ids it attempted** ([#2823], [#2819], [#2822])
- **The Query Store plan and text fetches stop opening their own store connections** ([#2819], [#2811], [#2796])
- **Web dashboard: a sparse trend chart spanned only its data burst, so an old incident read as current** ([#2802])
- **The `io_latency` baseline no longer times out, and eight other metrics got faster with it** ([#2820])
- **A `query_store` fetch probe that throws no longer reports `probe:0ms` and hands its whole cost to the `other:` residual** ([#2816])
- **Headless web Alert History: a tray-only alert now reads "Logged", not a misleading "Sent"/"Not sent"** ([#2814], [#2781])
- **Web dashboard: the server Daily Summary showed "Health: NaN"** ([#2807])
- **A wall-clock-budget-abandoned cycle no longer records as `SUCCESS`** ([#2801])
- **Web dashboard: a pinned notebook panel cell showed no "Pinned" badge in the read view** ([#2788])
- **The server-scoped watermark read now carries #2344's chunk-pruning bound** ([#2795])
- **query_store plan and text fetch: force HASH JOIN so the in-memory Query Store TVF is read once** ([#2791])
- **Web dashboard: the composer's partial-window notice read "1 days" on a one-day store** ([#2785])
- **PostgreSQL statement text was never stored on ANY Aurora server, because the Aurora fetch does not deduplicate `queryid`** ([#2786], [#2651], [#2284])
- **Web dashboard: an offline server no longer shows a green "Collectors OK"** ([#2779])
- **Darling Viewer and Dashboard: an offline server's Overview card no longer shows a green "Collectors OK"** ([#2784], [#2779])
- **Web dashboard: an over-long time range showed a raw API validation error instead of a range hint** ([#2780])
- **Web dashboard: Alert History dropped the meaningless "tray" delivery channel** ([#2781])
- **`query_store` plan and text fetches: an EXPLICIT command timeout on both halves, replacing two defaults nobody chose** ([#2776])
- **query_store fetch failures re-paid full cost every cycle, forever** ([#2776])
- **Web dashboard: server cards got NARROWER as the window got WIDER, until the stat tiles clipped** ([#2772])
- **Web dashboard: a chart panel with a single collected time-bucket read as "not enough data points" beside siblings that plotted** ([#2773])
- **`/api/triage`: store self-alerts no longer render three "Could not resolve server" errors** ([#2768], [#2710])
- **Lite: a store with a large pre-#1907 Query Store backlog could not start at all** ([#2748])
- **`plan_correction` collector: `OPTION(RECOMPILE)` restored to the payload's two statements** ([#2766], [#2760], [#2764])
- **`plan_correction` collector: the shipping SELECT still joined the Query Store catalog views live** ([#2764], [#2673])
- **query_store collector's open-interval re-read cost outgrew the window it was tuned against, fleet-wide** ([#2759])
- **Darling Viewer: the Fleet Health overview tab's server total wobbled against the sidebar's stable count, and could claim a false all-clear** ([#2753])
- **MemoryPressureEventsCollector threw "Arithmetic overflow error converting expression to data type int" on every collection, minutes after #2749's fix deployed** ([#2755])
- **Darling Viewer: Availability Groups grid sorted numeric columns as text** ([#2757])
- **The CPU chart degrades to disconnected dots after roughly an hour, on every server** ([#2749])
- **query_stats grouped by object_name no longer collapses every ad-hoc statement into one null-labeled row** ([#2737], [#1568])
- **A read-only Viewer seat could silently discard local Default Time Range / Auto-refresh interval changes** ([#2715])
- **The plan-correction collector can no longer run minutes on a monitored server** ([#2673])
- **The runaway plan-fetch cap now HOLDS instead of oscillating off** ([#2685])
- **Heavy collectors can no longer occupy a monitored server for minutes** ([#2673])
- **The Query Store collector stops grinding on a plan-churning tenant** ([#2683])
- **Deadlocks collection no longer fails on every Azure SQL DB target** ([#2681])
- **PMLite lost Azure SQL DB deadlock and blocked-process capture until an app restart** ([#2677])
- **Query Store plan and text capture decompressed each item up to 3x per cycle** ([#2675])
- **`upgrade-darling.ps1` crashed before copying anything on an install with no prior rollback backups** ([#2671])

## [3.6.0] - 2026-08-27

Full entries: [docs/changelog/3.6.md](docs/changelog/3.6.md)

### Added

- **Store exposure admits both roles at once** ([#2665])
- **PostgreSQL I/O and database trends** ([#2663])
- **The first PostgreSQL trend reads** ([#2663])
- **PostgreSQL deadlocks, not just the count** ([#2661])
- **PostgreSQL configuration, and what changed in it** ([#2658])
- **Test an index from the predicate grid** ([#2612])
- **Azure SQL DB now reports every database's size, not just the connected one** ([#2643])
- **Mark rows in grids** ([#2645])
- **Test a PostgreSQL index without building it** ([#2612])
- **The last eight PostgreSQL collectors are readable outside the Windows Viewer** ([#2629])
- **Sampled waits and per-query OS CPU are readable from the MCP and the web dashboard** ([#2629])
- **Self-hosted PostgreSQL now answers "which queries cost the most"** ([#2625])
- **Permanent-gap messages now name the collector that DOES answer the question** ([#2625])
- **PostgreSQL plan capture reaches Aurora and RDS** ([#2538])
- **`get_pg_plans`: the plan itself, not a pointer to one** ([#2567])
- **PostgreSQL execution plans, captured and redacted** ([#2566], [#2538])
- **Which PostgreSQL columns are filtered on, and where the planner is wrong about them** ([#2603])
- **OS CPU and disk per query on PostgreSQL** ([#2603])
- **PostgreSQL wait analysis stops being Aurora-only** ([#2603])
- **Three PostgreSQL collectors could not say which database their rows described** ([#2599])
- **PostgreSQL index bloat, MEASURED rather than estimated** ([#2561])
- **What is resident in PostgreSQL shared buffers, with the join every published example gets wrong** ([#2544])
- **PostgreSQL replication connections, with the measure that actually catches a stalled standby** ([#2544])
- **PostgreSQL column statistics, with the customer data deliberately left behind** ([#2543])
- **PostgreSQL lock state by mode, type and relation** ([#2544])
- **PostgreSQL extension availability: the third capability axis, and the only actionable one** ([#2545])
- **PostgreSQL write-side collection: checkpoints, background writer and WAL** ([#2544])
- **A captured PostgreSQL plan is an orphan without `%Q`, so plan-capture readiness now checks for it** ([#2538])
- **The web session cookie can now carry WHO is holding it, and the signature covers it** ([#2550])
- **PostgreSQL targets now say whether plan capture is even possible** ([#2564])
- **HTTPS for the web dashboard, so the access token stops crossing the LAN in the clear** ([#2562], [#2550])
- **The sessions behind a pinned xmin horizon, and the ones that only look like it** ([#2540])
- **`idle in transaction` is not automatically starving your vacuum, and this is the finding the read exists to make** ([#2540])
- **A held horizon is a share of samples, not a flag** ([#2540])
- **No statement text is stored, and that is a decision rather than an omission** ([#2540])
- **The permission that gutted this feature silently, caught before it shipped** ([#2540], [#2542])
- **The denominator, and a defect only a live run could find** ([#2540], [#2508])
- **PostgreSQL index usage, with the half that decides whether an index can actually go** ([#2541], [#2530])
- **The scan count only a monitoring store can produce** ([#2541])
- **PostgreSQL table bloat: the damage, next to the cause chain we already collected** ([#2542])
- **The bloat measurement decision, settled by measurement rather than by reasoning** ([#2542])
- **The bloat estimate is suppressed, not captioned, when its inputs cannot be trusted** ([#2542])
- **The permissions trap that would have shipped a tool telling everyone to VACUUM FULL everything** ([#2542])
- **A Storage tab on both front ends, and neither read is MCP-only** ([#2541], [#2542])
- **Version floors for both collectors, measured against six live majors rather than read from documentation** ([#2541], [#2542])
- **Both new collectors are fan-outs, and the cost of that was budgeted rather than discovered** ([#2541], [#2542])
- **A fourth kind of nothing: `precondition`, for a setup step somebody can actually change** ([#2546])
- **The WPF viewer renders PostgreSQL tabs at a PostgreSQL target, and stops rendering nineteen empty SQL Server ones** ([#2530], [#2547])
- **The two Aurora-only PostgreSQL panels are SHOWN on stock PostgreSQL, not hidden** ([#2530])
- **One copy of the PostgreSQL read SQL, shared by the MCP surface and the desktop viewer** ([#2530])
- **The viewer's server rows carry the engine discriminator, from BOTH server reads** ([#2530])
- **PostgreSQL temp-file spills, cache hit ratio, deadlocks and the rollback ratio - one collector, one read** ([#2539])
- **A PostgreSQL statistics reset is reported AS a reset** ([#2539])
- **The web dashboard renders PostgreSQL tabs at a PostgreSQL target, and stops rendering twelve empty SQL Server ones** ([#2530], [#2539], [#2532])
- **The store records what ENGINE a monitored target is** ([#2530])
- **Ten viewer surfaces the browser and MCP could not reach now have read endpoints** ([#2484])
- **An in-place upgrade can now remove the files the new build stopped shipping, and always names them ([#2529])** ([#2185], [#2525])
- **A supported in-place upgrade script, because the step that had no script was the one accumulating 5.48 GB** ([#2525], [#2185])
- **The web dashboard gets a per-query drill-down, which is what makes `get_query_trend` reachable there at all** ([#2520], [#2353])
- **The analysis family can be anchored at a past window too, and `analyze_server` refuses to persist when it is** ([#2506], [#2495])
- **`analyze_server` accepts the anchor and does not persist an anchored run** ([#2506])
- **Every windowed read can be anchored at a past incident, not just at now** ([#2495])
- **Five ready-made dashboards, so a new user's Custom Views page is not an empty one** ([#2480], [#2437], [#1563])
- **The web dashboard's server page gets the viewer's tabs: twelve sections, 61 of the 82 served reads, and a time range** ([#2475], [#2422], [#1949])

### Changed

- **The SQL-Agent collectors no longer gate on msdb access, so running the GRANT we advise actually does something** ([#2559])
- **`get_pg_top_queries` and `get_pg_blocking` return their int8 IDENTITIES as strings, because a JSON number was rounding them** ([#2548])
- **Fifteen reads stopped answering "nothing happened" and "nothing was collected" with the same sentence** ([#2485])
- **`get_collection_health` serves what a HEAVY run costs, not only what runs cost on average** ([#2460], [#2459], [#2446])
- **`sweep_pressure` answers the single-sweep question as well as the sustained one** ([#2446], [#2296])
- **Lite's portable ZIP is self-contained, which HALVED it** ([#2501], [#2489], [#2499])

### Fixed

- **Ten PostgreSQL MCP tools were never registered with the host, so no agent could call them** ([#2659])
- **PostgreSQL 18 silently lost every I/O byte figure, and the estimate it replaced was off by an order of magnitude** ([#2655])
- **A PostgreSQL column that the server's VERSION removed read as a missing measurement** ([#2653])
- **Self-hosted PostgreSQL had no query TEXT, so `test_hypothetical_index` could never work there** ([#2651])
- **Azure SQL DB captured almost no deadlocks, and could not say so** ([#2641])
- **Lite forgot the time range you picked** ([#2640])
- **Lite's Database Sizes grid looked broken on Azure SQL DB** ([#2640])
- **A missing-extension message said CREATE EXTENSION without naming which database** ([#2638])
- **`get_index_usage` hid whole databases behind a fixed 200-row cap and never said so** ([#2636])
- **RDS plan capture reported SUCCESS "no new plans" when the AWS call was DENIED** ([#2633])
- **`IsAwsRds` was never set on a PostgreSQL target, so plain RDS PostgreSQL could never capture plans** ([#2633])
- **A summary count taken over a capped result read as a fact about the server** ([#2629])
- **`pg_wait_sampling` excluded only `Activity`, so `Client`/`ClientRead` was 100% of the profile** ([#2630])
- **An unknown column `format:` rendered as raw text instead of failing** ([#2629])
- **`--enable-web` on a non-Windows host reported the wrong reason and hid the path that works** ([#2626])
- **A per-database collector that failed in SOME databases reported SUCCESS with no note** ([#2623])
- **Three PostgreSQL collectors wrote fewer payload values than they declared columns** ([#2599])
- **`pg_index_bloat` had never returned a single row** ([#2617])
- **The README documented an upgrade script that no released build contains** ([#2593])
- **Tag management had no discoverable entry point in the Darling Viewer, and none at all on a viewer with no servers** ([#2595])
- **The plan-capture remedy recommended the one setting measured to be catastrophic** ([#2565])
- **`extension_available` said no on every PostgreSQL server in existence, including ones actively running auto_explain** ([#2564])
- **The Viewer's connection self-test probed a different connection string than startup opens** ([#2578])
- **A gated-off collector logged a fake SUCCESS on SQL Server targets** ([#2579])
- **Rollback backups kept an unhardened copy of `darling.json`** ([#2574])
- **The upgrade-path gate was climbing from a store nobody has** ([#2572])
- **`get_running_jobs` still claimed a server had no jobs running when we were never allowed to look** ([#2559])
- **A wildcard `listen` refused every LAN request with a 400 — which is what the compose distribution ships** ([#2569])
- **Editing a REGISTERED server in `darling.json` was silently ignored, and the warning about the adjacent case made that worse** ([#2552], [#2252], [#2254], [#2158])
- **`get_pg_top_queries` had never returned a row on any engine: its SQL did not parse** ([#2554], [#2219])
- **A read that throws now gets the same honest capability answer as one that comes back empty** ([#2554])
- **On a PostgreSQL target, 55 more reads stopped telling you to go and check a collector that will never run** ([#2532], [#2511], [#2530])
- **Eight PostgreSQL reads told a SQL Server target its vacuum was healthy, and stock PostgreSQL was told to go and check a collector that will never run** ([#2532], [#2530], [#2518])
- **The Overview lanes skewed apart when one lane's numbers got long** ([#2533])
- **Lite's Overview lanes carried the same latent gutter misalignment as [#2533], now fixed there too** ([#2535])
- **A SQL Server read aimed at a PostgreSQL target says so, instead of telling you to check collection** ([#2530], [#2511])
- **A fresh store never seeded, so the control plane silently overrode `darling.json`** ([#2524])
- **The `tempdb Space` alert measured distance to the next autogrow, not distance to the ceiling** ([#2515], [#2512])
- **Nineteen Darling reads (eighteen on Lite) told an Azure SQL Database user to go and start a capture that cannot exist there** ([#2511])
- **Azure SQL Database gets tempdb monitoring back, and the reason it lost it was checkably false** ([#2512], [#2515])
- **Lite's .NET runtime prerequisites were documented against the wrong artifact** ([#2489], [#2481])


## [3.5.0] - 2026-08-19

Full entries: [docs/changelog/3.5.md](docs/changelog/3.5.md)

### Added

- **Alert on database file SIZE growth, graded per server** ([#2349])
- **`Database` and `LastEventUtc` on the incident projection** ([#2361])
- **Every fingerprinted alert now carries a monotonic occurrence total, not just blocking and deadlocks** ([#2362])
- **`--harden-files`: re-apply the secret-file ACLs from an elevated prompt** ([#2352])
- **Declared peer stores: a Darling MCP server that names its siblings instead of answering "unknown server"** ([#2339])
- **`cpu_attribution` on the top-CPU rankings: what fraction of the box the ranking explains** ([#2320])
- **`get_query_store_health`: the MCP read for the new collector, both SKUs** ([#2319])
- **Per-database Query Store health: a new `query_store_health` collector, both SKUs, both stores** ([#2319])
- **V75 gives plan CONTENT its own retention horizon, because the fact-coupled one cannot bound a young store** ([#2316])
- **The generic webhook can now hand automation the alert's structure: `{{context_json}}`, `{{incidents_json}}` and `{{dedup_key}}`** ([#2302])
- **get_collection_health now carries a sweep_pressure verdict, so half-rate collection stops hiding behind 40 healthy collectors** ([#2296])
- **V74 stores query_store statement text out of the sorted stream, with its own watermarked fetch, prune and Viewer probe** ([#2150])
- **The query_store collector can now resolve statement text through a separate watermarked fetch, so the text stops being sorted** ([#2150], [#2210], [#1556], [#2164])
- **The darling.json reconciliation now tells "never registered" from "deliberately removed"** ([#2258], [#2252], [#2280])
- **The store's scale test now reports what the compression job DID, not only how long it took** ([#2266], [#2136], [#1902])
- **PostgreSQL statement text is stored, so `get_pg_top_queries` returns something readable** ([#2219])
- **`add_servers` refuses a registration whose connection lands in a database already monitored** ([#2280], [#2228], [#2220], [#2277], [#2158], [#2218])
- **Add Server warns when a SQL-auth password may not be decryptable by the service** ([#2279], [#2255], [#2273])
- **A skipped database-state maintenance cycle now says so, once** ([#2266], [#2189], [#2203])
- **`server_id` identity now carries engine and port, without re-keying anything that exists** ([#2218], [#2213], [#2158])
- **A registration connected to a database it does not name now says so** ([#2228], [#2220], [#2158], [#1535])
- **Editing a server's address keeps its identity, so its collected history stays attached** ([#2158], [#2228], [#2218])
- **The database-state heal tests no longer assume a best-effort maintenance cycle always runs** ([#2266], [#2189], [#2203])
- **`get_top_queries_by_cpu` can rank a procedure's dynamic SQL as ONE statement: `group_by: "host_object"`** ([#2235], [#2012])
- **The Query Store tick and the Query Store backfill no longer run against one server at the same time** ([#2165], [#2058], [#2148], [#1960])
- **A credential the service cannot decrypt now says why, once, instead of `Key not valid for use in specified state` every 60 seconds forever** ([#2255], [#2252])
- **The incident readers take an alert's Dedup Key: `get_deadlocks` / `get_deadlock_detail` / `get_blocking` accept `dedup_key`** ([#2159], [#1140], [#2138])
- **An `initdb` loader failure now names WHICH binary could not load** ([#2185], [#2186])
- **A headless host can register a monitored server: `--add-server`** ([#2256], [#2252], [#2254], [#2158], [#2097])
- **Darling captures PostgreSQL blocking chains** ([#2213])
- **Every PostgreSQL migration rung is now pinned identical to the generated schema**
- **The viewer's store-schema probe is pinned to its reader's arity**
- **Darling monitors PostgreSQL and Amazon Aurora PostgreSQL** ([#2213])
- **The store can keep plan XML readable over plain SQL: `plan_xml_compression` ([#2171], asked for by @argpna)**
- **Alerts carry a monotonic occurrence total per incident, not just a rolling-window count** ([#2216])
- **A failing forced plan now alerts** ([#2157])
- **Every previously-hardcoded alert threshold is now a real setting** ([#2107])
- **Per-database collection timing now separates server think-time from row streaming** ([#2164])
- **The two collector memory bounds are now operator knobs** ([#2164], [#2170])
- **The Query Store backfill has an off switch** ([#2167])
- **The store measures its own background jobs** ([#2136])
- **The store alerts when its own background jobs outgrow their schedule** ([#2136])
- **The #2136 capacity model is now proven by a synthetic scale test, not asserted from one observation**

### Changed

- **The compose `statement_timeout` is a store setting instead of a hardcoded 15s** ([#2357])
- **MCP tool results serialize compact instead of pretty-printed** ([#2350])
- **The test suites run under Microsoft.Testing.Platform instead of VSTest** ([#2347])
- **The query_store per-database log split now names the plan-XML and text fetches** ([#2312])
- **Query Store statement text is now resolved from `collect.query_store_text`, and the separate fetch is ON** ([#2150], [#1565])
- **Lite's Query Store prune names the state keys it retired, like Darling's** ([#2205], [#2195])
- **Query Store plan XML is stored once per plan instead of re-shipped every pass, and moves into the shared plan dimension** ([#2164], [#2210])
- **Query Store collection stops re-shipping plan XML the store already holds** ([#2164])
- **A deliberately offline database alerts once instead of every cooldown** ([#2166], [#2203])
- **Force-plan findings carry a machine-first verdict for MCP consumers** ([#2138])
- **Plan-regression detection scores CPU as the primary signal and gates on absolute spend** ([#2138])
- **The force-plan recommendation now warns when the regressed query is parameter-sensitive** ([#2138])

### Fixed

- **Server Inventory's `Last Updated` was a config-snapshot time wearing a freshness label** ([#2359])
- **get_query_store_top reported a window it could not serve** ([#2364])
- **Server Inventory's Last Updated looked broken on decommissioned servers** ([#2359])
- **Extended-length paths no longer slip past the install-location guards** ([#2348])
- **get_query_trend silently truncated any window past 4 days** ([#2353])
- **The store's scale test no longer asserts that TimescaleDB compresses more rows in more time** ([#2266], [#2136])
- **The service now says WHY it cannot start when it is installed somewhere its own account cannot read** ([#2185])
- **The per-database watermark read was an unbounded MAX over every chunk in retention** ([#2344])
- **Aurora detection called a catalog lookup instead of the function, and silently disabled two collectors on real Aurora** ([#2340])
- **query_store's plan/text fetch is activity-driven: the store is the watermark** ([#2312])
- **Darling Viewer crash on Queries -> Query Store by Duration** ([#2181], [#2331])
- **The store self-metrics sweep's ~5-a-day "Exception while reading from stream" ERRORs were command timeouts in a network-fault costume** ([#2317])
- **Gapped charts no longer bury their neighbours under opaque black fill** ([#2324])
- **The plan fetch's adaptive candidate sizing is finally wired** ([#2312])
- **max_cpu_ms can no longer masquerade as a this-window number** ([#2235])
- **The query_store collector stops re-aggregating the open interval every cycle** ([#2312])
- **FinOps column filters no longer follow you to the next server** ([#2306])
- **A clean service stop no longer reads as seven faults** ([#2299], [#2294])
- **Index Analysis could show 2,000+ recommendations and an empty Recommendations grid** ([#2300])
- **The MCP host can read its server registry again - by not reading it** ([#2298], [#2293])
- **The FinOps provisioning verdict said UNDER_PROVISIONED for every server alive** ([#2246], [#2150])
- **Half of every delta collector's output was a zero that never happened** ([#2233], [#2234])
- **`tempdb_stats` failed forever on Azure SQL Database, and could never have succeeded** ([#2150])
- **The installer's mapped-drive refusal failed open when WMI was unavailable** ([#2201], [#2187])
- **The service now names the `darling.json` servers it is not monitoring** ([#2254], [#2252], [#2158], [#2256])
- **Microsoft Entra MFA can actually connect - Lite** ([#2184])
- **Dropped databases no longer leave Query Store collection state behind forever - both apps** ([#2188], [#2191])
- **A managed-Postgres bootstrap failure now says what went wrong instead of printing a Win32 number** ([#2186], [#1738])
- **A missing store credential no longer reads as a first run after a bootstrap has already failed** ([#2197], [#2186])
- **A database observed mid-restore no longer learns RESTORING as its expected state and then alerts forever for being healthy** ([#2189], [#2203])
- **Lite's database-state alert now goes quiet after announcing a chosen state, like Darling's** ([#2203], [#2166])
- **The doc-comment hygiene pin now catches a stacked summary whose first block is never closed** ([#2190])
- **The installer refuses an install directory the service could never read** ([#2187])
- **The store's compressed plan format is now documented** ([#2171])
- **tempdb no longer reports more than 100% used in FinOps Database Sizes** ([#2169])
- **Backing out of Custom Range no longer strands an open calendar - both apps** ([#2154])
- **One wedged background task can no longer stop collection - in either app** ([#2148])
- **The retention purge retries once on a deadlock instead of wasting the cycle** ([#2143])
- **Remote viewers can finally use the exact connection string `--print-viewer-connection` prints** ([#2117])
- **`--collapse-legacy-slices` no longer dies at TimescaleDB's decompression rail on compressed chunks** ([#2105])
- **A Query Store member whose catch-up window can't fit the command timeout now shrinks it until one does** ([#2111])
- **`--collapse-legacy-slices` no longer dies with "Exception while reading from stream" on a store fresh off a large catch-up** ([#2105])
- **Query Store catch-up can no longer spiral a big database into permanent timeout** ([#2102])
- **Multi-incident alerts render as labeled, self-contained units instead of one flat fact list** ([#2108])
- **Every database-scoped alert exposes a discrete Database fact** ([#2109])
- **Query Store backfill yields to the live path on contended replicas** ([#2111])
- **Version stamps are single-sourced from `<Version>`** ([#2113])
- **Lite no longer crashes with an uncatchable stack overflow on Queries > Query Store by Duration** ([#2114])
- **Upgrading a 3.3.0-era Darling store to 3.4.0 no longer fails the migration ladder** ([#2119])
- **The upgrade path itself is now release-gated** ([#2119])
- **Store Disk Pressure no longer re-notifies every cooldown while free space sits at an unchanged level** ([#2101])
- **Compression Job Stuck no longer false-alarms on a job it caught mid-run**
- **The Job History tab speaks display names** ([#2126])
- **The long-query completion XE session actually gets created now** ([#2129])
- **`--collapse-legacy-slices` narrows its slice instead of dying when a day does not fit the statement timeout** ([#2105])
- **Query Store collection no longer has a fixed cost that big catalogs cannot pay** ([#2133])
- **`--collapse-legacy-slices` runs ~40% fewer window scans and narrates its progress**

## [3.4.0] - 2026-08-06

Full entries: [docs/changelog/3.4.md](docs/changelog/3.4.md)

### Important

- **Darling keeps hourly-grain history 90 days instead of 21, and existing stores are moved to the new horizon automatically** ([#1937], [#1939])
- **Lite repairs its stored Query Store history once, on the first launch after upgrading - expect a few extra seconds and a slightly larger archive** ([#1912], [#1907])
- **OPERATOR ACTION, Darling only: run `--collapse-legacy-slices` PROMPTLY after upgrading, and the sooner the better** ([#1912], [#1759], [#1907])
- **Version Store (PVS) pressure alert in both apps** ([#1984], [#1951])
- **Lite's data now lives OUTSIDE the install directory - and on any version older than this one, upgrading with `Setup.exe` destroys it** ([#1832])

### Added

- **Stable releases now ship the Linux artifacts**
- **OPTIONAL, Darling only, for stores that collected before this release: `--recompress-plan-dim` converts the plan dimension's existing text rows to the gzip form new plans already use** ([#2076], [#2069])
- **The store measures itself: an hourly self-metrics sweep records every hypertable's size and compression, the payload dimension tables, and the whole store into `collect.store_metrics`, with a `get_store_metrics` MCP/REST read for capacity forecasting** ([#2068])
- **New execution plans are stored gzip-compressed, shrinking the store's single largest object by a projected further ~35% with no change to what is captured** ([#2069], [#2068], [#2071])
- **The web fleet gains tags: coloured pills, a group-by-tag tree, and tag-name search** ([#2020])
- **Darling runs on Linux: a compose file for the whole stack, a systemd walkthrough, and the store connection string as a secret reference** ([#1804])
- **Enabling a default-OFF collector fleet-wide actually takes effect now, and the schedule editor stops showing it as already-enabled** ([#2064], [#2061])
- **The Viewer's Settings dialog keeps Save and Close pinned below the scroll** ([#2063])
- **Findings now keep their evidence: the drill-down behind every persisted finding survives read-back, and `get_analysis_findings` can return it** ([#2060])
- **Darling backfills the Query Store history the live path never takes — newest-first, on its own tick, never past the raw tier's horizon** ([#2022], [#1960], [#1556], [#1937], [#2058])
- **The "server silenced" muted-bell reaches Lite's sidebar too** ([#2031])
- **The Darling service builds for Linux on every PR and ships a nightly container image** ([#1804])
- **Network exposure works in a container: the bind ladder's managed-mode gate extends to \`managed OR containerized\`** ([#1804])
- **Secrets without DPAPI: every plaintext secret slot also takes an \`env:NAME\` or \`file:/path\` reference** ([#1804])
- **A headless \`--test\` flag on the Darling Viewer: the #1954 connection self-test without the UI** ([#2005])
- **\`get_pvs_stats\`: the ADR persistent version store gets a browsable MCP reader on both hosts, plus the web mirror** ([#2029], [#2028], [#2018], [#1984])
- **Automatic plan correction is finally agent-readable: a \`get_plan_corrections\` tool on both MCP hosts, the web read mirror, and custom-view measures** ([#2028])
- **A silenced server now LOOKS silenced - a muted-bell indicator beside the status dot, and Silence/Unsilence are mutually exclusive** ([#2031], [#2020], [#2011])
- **The Query Store grid gains the inline plan button its sibling grids always had** ([#1980], [#1949])
- **A PVS trend chart on the FinOps Version Store tab, in both apps** ([#1984], [#1951])
- **The Procedure Stats comparison grid gains the text column its two siblings always had** ([#1981], [#1949], [#1568], [#1767])
- **Lite's sidebar groups the fleet into its tag tree, with inline assign and tag CRUD** ([#2020])
- **Server tags come to Lite: a Manage Tags editor, colour, coloured pills, and search-by-tag on the Overview** ([#2020])
- **Tags get colour, and a server's tags show as coloured pills on the Overview cards (Viewer)** ([#2008], [#2011])
- **Find servers fast: a live search box on the server list across all three surfaces** ([#2008])
- **The Viewer's connect-failure overlay gains "Run self-test" - a layered probe that names WHICH layer broke instead of one collapsed error** ([#1954], [#1966])
- **You can see what automatic plan correction is doing, including the queries the engine is working on right now** ([#1952])
- **Both apps now collect and show the ADR persistent version store, so a database that grows for no visible reason finally has an explanation** ([#1951])
- **The Darling Viewer says which darling.json it read, and what it parsed out of it** ([#1954])
- **Darling hands you a working remote Viewer setup instead of a documentation exercise: `--export-viewer-config`** ([#1953], [#1970])
- **`--print-viewer-connection` warns BEFORE it prints, not after** ([#1953])
- **The PagerDuty channel can route through a proxy** ([#1945])
- **PagerDuty is a first-class alert channel in Lite and Darling** ([#1943])
- **Collection Health now says whether a collector that has come back empty all week had anything to collect** ([#1852], [#1837], [#1863], [#1867])
- **A blocking alert that fires on total blocked WAIT TIME, not just how many sessions are blocked** ([#1839], [#1812], [#1830], [#1840])
- **A database-state alert that fires when a monitored database leaves its expected state** ([#1986])

### Changed

- **A bounded Query Store cycle now DEFERS its backlog instead of dropping it - collection ships oldest-first from the watermark and resumes exactly where it stopped** ([#1960], [#1556])
- **Analysis findings re-notify only when they WORSEN or RESOLVE-and-return — a standing story no longer re-fires on every cooldown expiry** ([#2054], [#1984])
- **Top-queries reads now say when a hash group merged different statements, instead of labeling a blend with one arbitrary text** ([#2012], [#1767])
- **Top-queries reads now split same-hash statements by their hosting procedure, so INSERT...EXEC callers stop blending outright** ([#2012], [#1568], [#1767])
- **The orphaned CPU/IO baseline aggregates are retired — every store drops them on the next service start** ([#2007], [#1995])
- **Both MCP hosts move to ModelContextProtocol SDK 2.0 (the MCP 2026-07-28 specification)** ([#1990], [#1822], [#1074])
- **`get_analysis_findings` now returns one entry per diagnostic chain instead of one per engine cycle - a 24h read shrinks ~28x** ([#2000])
- **Memory-pressure anomalies no longer fire on healthy servers with young baselines** ([#1996])
- **CPU and I/O latency reach robust-baseline parity in Darling** ([#1743])
- **The anomaly engine judges deviations against median/MAD instead of mean/stddev, and its confidence is honest** ([#1743], [#1606])
- **The MCP docs now lead with the boundary instead of the warning**
- **Query text and query plan columns sit at the FRONT of every query grid now, right of the time column, in both apps and the web UI** ([#1949])
- **Setting up a remote Darling Viewer is its own section in the docs now, and it leads with the verb that does it for you** ([#1955], [#1953], [#1970])
- **The SQL Server grant block is documented in ONE place, and it now includes `ALTER SETTINGS`** ([#1955])
- **Deadlocks collection defaults to the 5-minute tier instead of every minute, in both apps** ([#1963])
- **Alt mnemonics for the three checkboxes that live on tab content: `Auto-refresh` in both apps, `All Databases` in Lite's FinOps tab** ([#1878], [#1847], [#1860])
- **Azure SQL Managed Instance now collects Query Store plan type and replica role, instead of storing NULL placeholders on every instance** ([#1886], [#1848], [#1872], [#1934])
- **Bring-your-own PostgreSQL now documents how to size TimescaleDB's background workers, and that the pool is shared by the whole cluster** ([#1899])
- **The Settings windows' section-local buttons have Alt mnemonics, and the three identically-labelled webhook test buttons now say which channel they test** ([#1856], [#1847])
- **Alt mnemonics reach the context menus and the dialog checkboxes and radio buttons [#1847] scoped out** ([#1860], [#1857], [#1878])
- **Alert Detail has a Close button, and the plan and graph viewer windows close on Esc** ([#1858], [#1847])
- **Enter no longer submits three Darling Viewer dialogs that their Lite twins leave alone** ([#1859], [#1828])
- **Every dialog in Lite and the Darling Viewer now has Alt mnemonics on its action buttons, and Esc closes it** ([#1847], [#1828], [#1856], [#1857], [#1858], [#1859])
- **The Long Running Query filter checkboxes stopped eating an underscore out of the wait and procedure names they name** ([#1847])
- **Azure SQL Database now attributes its Query Store rows to a replica role instead of storing a NULL** ([#1872], [#1844], [#1836], [#1848], [#1871], [#1841])
- **CI: the version-bump check survives the deprecated/ layout transition** ([#1821])

### Fixed

- **MCP no longer loses the monitored-server registry over an SMTP password it never reads**
- **A baseline query that runs out of time now SAYS so instead of reading as a broken connection**
- **A recompiled plan's CPU is no longer discarded, so per-query attribution stops under-reporting a plan-churning instance** ([#2235], [#2234], [#2233], [#2012])
- **`--encrypt-password` and `--configure-network` explain themselves on a non-interactive console instead of silently doing nothing** ([#2097])
- **Linux: `add_servers` accepts `env:`/`file:` secret references, unblocking onboarding on compose deployments** ([#2087])
- **An already-hardened config backup no longer errors on every service start** ([#2093])
- **Self-alerts and PVS alerts now carry their real severity in Teams/Slack/PagerDuty/webhook payloads instead of rendering INFO-blue** ([#2090])
- **`--configure-network` no longer corrupts its edit (and then refuses to write) when a postgres/mcp/web section's last member is a string with no trailing comma** ([#2073])
- **Lite's two server-delete doors now run the same deep cleanup - Manage Servers' Delete no longer leaves a removed server's runtime state behind for a re-add to resurrect** ([#2033], [#2026], [#1696])
- **A transient config read failure at boot no longer silently kills the web dashboard and MCP server for the whole process lifetime** ([#2038])
- **Removed servers leave the web dashboard instead of lingering as ghosts** ([#2030])
- **Add Multiple Servers accepts SQL Server's own \`host,port\` syntax instead of eating the port as a display name** ([#2027])
- **The Viewer's tag editors now honor the read-only seat instead of failing silently** ([#2008])
- **The Darling Viewer looks for a relative `Root Certificate` beside `darling.json`, not beside wherever it happened to be launched from** ([#1970], [#1954])
- **A failed `--export-viewer-config` no longer leaves a half-finished handoff folder behind without saying so** ([#1973])
- **A Lite test suite that failed about one run in six, on a race between the tests rather than anything in the product** ([#1965], [#1957])
- **The Darling installer's SECURITY WARNING was right, and the files it named really were left world-readable** ([#1957], [#1818])
- **Darling's retention summary line described a posture the store does not have** ([#1958], [#1849], [#1942])
- **default_trace_events reads the CURRENT trace file instead of the whole rollover set** ([#1962])
- **query_stats and procedure_stats rank BEFORE they render** ([#1959])
- **--collapse-legacy-slices was unreachable from the command line** ([#1912])
- **Chart lines no longer connect across collection gaps** ([#1944])
- **The #1912 destroy-pin live test is self-sufficient under a SearchPath-pinned rig** ([#1940])
- **Every remaining 21-day mention in code and CLI output caught up with the 90-day horizon** ([#1937])
- **The Query Store slicer overlay is drawn at the hour the work RAN, so it lines up with the bars underneath it again** ([#1921], [#1841], [#1892], [#1923], [#1845], [#683])
- **Darling gains `--collapse-legacy-slices`, which repairs the Query Store rows stored before the split-slice fix and re-materializes the rollups they fed** ([#1912], [#1907], [#1849], [#1759], [#1793])
- **The retention chunk-geometry guard now reads the chunks on disk, not just the interval the catalog is configured with** ([#1925], [#1915])
- **Query Store execution counts stopped reporting a sliver of an interval instead of the whole thing** ([#1907], [#1841], [#1845], [#1853], [#1912])
- **A live-test rig without TimescaleDB preloaded now says so, instead of failing every test with "Connection is not open"** ([#1922], [#1794], [#1896])
- **The force-plan recommendation was telling operators to write the one call form that errors** ([#1914], [#1882])
- **The Dashboard's "Collection Stopped" alert history row threw away the figure the alert was raised to report** ([#1913], [#1846], [#1881])
- **The other half of the retention coverage invariant - chunk GEOMETRY - is now guarded against a real TimescaleDB** ([#1915], [#1905], [#1877], [#1925])
- **The retention ordering that keeps purges from stopping is now derived from the policy list, not asserted against whichever pairs happened to exist** ([#1905], [#1877], [#1757], [#1784], [#1915])
- **Alert history stored the PostgreSQL major version as an alert's "current value", and other numbers lifted out of prose** ([#1881], [#1846], [#1913])
- **A force-plan recommendation driven by a read-only secondary replica's regression no longer silently recommends forcing on the primary** ([#1882], [#1850], [#1914])
- **Query Store charts no longer draw a bar outside the range you asked for, or drop the newest one** ([#1892], [#1841])
- **The Add Server dialogs now size themselves to the monitor they are actually on** ([#1891], [#1828], [#1829])
- **Lite's MCP `get_alert_settings` now reports `cpu.mode`, in the same vocabulary Darling uses** ([#1911], [#1895])
- **A retention policy whose coverage list GROWS is now re-held, instead of purging past the new tier forever** ([#1877], [#1869], [#1784], [#1905], [#1906])
- **A cross-database lock now names the database the contended object actually lives in, from both blocking collectors** ([#1893], [#1876], [#1865], [#1898])
- **CI's gated-live PostgreSQL now runs the worker sizing the product ships, so TimescaleDB's background jobs can actually launch** ([#1888], [#1862], [#1874], [#1899], [#1705])
- **The same blocked object no longer raises two different alerts depending on which collector saw it** ([#1876], [#1865], [#1893])
- **A blocked-process collector that runs per database no longer discards its own diagnostics** ([#1875], [#1851], [#1535], [#1865], [#1556])
- **Alerts that have no number to report show a dash instead of "0.00"** ([#1846], [#1881])
- **Plan-regression analysis stopped silently discarding one replica's Query Store rows on an Availability Group** ([#1850], [#1841], [#1845], [#1882])
- **A database or server whose name contains an underscore no longer renders with the underscore missing and a stray Alt key attached to it** ([#1857], [#1847])
- **Lite's data migration no longer leaves your real store in the folder `Setup.exe` deletes when an empty one beat it to the new location** ([#1842], [#1832])
- **Lite's new "Total blocked wait" box now grays out, resets, and shows up in the alert preview like every other threshold** ([#1840], [#1839])
- **Lite and Darling's MCP `get_alert_settings` now use the same key names for the same fields** ([#1840])
- **A grouped number in an alert's display text is no longer truncated to its first digit group** ([#1834], [#1830])
- **Installing Darling over a service that was re-homed to a domain account no longer silently re-ACLs the config around the wrong principal** ([#1824])
- **The documentation stops recommending `SQLAgentReaderRole` in the four places [#1823] missed** ([#1826])
- **A blocked-process row that says `Unresolved` now says WHY it is unresolved** ([#1865], [#1851], [#1854], [#1875], [#1876])
- **A database whose file space `database_size_stats` could not read is now named on the collection-log row instead of vanishing** ([#1851], [#1837])
- **Collection Health's Note and Last Error now show the NEWEST message, not the alphabetically greatest one** ([#1855])
- **The Query Store hourly and daily rollups stopped baking in the pre-dedup inflation - a Custom Views panel reaching past the raw tier no longer steps by two orders of magnitude at the boundary** ([#1849], [#1841], [#1759], [#1793], [#1680], [#1581], [#1869])
- **The Query Store DAILY numbers stopped over-counting by roughly 2x - the tier that is kept indefinitely is now the accurate one** ([#1869], [#1879], [#1877], [#1849], [#1581])
- **Query Store charts now put an interval's work in the hour it RAN, and the duration trend stopped overstating** ([#1841], [#1849], [#1850])
- **Query Store totals no longer count the same interval once per collection - live stores were overstating hot queries by one to two orders of magnitude** ([#1841])
- **Alert History rows stored under the old "TempDB Space" metric name show their percentage again** ([#1830])
- **An enumerated collector that enumerates NOTHING now says so on its collection-log row** ([#1837], [#1833])
- **Collection Health now shows what a collector that collected NOTHING reported - and an enumeration can finally say why it found nothing** ([#1837], [#1855], [#1852], [#1836], [#1851], [#1823])
- **A Lite collector that fails before it runs no longer logs the previous collector's timings as its own**
- **Opening the Settings window after a data loss no longer deletes the webhook URLs that survived it** ([#1832])
- **A tray notification when an update is available, not just a title-bar suffix** ([#1832])
- **Query Store collects on Azure SQL Database** ([#1836], [#1833], [#1546], [#1558], [#1556])
- **Top Procedures collects on Azure SQL Database** ([#1833], [#1836], [#1837])
- **Chart x-axis timestamps now honor the Local/Server/UTC time toggle** ([#1831])
- **High CPU alerts no longer record 0 as their value in Alert History** ([#1830])
- **The Add Server dialog's buttons can no longer land off-screen when a taller auth mode is selected** ([#1828])
- **The CDC capture-job probe no longer fires an "Invalid object name 'msdb.dbo.cdc_jobs'" error event on servers that never configured CDC** ([#1096])
- **The documented least-privilege monitoring grants now actually run every collector, verified live** ([#1823])
- **The managed-store credential refusal now recognizes a re-homed service account and prints the ownership fix** ([#1823])
- **Darling installs re-homed to a domain account or gMSA survive upgrades and get correct remediation text** ([#1823], [#1802])
- **The gated-live Darling test suite no longer depends on which class the runner happens to schedule first** ([#1862])
- **The compression tests no longer race the background job that `add_compression_policy` starts on its own** ([#1889], [#1788], [#1760], [#1888])
- **A live test that fails to clean up after itself now fails the run instead of reporting success** ([#1873], [#1784], [#1862], [#1788], [#1794], [#1896])
- **A live test that fails mid-body now reports its OWN error, not whatever its teardown said afterwards** ([#1896], [#1794], [#1873], [#1902])
- **The live teardowns that could corrupt OTHER tests' results are converted first, and the backlog can now only shrink** ([#1902], [#1896])
- **The whole Viewer read-path test family now reports its own failures too, and the ratchet drops to 19** ([#1902])
- **A compression test that asserts a job has NEVER RUN stopped losing to the job actually running** ([#1888], [#1760], [#1788], [#1889])
- **No live test cleans up on the connection its own body used any more** ([#1902], [#1874], [#1776])
- **The live compression tests had two ~200-second windows around midnight where they failed for the clock rather than for the code** ([#1972])
- **The gated-live plan-capture E2E no longer lets the target's plan cache decide its verdict** ([#1988], [#1767])
- **Dependabot's PRs now target `dev`, where they can actually merge** ([#1822], [#1990])

## [3.3.0] - 2026-07-29

Full entries: [docs/changelog/3.3.md](docs/changelog/3.3.md)

### Important

- **Darling stores still on PostgreSQL 17 are upgraded IN PLACE to PostgreSQL 18.4 + TimescaleDB 2.28.1 on the first service start** ([#1718], [#1752])
- **On Darling stores with pre-existing history, the raw retention purges stay safely HELD until rollup coverage reaches the oldest raw row** ([#1788], [#1799])
- **The first start on this release also retunes every compression policy to an hourly tick and raises `maintenance_work_mem` to the measured compression floor** ([#1779], [#1780], [#1813])
- **The Full Dashboard and the CLI Installer are retired**

### Added

- **The gated live-Postgres leg now asserts the cluster it connected to serves the pinned runtime versions** ([#1806], [#1787])
- **The backfill's slice refreshes now pass `refresh_newest_first` explicitly, and a degrade is impossible to ignore** ([#1797])
- **Two guards that turn structural properties into checked ones** ([#1796], [#1789], [#1793])
- **The catalog retention sweep no longer deletes history the rollups never captured** ([#1793], [#1784])
- **The payload-dimension GC now stands down when the purge it depends on failed** ([#1789], [#1782], [#1784])
- **The query text and plan XML now live once each, not once per collected row** ([#1768], [#1767], [#1759])
- **Eight members had someone else's documentation, and seven had lost their own** ([#1751], [#1739], [#1744])
- **Darling store runtime upgrades: in-place 17 to 18, validated end to end** ([#1718], [#1705])
- **The AG replica chips now mark which node is the local one** ([#1735])
- **Darling: an AG failover is reported once, not once per monitored node, and a standing replica disconnect can re-alert** ([#1734], [#1674])
- **Availability Groups tab in Lite, and one shared AG projection behind every surface** ([#1731])
- **Lite gets the Availability Group alerts, off a shared policy** ([#1726], [#1688])
- **Availability Groups tab in the WPF viewer** ([#1722])
- **The bundled PostgreSQL runtime now proves its own version instead of being taken at its word** ([#1713])
- **AG fixture evidence: state the commit-time conclusion the guidance rests on** ([#1709], [#1708])
- **AG fixture evidence: the frozen suspended-row surface, and a lag correction** ([#1707], [#1702], [#1703], [#1704])
- **AG fixture evidence: what `secondary_lag_seconds` actually measures on a suspended row** ([#1704], [#1707])
- **Darling Web: an "AG Health" seed notebook** ([#1699], [#1688], [#1695])
- **AG latency: commit-time columns, drain-time estimates, and the primary-side perfmon counters** ([#1695], [#1688])
- **AG collection: document the second grant it needs, and make the lag trap filterable** ([#1691], [#1688])
- **Darling: Availability Group alerts - failover, replica disconnected, sync fell behind, database suspended** ([#1692], [#1688], [#1674])
- **Availability Group health collection in Lite + Darling** ([#1688], [#1692])
- **A Docker Availability Group fixture, and what it found about the AG DMVs** ([#1689])
- **Availability Group topology, in the browser and over MCP** ([#1690])
- **Darling Web: the reporting layer grows scatter, dual-axis, and brush-zoom** ([#1683])
- **CI: every `report.*` view now EXECUTES against seeded rows, not just exists** ([#1677])
- **Connection alerts for servers that are already down, and re-alerts during a standing outage** ([#1674])
- **Darling: `--configure-network` can now expose the WEB DASHBOARD** ([#1617])
- **Darling Web: adaptive `auto` time bucket for composed time-series panels** ([#1619])
- **Darling Web: custom time span on the view range picker** ([#1619])
- **Darling: hourly continuous aggregates for query_stats + procedure_stats (composer query acceleration)** ([#1621])
- **Darling: three-tier retention - downsample past the raw hot window, keep the rollups** ([#1623])
- **Darling Web: composed panels read the CAGG rollups for old windows instead of truncating at the raw horizon** ([#1625])
- **Darling Web: Query Store panels route past the 21-day hourly horizon (query_store_stats_daily)** ([#1626], [#1627])
- **Darling Web: composed `object_name` panels on query_stats route to the CAGG rollups too — the last read-routing exception, closed** ([#1627])
- **Darling viewer: organize the fleet into nested tags** ([#1640])

### Changed

- **Darling store: `maintenance_work_mem` raised to the measured compression floor, and existing stores actually get it** ([#1780], [#1777])
- **CI: a dev/main push now cancels that branch's superseded in-flight build — newest SHA wins** ([#1729], [#1715], [#1716])
- **CI: build.yml handles the merge_group event** ([#1716], [#1715])
- **CI: the change classification the v4 pin bump silently broke is restored - and this time it is probe-validated** ([#1715], [#1712], [#1701])
- **CI: a documentation change stops paying for a .NET restore** ([#1712])
- **CI: the Lite fast / analysis-heavy test split collapses back into one step** ([#1701], [#1693], [#1694], [#1698])
- **Lite.Tests: the shared DuckDB fixture extends to 20 fast-bucket classes** ([#1698], [#1693], [#1694])
- **Lite.Tests: seeding is batched - one connection and one transaction per seed helper instead of a connection and an auto-commit per ROW** ([#1694], [#1693])
- **Lite.Tests: the analysis-heavy classes share one DuckDB per test class instead of rebuilding the schema per test** ([#1693])
- **Quarterly maintenance pass: dependency bumps, a zero-warning build restored, repo hygiene** ([#1643])
- **CI actually runs the deprecated Dashboard's build and tests again** ([#1643])
- **Full Dashboard and the CLI Installer are being retired.** ([#1612])
- **Darling Web: the custom-view panel grid is capped at two columns on wide screens** ([#1619])
- **Darling: the query_store_stats + procedure_stats continuous aggregates are reshaped to the composer's dimensions** ([#1624])

### Fixed

- **The service now hardens EXISTING config backups, not just the live config** ([#1818], [#1816])
- **The dimension GC's floor measurement now survives compressed history** ([#1817], [#1815])
- **A stale running_jobs snapshot no longer re-fires the same Long-Running Job alert forever** ([#1814], [#1812])
- **The dimension GC prunes against the oldest SURVIVING digest-carrying fact row, not an assumed horizon** ([#1813], [#1795])
- **Recommendations warm-up now survives the 512 MB archive/reset, and analysis reads the archive tier everywhere** ([#1811], [#1809])
- **Live-Postgres test cleanup no longer depends on the connection the failure just destroyed** ([#1810], [#1794])
- **A benign 1-second lock-timeout yield no longer paints a Critical health day** ([#1808], [#1805])
- **The .NET SDK band is now pinned three ways instead of agreeing by float** ([#1807], [#1758])
- **The payload-dimension batch upsert deadlocked against itself across concurrent collection cycles** ([#1803], [#1801])
- **`--backfill-rollups` converged every rollup to raw's oldest row, but the arming gates for the hourly tier measure the SOURCE - so a fully-DONE run could leave retention policies held** ([#1799], [#1798], [#1680])
- **The retention rollups only ever served what they had materialized, so old windows read empty while raw still held every row** ([#1788], [#1759], [#1680], [#1665])
- **The directories cluttering a field instance's install folder were invisible to the report built to find them** ([#1785], [#1770], [#1775])
- **Four test classes shared the live Postgres store without serializing against it, and three more were blamed for it by a substring match** ([#1783], [#1776])
- **The config backups handed a second readable copy of every secret to any interactively-logged-on user** ([#1786], [#1769], [#1792])
- **The compression self-heal read TimescaleDB's never-ran sentinel as a run that started in year 1** ([#1781], [#1760])
- **A chunk that was already eligible to compress could still sit uncompressed for most of a day** ([#1779], [#1778], [#1768], [#1680], [#1585])
- **One stuck rollback copy froze the ageing-out of every other one** ([#1775], [#1770], [#1718])
- **The restart delta seed read the whole store to find rows it was about to throw away** ([#1774], [#1772])
- **The service tried to open its own firewall on every start, could never succeed, and a fresh networked install got no rule at all** ([#1773], [#1771])
- **Anomaly baselines were being computed from a fraction of their intended history, silently** ([#1762], [#1757])
- **The Lite AG alert guard now captures the data service before checking it** ([#1756], [#1754], [#1755])
- **A package with an OLDER PostgreSQL major no longer replaces a working newer runtime and takes the store down** ([#1752], [#1738], [#1737])
- **The Lite AG alert sweep could throw instead of quietly doing nothing before the store opened** ([#1753], [#1726], [#1750])
- **Alert FIRINGS are logged now, in the shared engine and in Lite - not just resolutions** ([#1750], [#1685])
- **The config backups were world-readable copies of the credentials they back up** ([#1749], [#1721])
- **The daily-summary MCP live test no longer fails in the five minutes after UTC midnight** ([#1748])
- **Darling: the TimescaleDB ensure-summary log stopped lying about which continuous aggregates exist** ([#1747])
- **The in-place store upgrade could not authenticate anywhere but a developer's box, and one of its new guard tests could pass with the bug present** ([#1739])
- **The store-upgrade commit point is now pinned, and the degraded-upgrade alert stops asking for cleanup the product does itself** ([#1739], [#1718])
- **Azure SQL Database: `database_size_stats` stopped touching `master`, and error 300 now explains itself** ([#1732], [#1634], [#1642])
- **Lite: prove the Agent-status SQL, not just the decision it feeds** ([#1730], [#1725])
- **Credential-file ACL failures now name the owner, and the DPAPI credential files verify the result instead of assuming it** ([#1727])
- **Lite: the Job History Agent indicator stops calling absence of signal a problem** ([#1725])
- **Darling Web: a capped time-series panel kept the OLDEST slice of the window and said nothing** ([#1724])
- **Darling: "Agent Not Running" no longer nags servers that never ran SQL Agent** ([#1719])
- **Darling: retention policies were silently not being created AT ALL, on every store** ([#1711])
- **Darling: state the rule that decides how a new AG measure gets handled** ([#1710])
- **Darling: corrected the AG doc guidance on commit-time deltas** ([#1708], [#1703])
- **Darling: the AG lag alert now says what its number actually measures** ([#1703], [#1700])
- **AG suspension semantics: one rule that holds whether or not the vendor docs are right** ([#1702])
- **Darling: a suspended secondary that is falling behind now raises the sync alert** ([#1700], [#1692])
- **Darling: the Availability Group store reads are now executed against a real Postgres in CI** ([#1697], [#1692])
- **Lite + Darling: recovery notices were being styled and counted as live alerts in Alert History and the Daily Summary** ([#1692])
- **Darling: the MCP `get_alert_settings` tool under-reported the store by five columns** ([#1692], [#1674])
- **Darling test suite: a genuinely intermittent failure, roughly one full-suite run in six** ([#1692])
- **Lite + Darling: the query tools no longer present `max_dop` as current parallelism** ([#1682])
- **Darling: self-alert FIRINGS are logged, not just their recoveries** ([#1681])
- **Darling: the managed PostgreSQL now records a per-run audit trail for TimescaleDB background jobs** ([#1681])
- **Darling: retention policies no longer destroy history on the deploy that creates them** ([#1680])
- **Lite + Darling: a permission-denied collector is now visible without opening the Collection Health tab** ([#1591])
- **Docs: which collectors run on which platform** ([#1591])
- **Darling viewer: Server Inventory now explains missing hardware instead of showing zeros** ([#1591], [#1535], [#1663])
- **Upgrade scripts (deprecated path): the 2.1.0-to-2.2.0 compression migrations are now genuinely idempotent** ([#1676], [#1673])
- **Full Dashboard (deprecated): `report.daily_summary_v2` no longer garbles `worst_query_hash`** ([#1667], [#1666])
- **Darling Web: composed custom-view panels no longer route onto rollups the store doesn't have** ([#1675])
- **Full Dashboard (deprecated): the Trace Flag Changes grid and the `get_trace_flag_changes` MCP tool throw `InvalidCastException` now that the view returns rows** ([#1668])
- **Darling: managed PostgreSQL's `pg.log` finally rotates** ([#1670])
- **Darling viewer: built-in tabs no longer silently lose history past the raw retention horizon** ([#1661], [#1623], [#1625], [#1626], [#1627])
- **Lite + Darling: a monitoring login without `VIEW SERVER STATE` no longer loses its entire `server_properties` row** ([#1591])
- **Lite + Darling: the `get_active_queries` MCP tool no longer tells the model its data comes from sp_WhoIsActive** ([#1650])
- **Darling Web: a LAN-exposed dashboard no longer lets any local process in without a credential** ([#1649])
- **Darling + Lite: security hardening - firewall command injection via `allowFrom`, an unprotected `darling.json`, and MCP hosts with no DNS-rebinding guard** ([#1646], [#1647], [#1648], [#1576])
- **Lite + Darling: four things that grew forever - two tables and two log directories** ([#1651], [#1652])
- **Lite: the Excluded Databases picker throws a raw firewall error on an Azure database-level-firewall server** ([#1642])
- **Darling: a bring-your-own-Postgres `viewer` seat gets `permission denied` on the Manage Servers list and the Settings notification prefill** ([#1639], [#1506], [#1551])
- **Lite + Darling: Linux hosts on SQL Server 2025 CU1+ get real other-process CPU again** ([#1630], [#1048], [#1629])
- **Lite + Darling: default trace collection works on SQL Server on Linux** ([#1633], [#1632])
- **Full Dashboard (deprecated): the `report.trace_flag_changes` view no longer fails with Msg 245 on a trace-flag toggle** ([#1637], [#1635])
- **Default trace: the rollover strip no longer mangles paths whose DIRECTORY contains an underscore** ([#1636], [#1633])
- **Lite + Darling: Azure SQL DB collection works again when the client is firewalled out at the logical server but allowed at the DATABASE level** ([#1634], [#1631], [#857], [#1506])
- **Darling: collectors no longer fail with `22021: invalid byte sequence for encoding "UTF8": 0x00`** ([#1614])
- **Darling Web: a custom view no longer resets the picked time range / server / filters every 60 seconds** ([#1619])
- **Darling: the Custom Views composer and analyze_*_plan no longer time out on large stores** ([#1620])
- **Darling + Lite: the Long-Running Query alert's `sp_server_diagnostics` exclusion no longer fires a false alert when the health-check session is in a non-diagnostics wait** ([#1622])
- `install-darling.ps1`: the viewer-shortcut step no longer throws when run with no loaded user profile (a Windows service, scheduled task, CI runner, or remote session) where `GetFolderPath('Desktop'/'StartMenu')` returns empty. It creates only the shortcuts whose folder resolves and skips cleanly otherwise. ([#1613])

### Documentation

- **A second stale doc comment describing behaviour that no longer exists** ([#1744], [#1739])
- Darling README: added a "verify it's actually reachable" recipe for the opt-in LAN MCP/web endpoints — confirm the listener is on the LAN address (not loopback), the scoped firewall rule covers the client, and the client connects to the box IP rather than `localhost` — plus what to re-run after a reinstall. Enabling an endpoint is not the same as reaching it. ([#1616])

## [3.2.0] - 2026-07-21

Full entries: [docs/changelog/3.2.md](docs/changelog/3.2.md)

### Added

- **Darling MCP: bulk add/remove monitored servers — an MCP client can now stand up FLEET monitoring conversationally** ([#1609], [#1600], [#1608], [#1549])
- **Darling MCP: alert-tuning write tools — an MCP client can now tune thresholds and manage mute rules conversationally** ([#1608], [#1600])
- **Darling: headless `--enable-mcp` / `--disable-mcp` / `--enable-web` / `--disable-web` CLI verbs — bring an endpoint up (store + firewall) without the Viewer** ([#1601])
- **Darling MCP: `describe_custom_view_catalog` — the compose vocabulary an LLM needs to author a view without guessing** ([#1602], [#1600])
- **Darling MCP: create and manage Custom Views (CV2) over MCP** ([#1600])
- **Darling Web: Custom Views v2 - QS execution-weighted average + per-server measure availability greying** ([#1584])
- **Darling Web: Custom Views v2 - notebook mode (investigation documents)** ([#1583])
- **Darling Web: Custom Views v2 follow-ups (wave 2) - event annotations + per-panel drill-down** ([#1582])
- **Darling Web: Custom Views v2 follow-ups (wave 1) - gauge ratios, stacked-bar, threshold lines, measure availability** ([#1580])
- **Darling Web: Custom Views v2 - compose your own metrics, filters, grouping, units, and chart types** ([#1579])
- **Darling Web: custom views / notebooks — compose your own dashboards from the read catalog** ([#1576])
- **Darling Web: a read-only browser dashboard served by the service** ([#1574])
- **Long-running query completion trace (opt-in, default OFF) in Lite and the Darling viewer** ([#1572])
- **Darling viewer: ships as a Velopack `Setup.exe` for remote seats, like Lite and Dashboard** ([#1571])
- **Queries: Top Queries by Duration now attributes each statement to its named module â€” or "ad hoc" â€” in Lite and the Darling viewer** ([#1569])
- **Darling: `--configure-network` â€” a guided, fail-closed wizard for the opt-in LAN endpoints** ([#1570])
- **Darling: scripted install/uninstall ships in the zip â€” one elevated command instead of the manual service recipe** ([#1554])
- **Darling: a real diagnostic surface, store write-throughput headroom, and honest bootstrap status** ([#1552])
- **Nightly builds now include Darling** ([#1550])
- **Add Multiple Servers: onboard a whole fleet from one pasted list, in both apps** ([#1549])
- **NOC Overview: sort the server tiles by CPU% descending, with a persisted CPU%/Name selector (both apps)** ([#1547])
- **Front-end shell collapse: the standalone Plan Viewer's tab management hoisted to `Ui.StandalonePlanViewerController`** ([#1536])
- **Front-end shell collapse: the Latches & Spinlocks grouped trend-chart render body hoisted to `Ui.GroupedTrendChartRenderer`** ([#1534], [#1532])
- **Front-end shell collapse: the Session Stats trend chart + summary projection hoisted to `Ui.SessionStatsChartRenderer` / `Common.SessionStatsSummary`** ([#1533], [#1531], [#1527], [#1532])
- **Front-end shell collapse: the CPU-scheduler trend-chart render body hoisted to `Ui.CpuSchedulerChartRenderer`** ([#1532], [#1531], [#1527])
- **Front-end shell collapse: the CPU-scheduler metric projection + pressure classification hoisted to `Common.CpuSchedulerMetrics`** ([#1531])
- **Front-end shell collapse: the column-filter popup host/apply plumbing hoisted to `Ui.ColumnFilterPopupController`** ([#1530], [#1529])
- **Front-end shell collapse: the column-filter popup promoted to the shared `PerformanceMonitor.Ui.ColumnFilterPopup`** ([#1529], [#1523])
- **Front-end shell collapse: the deadlock-graph per-process walk hoisted to `Common.DeadlockGraphProcessParser`** ([#1528])
- **Front-end shell collapse: System Events counter-chart bodies hoisted to `Ui.SystemHealthChartRenderer`** ([#1527])
- **Front-end shell collapse: stateful chart-render helpers hoisted to `Ui.ChartRenderHelper`** ([#1526])
- **Front-end shell collapse: file-save dialogs hoisted to `Ui.FileSaveHelper`** ([#1525])
- **Front-end shell collapse â€” Batch B: `FindParentDataGrid` visual-tree helpers hoisted to `PerformanceMonitor.Ui`** ([#1524], [#1522])
- **Front-end shell collapse â€” Batch A: `SelectableItem` hoisted to `PerformanceMonitor.Ui`** ([#1523], [#1522])
- **Front-end shell collapse â€” PR0: the capability-inventory pin (safety net before any hoist)** ([#1522])
- **Analysis engine: plan-cache single-use bloat + ring-buffer memory-pressure issue-classification arms in the shared FactScorer (Tier-2 parity, batch-2b remainder)** ([#1521])
- **Analysis engine: two server-config issue-classification arms (priority boost + lightweight pooling) in the shared FactScorer (Tier-2 parity, batch-2b)** ([#1520])
- **Analysis engine: two tempdb-deep issue-classification arms in the shared FactScorer (Tier-2 parity)** ([#1519])
- **Analysis engine: three more issue-classification arms in the shared FactScorer (Tier-2 parity)** ([#1517])
- **Lite MCP: the 18 Darling-only tools ported (drains the MCP inventory ratchet to ZERO)** ([#1518])
- **Darling viewer: Query Store Regressions sub-tab (Dashboard parity BUILD)** ([#1516], [#1515])
- **Darling viewer: live Current Active Queries sub-tab (service-mediated)** ([#1515])
- **Lite viewer: Blocking Stats sub-tab (Tier-1 parity BUILD)** ([#1514])
- **Lite viewer: Expensive Queries sub-tab (Tier-1 parity BUILD)** ([#1513])
- **Config-diff collapse to shared Common + Lite Configuration Changes tab (Tier-1 parity)** ([#1512], [#1507])
- **Lite: connection-lost / connection-restored alerts, and a generic webhook channel for both apps** ([#1506])
- Alongside it, a third webhook channel in the shared `WebhookAlertService` â€” **generic HTTP POST** â€” beside Teams and Slack, for **Lite and Darling**. It POSTs an operator-authored JSON body with `{{metric}}`, `{{server}}`, `{{value}}`, `{{threshold}}`, `{{severity}}`, `{{context}}` and `{{timestamp}}` placeholders and an arbitrary JSON headers object, which covers PagerDuty / Opsgenie / n8n and â€” the motivating use case â€” a GitHub `repository_dispatch` that re-runs a workflow when an alert fires. **This is deliberately not a script/exe runner:** arbitrary command execution in a signed binary is an EDR-flagged attack path, and a webhook reaches the same automation with no process-execution surface. Every substituted placeholder is **JSON-escaped** (via `JsonSerializer`, which â€” unlike `JsonEncodedText.Encode` â€” tolerates the lone surrogates SQL Server names can carry instead of throwing), so a server name carrying a quote or backslash â€” `HOST\INSTANCE`, or something hostile â€” cannot break out of the template and corrupt or inject into the posted JSON; the substituted body is validated as JSON before it goes on the wire, malformed headers JSON degrades to a clear logged error instead of throwing into the alert loop, and a `User-Agent` is defaulted (GitHub's API 403s requests without one). Settings live beside Teams/Slack in both apps' Settings windows with a "Send Test" button and a `repository_dispatch` example; the URL and the headers JSON are treated as **secrets** â€” Windows Credential Manager in Lite, and in Darling's Postgres control plane (schema **V26**) `generic_url` + `generic_headers` are column-`REVOKE`d from the read-only viewer role like the existing webhook URLs, since the headers carry the `Authorization` bearer token. The deprecated Dashboard satisfies the shared settings interface with the channel off and gains no new UI.
- **Lite viewer: System Events tab (Tier-1 parity BUILD â€” drains the coverage pin to ZERO)** ([#1509])
- **Lite viewer: CPU Scheduler, Plan Cache & Session Stats tabs (Tier-1 parity BUILD, batch of 3)** ([#1505])
- **Lite viewer: Latches & Spinlocks tab (Tier-1 parity BUILD)** ([#1504])
- **Parity "net" test guards, round 2 (Tier 0): three cross-app PIN tests** ([#1501])
- **Parity "net" test guards (Tier 0): collector-coverage pin + Lite<->Darling theme pin** ([#1499])
- **Retained fleet-wide "Job History" tab (Lite + Darling)** ([#1489])
- **Global per-server database filter** ([#1490])
- **Darling: "Get Actual Plan" (service-mediated), matching the Dashboard's surfaces**
- **Shared `ActualPlanExecutor` and `QueryModificationDetector`**
- **Darling: managed Postgres derives a memory-tuning `postgresql.conf` block from host RAM â€” scale-readiness for the up-to-500-servers store** ([#1478], [#1463])
- **MCP: `get_analysis_findings` and `analyze_server` now return the full copy-paste remediation command (Lite + Darling)** ([#1475], [#1473], [#1474])
- **Lite: recommendation cards now produce the SAME runnable copy-paste T-SQL command the Darling viewer does â€” killing an advise-only-vs-command drift** ([#1474], [#1473])
- **Darling Viewer: a runnable copy-paste T-SQL command for EVERY remediable recommendation, since the headless viewer has no in-app "Apply Fix"** ([#1473])
- **Darling Viewer: per-server alert badge with Acknowledge (ack-until-worse), so you can see at a glance which servers need attention** ([#1470])
- **Shared `system_health_events` collector (Stage 1: raw `system_health` Extended Events capture), so both Lite and Darling collect them** ([#1262])
- **Darling Viewer: the System Events tab gains the CPU Tasks and I/O Issues sub-tabs (`system_health` parity, Stage 2b)** ([#1262])
- **Shared `cpu_scheduler_stats` + `plan_cache_stats` collectors, so both Lite and Darling collect them** ([#1262])
- **Shared `latch_stats` + `spinlock_stats` collectors, so both Lite and Darling collect them** ([#1262])
- **Darling security hardening Phase 1: least-privilege schema split + roles, so the Viewer stops connecting as the superuser** ([#1262])
- **Execution-plan capture extended to procedure_stats, blocked_process_report, and deadlocks** ([#1262])
- **Darling Viewer W1j: the Memory inner tab â€” Lite's four Memory sub-tabs, copied** ([#1262])
- **PerformanceMonitor.Collectors: the shared collector-definition library â€” ALL 26 of Lite's collectors extracted** ([#1262], [#1180], [#1240], [#1048], [#989], [#857], [#1135], [#1286], [#1140])
- **Darling: the Postgres schema is now generated from the shared collector definitions** ([#1262], [#857])
- **Darling captures execution plans; Lite still does not** ([#1262])
- **Darling Viewer W2a: the top-level aggregate surfaces â€” the Overview server-cards grid and full Alert History parity, copied from Lite** ([#1262])
- **Darling Viewer: the Plan Viewer inner tab lights up View Plan against the stored plans â€” copied from Lite** ([#1262])
- **Darling M3: the first Viewer shell â€” servers, collection health, wait trend** ([#1262], [#1134], [#1140])
- **Darling Viewer W1a: the CPU and tempdb inner tabs, copied from Lite** ([#1262])
- **Darling Viewer W1c: the File I/O tab and the Blocking tab's Trends + Current Waits sub-tabs, copied from Lite** ([#1262])
- **Darling Viewer W1e: the Blocking tab reaches full Lite depth â€” widened Blocked Process Reports, the Deadlocks sub-tab, and the block-chain + deadlock-graph viewers, copied from Lite** ([#1262])
- **Darling Viewer W1f-1: the Queries tab becomes Lite's nested sub-tab group â€” the three big grids (Top Queries / Top Procedures / Query Store), copied from Lite** ([#1262])
- **Darling Viewer W1f-2: the Queries tab is completed to Lite's full six sub-tabs â€” Performance Trends, Active Queries, Query Heatmap, copied from Lite** ([#1262])
- **Darling Viewer W1d: the Overview inner tab becomes Lite's correlated timeline lanes, copied** ([#1262])
- **Darling Viewer W1i: the Daily Summary inner tab + Collection Health upgraded to Lite's 3-sub-tab shape â€” copied from Lite** ([#1262], [#1248])
- **Darling Viewer W1h: the Perfmon and Running Jobs inner tabs â€” copied from Lite** ([#1262])
- **Darling Viewer W1g: the Configuration inner tab â€” Lite's four read-only snapshot grids, copied** ([#1262])
- **Darling Viewer W0: copy-parity foundations + navigation inverted to Lite's shape** ([#1262])
- **Darling Viewer W1b: the Wait Stats inner tab â€” Lite's wait-type picker + trend, copied** ([#1262])
- **Lite and Dashboard: the six alert-context builders move into the new shared `PerformanceMonitor.Alerting` library** ([#1262], [#1140], [#1091], [#1145], [#754], [#1136], [#1141], [#1154], [#1202], [#1236], [#749], [#1128])
- **Darling: optional TimescaleDB adoption â€” hypertables, chunk-based retention, and compression as the archival tier** ([#1262])
- **Darling: managed bundled Postgres â€” the zero-admin first-run bootstrap** ([#1262])
- **Darling: scaffold for the net-new headless/centralized edition** ([#1262])
- **Darling: operator documentation for the headless edition** ([#1262])
- **Darling: wired into the release pipeline** ([#1262])
- **Darling: nightly CI now covers the gated live-PostgreSQL tests** ([#1262])
- **README: Managed Identity and Service Principal authentication documented** ([#1321], [#1042], [#1268])
- **Darling: Index Analysis rollup gains the workload-impact columns** ([#1262])

### Changed

- **CI: PR build/test/publish is now gated per product, so a change to one edition doesn't rebuild the others** ([#1599], [#1598])
- **CI: the gated-live Darling PostgreSQL test suite now runs on pull requests, not only in the nightly** ([#1598], [#1586])
- **Viewer + Lite: plan-action menu verbs renamed to end the "Actual" overload** ([#1542])
- **Dashboard: the add/edit-server state machine and its predicates move to `Installer.Core`, where CI can test them** ([#1498])
- **Darling + Lite: the `system_health` significance predicates are collapsed into one shared copy in `PerformanceMonitor.Common` (Tier-1 parity groundwork)** ([#1507])
- **Lite and Darling: the triplicated baseline analysis MODEL is collapsed into one shared copy in `PerformanceMonitor.Analysis` (Tier 0 parity collapse)** ([#1503])
- **Lite: the 35 collector-table DuckDB schemas are now GENERATED from the shared `CollectorCatalog` (Tier 0 parity collapse) instead of hand-written** ([#1502])
- **Analysis findings: parallelism (CXPACKET) can no longer reach CRITICAL on amplifier stacking alone; a real thread-exhaustion meltdown escalates through THREADPOOL instead (Stage B of the recs-engine remainder â€” the severity half)** ([#1494])
- **Analysis findings: anomaly detections fold into the regular incident, and alerts dedup per incident (Stage B of the recs-engine remainder)** ([#1493])
- **Darling: `collect.collection_log` becomes a TimescaleDB hypertable â€” O(1) `drop_chunks` retention + compression, scale-readiness for the up-to-500-servers store** ([#1479], [#1478])
- **Darling viewer W2b: the Recommendations aggregate tab re-skinned to Lite's advise-only card design** ([#1262])
- **Lite: `date_diff` replaced with the PostgreSQL-shared spelling, bit-exactly (headless groundwork, phase 1a)** ([#1262])
- **Lite: DuckDB SQL normalized to the PostgreSQL-shared dialect (headless groundwork, phase 1a)** ([#1262])
- **Darling viewer: the Latches & Spinlocks tab is consolidated into one view** ([#1488])

### Removed

- **Plan Cache tab: dropped the momentary Cache Object Type breakdown grid (both apps)** ([#1539])
- **Queries > Expensive Queries sub-tab dropped from both apps; the Performance Calendar day-drill repoints to Top Queries** ([#1544])

### Fixed

- **Darling.Tests: the compression self-heal gated-live test no longer races the TimescaleDB scheduler** ([#1604], [#1585], [#1586], [#1588])
- **Darling service: the sweep hang-watchdog no longer cries wolf when a server is merely waiting its turn** ([#1597], [#1590], [#1593], [#1553], [#1557], [#1581])
- **Darling service: settled the #1581 "narrow the credential ACL scope" question — the lockdown does NOT silence file logging, and the scope stays** ([#1596])
- **Darling service: removing an already-absent firewall rule no longer logs a bogus warning on every shutdown** ([#1594])
- **FinOps Server Inventory: an Azure SQL DB monitoring login without VIEW DATABASE STATE no longer loses the entire server row** ([#1592], [#1589])
- **Darling analysis: the RegressedQueries plan-regression drill-down no longer scans all Query Store history on large stores** ([#1593])
- **Connectivity probe: Azure SQL DB is no longer silently mis-detected as on-prem when the monitoring login lacks VIEW DATABASE STATE** ([#1589])
- **Darling service: file logging now fails LOUDLY instead of going silent** ([#1590])
- **Darling scheduler: a service restart no longer herds all servers' first catch-up sweep into one tick** ([#1590], [#1577], [#1553])
- **Darling service: the compression-job self-heal can now actually re-arm a stuck job (`alter_job` job_id type fix)** ([#1586], [#1585], [#1588])
- **Darling service: TimescaleDB compression-job self-heal + single-instance / argument-handling hardening** ([#1585], [#1586])
- **Darling scheduler: a service restart no longer starves long-frequency collectors - NextDue is seeded from the persisted collection watermark, not a full-interval offset** ([#1577], [#1553])
- **Collector health: a healthy DAILY collector no longer bands STALE/FAILING, and no longer inflates the Overview "collectors failing" count** ([#1578])
- **Darling: the retention purge's batched DELETE no longer breaks on compressed chunks** ([#1567])
- **Darling: the viewer's store pool is now capped too, and the postgres.exe process count is documented for what it actually is** ([#1566], [#1559])
- **query_store collector: the self-exclusion filter was 75% of the read's cost â€” bounded to a 100-char scan; per-database log lines; rdsadmin default-excluded** ([#1565], [#1557])
- **Darling: the viewer's MCP toggle now actually starts and stops the MCP server â€” live, no service restart** ([#1560])
- **Darling managed store: size shared_buffers for the co-located reality, healing the Windows backend-spawn failures** ([#1559])
- **Query Store: stop collecting from readable-secondary replicas â€” their Query Store is the primary's replicated content, not local activity** ([#1558], [#1557], [#1546])
- **Collectors: a stalled query_store cycle can no longer run the collector out of memory and take the box down â€” reads are byte-bounded, writes flush per database, and an over-memory service stops launching new work** ([#1557], [#1553])
- **Darling: one slow server can no longer strand the rest of the fleet â€” the collection sweep goes bounded-parallel with cadence jitter and per-server exception containment** ([#1553], [#1552])
- **Index Analysis: the monitor-side dedupe engine absorbs the sp_IndexCleanup review fixes it predated** ([#1548])
- **Query Store: attribute runtime stats to their replica role, so an AG's primary stops silently reporting secondary workload as its own** ([#1546], [#1535])
- **Lite: the plan verbs no longer sit on every grid â€” only on the query grids that can actually use them** ([#1545], [#1543])
- **Darling viewer: stop the hard crash when interacting with the system-tray icon (Hardcodet TrayToolTip issue #422)** ([#1538])
- **Charts: increase the shared top headroom from 5% to 15% so a series that plateaus at a hard ceiling no longer reads as clipped at the top** ([#1538])
- **The gated live-PostgreSQL test suite runs green for the first time â€” one real grant gap and five latent test defects** ([#1551])
- **Darling viewer: query-tab plan menus fixed â€” no greyed-out verbs, right-click acts on the clicked row, and the Regressions / live Active Queries grids get plan access** ([#1543])
- **Darling viewer: the first click on an unsorted grid column now sorts descending, and header-copy plus middle-click panning work on every grid** ([#1541])
- **Charts: finish the window-pin campaign so trend charts stay pinned to the selected window** ([#1540])
- **Lite and Darling: Azure SQL DB deadlock and blocked-process capture now covers EVERY monitored database, not just the connection's own** ([#1346], [#1535], [#1251], [#1086])
- **Lite and Darling: an Azure SQL DB outage no longer permanently wedges database-scoped collection until the app is restarted** ([#1506], [#857])
- **Dashboard and Lite: the server-list version display no longer risks a crash on a two-part version string** ([#1510], [#1498])
- **Clean install is Azure SQL Managed Instance-safe, and a failed drop no longer leaves the database bricked** ([#1498])
- **CLI installer: a password beginning with `--` gets a clear message instead of a misleading "password required"** ([#1498])
- **Dashboard: a failed upgrade migration no longer stamps the database as successfully upgraded** ([#1498], [#963])
- **Dashboard: a failed version check no longer reinstalls over an existing database and reports it as up to date** ([#1498])
- **A non-destructive repair path, so aborting on a failed migration doesn't leave you stuck** ([#1498])
- **`--repair` exit-code contract** ([#1498])
- **Dashboard: retyping the server name no longer leaves the previous server's answers on screen** ([#1498])
- **Cancelling an install is reported as a cancellation, not a failure â€” at every stage** ([#1498])
- **Cancelling a clean install no longer reports success over a database it just dropped** ([#1498])
- **Dashboard: "Save" is no longer enabled on a server with no PerformanceMonitor database** ([#1498])
- **Dashboard: cancelling an edit no longer silently renames the server you were editing** ([#1498])
- **Dashboard: a failed or cancelled clean install no longer hides the install log** ([#1498])
- **Both installers now refuse to run over a database that is NEWER than the binary** ([#1498])
- **Lite and Darling: the collector gate surface is collapsed to ONE shared layer, so Darling stops attempting collectors it can't run (no-msdb / Azure SQL DB / pre-2016) every cycle** ([#1500], [#1492])
- **Darling viewer: the collector-schedule presets now include `job_history`, `agent_status`, and `default_trace_events`, matching Lite** ([#1495], [#1494])
- **Lite: the `v_job_history` and `v_default_trace_events` archive views no longer double-count rows after a 512 MB emergency reset** ([#1492])
- **Lite and Darling: `agent_status` and `running_jobs` are now gated off AWS RDS in BOTH apps** ([#1492])
- **Dashboard: the cross-app `ThemeParityTests` pass again â€” the Dashboard theme palettes are re-synced to Lite** ([#1492], [#1441])
- **Lite, Dashboard, and Darling: anomaly-engine remainder Stage A â€” baseline-quality gate, one-fact wait-profile detection, and honest no-baseline rendering** ([#1491])
- **Lite and Darling: the default-trace collector can now suppress Object-DDL (schema-change) events, so a create/drop-happy workload no longer floods the System Events tab** ([#1485])
- **Lite, Dashboard, and Darling: young-store anomaly noise -- absolute-magnitude floors and a sigma display cap** ([#1486])
- **Viewer Memory and Query Performance Trends charts: the last of the trend-chart dead space is gone** ([#1487], [#1483], [#1484])
- **Viewer File I/O charts: the dead space at the chart edges is gone** ([#1484], [#1483])
- **Viewer trend charts: the empty "dead space" at the tempdb, CPU, and Blocking Stats chart edges is gone** ([#1483])
- **Darling viewer: grid default-sort audit -- history grids now newest-first, the FinOps Database Sizes grid now biggest-first** ([#1481])
- **Darling: the viewer no longer falsely gates a correctly-migrated V23 store as "schema v22"** ([#1480], [#1479])
- **Darling: the fired-alert history (`config_alert_log`) now has a retention purge, so it no longer grows unbounded** ([#1477])
- **Darling: schema V22 adds a supporting index for the FinOps Index Analysis read** ([#1477])
- **Lite and Darling: "Current Waits" now honors the ignored-waits filter, matching Wait Stats** ([#1476])
- **Lite and Dashboard: the analysis-findings retention purge is now actually scheduled** ([#1471])
- **Darling: schema V14 refreshes every `v_*` passthrough view so a store upgraded across a column-adding migration serves the new columns** ([#1262])
- **Darling viewer: the CPU series now aligns with every other timeline on non-UTC servers** ([#1262])
- **Darling: schema V5 adds the five missing passthrough views** ([#1262])
- **Darling: managed PostgreSQL now sizes background workers for TimescaleDB's compression jobs** ([#1262])
- **Lite, Dashboard, and Darling: muting a finding "across all servers" now actually mutes everywhere** ([#1262])

## [3.1.0] - 2026-06-28

Full entries: [docs/changelog/3.1.md](docs/changelog/3.1.md)

### Added

- **Lite and Dashboard: a shared block-chain viewer reconstructs the full apex â†’ victim blocking tree** ([#1207])
- **Lite and Dashboard: a shared deadlock graph viewer shows the waits-for cycles** ([#1216])
- **Lite and Dashboard: an always-on DMV blocking-snapshot fallback keeps blocking visible without the blocked-process report** ([#1227])
- **Lite and Dashboard: analysis findings are grouped into trackable incidents on the Recommendations surface** ([#1214])
- **Lite and Dashboard: FinOps storage analysis becomes an object-growth â†’ index drill with in-grid heatmaps** ([#1138], [#1135])
- **Lite and Dashboard: per-server override for the alert delivery mode** ([#1236], [#1141])
- **Lite and Dashboard: MCP tools return a structured status envelope for non-data outcomes** ([#1224])
- **Lite and Dashboard: in-app plan navigation on every query-identifying surface** ([#1184])
- **Full Dashboard: a "Collection Stopped" alert for when the collector itself goes dark** ([#1246])

### Changed

- **Lite and Dashboard: the recommendation engine's operator advice is rebuilt to be sourced and composed from your server's own facts** ([#1189], [#1196], [#1197], [#1198], [#1203], [#1244])
- **Lite and Dashboard: click a chart's legend key (or a series line) to isolate that series**
- **Lite and Dashboard: alert payloads carry a stable dedup fingerprint and involved-object list** ([#1140], [#1154], [#1141], [#1145])
- **Lite and Dashboard: optional per-event notification mode for deadlocks and blocking** ([#1141], [#1140])
- **Lite and Dashboard: the low-disk (Volume Free Space) alert is now severity-graded instead of always INFO** ([#1136])
- **Lite and Dashboard: the execution-plan viewer is now one shared control** ([#1205])
- **Lite and Dashboard: "Show Active Queries at This Time" right-click drill-down now works on every resource chart** ([#1208], [#682])
- **Lite and Dashboard: the Overview correlated-timeline lanes get the same Active Queries drill-down** ([#1210], [#1208])
- **Dashboard: the Queries and Resource tabs load much faster** ([#1181], [#1182], [#1190])
- **Lite and Dashboard: the Active Queries view refreshes when you open it** ([#1183])
- **Lite and Dashboard: resolved/cleared tray toasts are themed to match the condition cards** ([#1186])

### Fixed

- **Dashboard: analysis findings persist their database name again, and the latest-run read returns the remediation action** ([#1262])
- **Lite and Dashboard: Collection Health no longer flags skip-if-unchanged collectors as STALE/NEVER_RUN** ([#1248], [#1246])
- **Lite and Dashboard: the Overview's empty Blocking/Deadlocking lane now renders as a live grid instead of a dead black box** ([#1245])
- **Lite and Dashboard: corrected wrong and misleading recommendation advice, plus a MAXDOP code bug** ([#1185], [#1187], [#1192], [#1194])
- **Lite: the Alert History Value/Threshold columns no longer show a raw full-precision float** ([#1134])
- **Dashboard: muted and resolved alerts no longer inflate the sidebar Alert badge** ([#1226], [#1228])
- **Lite and Dashboard: Alert History rows are classified by one shared metric classifier, fixing the "Restored" row styling** ([#1228])
- **Lite and Dashboard: the Capture Down and Failed Agent Job alerts render at their real severity in email and webhook notifications** ([#1229], [#1136])
- **Lite and Dashboard: webhook (Teams/Slack) alerts no longer re-fire after an app restart** ([#1145])
- **Lite and Dashboard: alert cooldown is keyed on the [#1140] fingerprint, not just (server, metric)** ([#1154])
- **Lite: the `index_object_stats` collector no longer times out and returns zero index data on larger estates** ([#1135])
- **Dashboard: the execution-plan node "actual of estimated rows" badge no longer over-reports for multi-execution operators** ([#1205])
- **Lite: a fresh install no longer floods collection and the wait-stats tab with benign waits** ([#1240])
- **Lite: picker-chart trends no longer run an N+1 query loop** ([#1209])
- **Lite and Dashboard: upgrading the desktop app now replaces the running tray instance instead of surfacing the stale one** ([#1148])
- **Lite: the Blocking tab's ~930ms hitch and interactive UI-thread stalls are removed** ([#1193], [#1202])
- **Dashboard: "View Plan" was a silent no-op on several surfaces** ([#1181], [#1190])
- **Dashboard: the FinOps Database Sizes tab no longer shows blank until a manual refresh** ([#1179])
- **Lite and Dashboard: failed-Agent-job alerts no longer coalesce or replay after a restart** ([#1157], [#1173], [#1140], [#1145])
- **Dashboard: anomaly and baseline detection corrected to match Lite's already-fixed behavior** ([#1155])

## [3.0.0] - 2026-06-15

Full entries: [docs/changelog/3.0.md](docs/changelog/3.0.md)

### Important

- **Major release â€” 2.11.0 â†’ 3.0.0, no breaking changes.**

### Fixed

- **Lite and Dashboard: Azure SQL Database shows its real product name in FinOps â†’ Server Inventory**
- **Dashboard: "Deadlocks Cleared" no longer flaps right after every deadlock** ([#1091])
- **Lite: blocking and deadlock alerts no longer re-fire for the same events every cooldown** ([#1091])
- **Lite and Dashboard: low-disk (Volume Free Space) alert no longer re-fires every cooldown for a standing full volume** ([#1091])
- **Lite and Dashboard: low-disk and failed-Agent-job conditions now light the server tab badge** ([#754], [#749])
- **Lite: blocking/deadlock XE sessions now self-heal and failures are surfaced** ([#1086])
- **Dashboard: blocking/deadlock XE sessions self-heal, Azure SQL DB sessions are actually created, and a missing session raises a Capture Down alert** ([#1086])
- **Blocked-process and deadlock XML processors no longer loop on un-parseable events**
- **Lite and Dashboard UI no longer goes blank or disappears after sleep/wake** ([#1050])
- **"Silence All Alerts" now suppresses email too** ([#1035])
- **Dashboard time labels are now consistently 24-hour** ([#1012])
- **Lite UI no longer freezes during archival** ([#979])
- **Lite FinOps no longer recommends an edition downgrade on an Availability Group secondary** ([#980])
- **Lite alert emails no longer re-fire after an app restart** ([#981])
- **Dashboard alert emails no longer re-fire after an app restart** ([#981])
- **Analysis-finding notification cooldowns now persist across restarts on both Lite and Dashboard**
- **Data Retention job no longer fails with `xp_delete_file` error 22049** ([#972])
- **Codebase-wide correctness and security hardening pass**
- **FinOps no longer recommends downgrading to Standard Edition on a server running Availability Groups** ([#1085], [#980])
- **Server-tab alert badge is now clearable** ([#1092], [#1122])
- **Long-running-query alert no longer constantly trips on CDC capture jobs** ([#1096])

### Changed

- **Plan parsing / analysis extracted to shared library `PerformanceMonitor.PlanAnalysis`**
- **`PlanIconMapper` split to break a shared-library WPF dependency**
- **Analysis engine extracted to shared library `PerformanceMonitor.Analysis`**
- **Trace files are now bounded at the source** ([#972])
- **Blocked-process reports expose blocker-side fields as typed columns**
- **Blocking-chain reconstruction now reads typed columns from `collect.blocking_BlockedProcessReport`**
- **Analysis minimum-data threshold lowered to 24 hours**
- **Major UI-responsiveness overhaul â€” the data path now runs off the WPF dispatcher in both apps** ([#1121], [#1116], [#1050])

### Added

- **`tools/Remove-OrphanedTraceFiles.ps1`** ([#972])
- **`FactAdvice` and `FactRemediation` in `PerformanceMonitor.Analysis`**
- **Object- and index-level collection: sizes, growth, usage, and locking/contention** ([#1103])
- **Recommendations / Apply Fix engine (advise-and-act rebuild)**
- **Low volume free-space alert** ([#754])
- **Failed SQL Agent job alert** ([#749])
- **Installer: optional custom data/log file locations** ([#768])

[#2862]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2862
[#2849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2849
[#2843]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2843
[#2818]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2818
[#1564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1564
[#2833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2833
[#2825]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2825
[#2820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2820
[#2802]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2802
[#2814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2814
[#2807]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2807
[#2788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2788
[#749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/749
[#754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/754
[#768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/768
[#972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/972
[#979]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/979
[#980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/980
[#981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/981
[#989]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/989
[#1012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1012
[#1035]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1035
[#1048]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1048
[#1050]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1050
[#1180]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1180
[#1085]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1085
[#1086]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1086
[#1091]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1091
[#1092]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1092
[#1096]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1096
[#1103]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1103
[#1116]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1116
[#1121]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1121
[#1122]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1122
[#1134]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1134
[#1135]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1135
[#1136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1136
[#1205]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1205
[#1208]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1208
[#1210]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1210
[#1140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1140
[#1141]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1141
[#1145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1145
[#1154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1154
[#1226]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1226
[#1228]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1228
[#1229]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1229
[#1138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1138
[#1207]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1207
[#1209]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1209
[#1214]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1214
[#1216]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1216
[#1224]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1224
[#1227]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1227
[#1236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1236
[#1240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1240
[#1185]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1185
[#1187]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1187
[#1189]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1189
[#1192]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1192
[#1194]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1194
[#1196]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1196
[#1197]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1197
[#1198]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1198
[#1203]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1203
[#1244]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1244
[#1245]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1245
[#1246]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1246
[#1248]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1248
[#1262]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1262
[#1479]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1479
[#1480]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1480
[#1481]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1481
[#1483]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1483
[#1485]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1485
[#1484]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1484
[#1488]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1488
[#1539]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1539
[#1486]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1486
[#1540]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1540
[#1517]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1517
[#1346]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1346
[#1535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1535
[#1251]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1251
[#1510]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1510
[#1507]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1507
[#1506]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1506
[#1509]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1509
[#1512]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1512
[#1513]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1513
[#1514]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1514
[#1515]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1515
[#1543]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1543
[#1544]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1544
[#1545]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1545
[#1542]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1542
[#1541]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1541
[#1538]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1538
[#1548]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1548
[#1546]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1546
[#1547]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1547
[#1549]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1549
[#1550]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1550
[#1551]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1551
[#1552]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1552
[#1553]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1553
[#1554]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1554
[#1556]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1556
[#1557]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1557
[#1558]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1558
[#1559]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1559
[#1560]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1560
[#1565]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1565
[#1566]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1566
[#1567]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1567
[#1569]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1569
[#1570]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1570
[#1572]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1572
[#1571]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1571
[#1574]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1574
[#1577]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1577
[#1576]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1576
[#1579]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1579
[#1580]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1580
[#1582]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1582
[#1128]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1128
[#1581]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1581
[#1583]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1583
[#1585]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1585
[#1586]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1586
[#1588]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1588
[#1584]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1584
[#1589]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1589

[#1590]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1590
[#1592]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1592
[#1593]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1593
[#1594]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1594
[#1596]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1596
[#1597]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1597
[#1598]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1598
[#1599]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1599
[#1608]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1608
[#1609]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1609
[#1612]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1612
[#1613]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1613
[#1614]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1614
[#1616]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1616
[#1619]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1619
[#1620]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1620
[#1622]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1622
[#1623]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1623
[#1624]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1624
[#1625]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1625
[#1626]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1626
[#1627]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1627
[#1631]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1631
[#1629]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1629
[#1630]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1630
[#1632]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1632
[#1633]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1633
[#1634]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1634
[#1635]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1635
[#1636]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1636
[#1637]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1637
[#1639]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1639
[#1674]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1674
[#1668]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1668
[#1670]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1670
[#1675]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1675
[#1677]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1677
[#1683]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1683
[#1689]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1689
[#1676]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1676
[#1640]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1640
[#1642]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1642
[#1646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1646
[#1647]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1647
[#1648]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1648
[#1651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1651
[#1652]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1652
[#1617]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1617
[#1621]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1621
[#1601]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1601
[#1604]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1604
[#1602]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1602
[#1600]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1600
[#1578]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1578
[#1536]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1536
[#1534]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1534
[#1533]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1533
[#1532]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1532
[#1531]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1531
[#1530]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1530
[#1529]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1529
[#1528]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1528
[#1527]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1527
[#1526]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1526
[#1525]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1525
[#1524]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1524
[#1523]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1523
[#1522]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1522
[#1521]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1521
[#1520]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1520
[#1519]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1519
[#1518]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1518
[#1516]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1516
[#1505]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1505
[#1504]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1504
[#1503]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1503
[#1502]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1502
[#1501]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1501
[#1500]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1500
[#1499]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1499
[#1498]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1498
[#963]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/963
[#1495]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1495
[#1494]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1494
[#1493]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1493
[#1492]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1492
[#1491]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1491
[#1487]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1487
[#1441]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1441
[#1490]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1490
[#1478]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1478
[#1477]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1477
[#1476]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1476
[#1475]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1475
[#1474]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1474
[#1473]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1473
[#1471]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1471
[#1463]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1463
[#1489]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1489
[#1042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1042
[#1268]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1268
[#1321]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1321
[#1286]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1286
[#1470]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1470
[#1148]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1148
[#1155]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1155
[#1157]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1157
[#1173]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1173
[#1179]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1179
[#1181]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1181
[#1182]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1182
[#1183]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1183
[#1184]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1184
[#1186]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1186
[#1190]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1190
[#1193]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1193
[#1202]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1202

## [2.11.0] - 2026-05-19

### Important

- **.NET 10 upgrade** â€” Dashboard, Lite, Installer, and Installer.Core now target `net10.0` (Windows projects target `net10.0-windows`). Building from source now requires the .NET 10 SDK; CI is pinned to 10.0.204 via `global.json` for reproducible builds. End users running prebuilt Velopack installers do not need to install anything separately â€” runtime is bundled ([#958])
- **Setup.exe is now the recommended install path** for Dashboard and Lite â€” the README steers users to the Velopack `Setup.exe`, which installs to `%LocalAppData%`, registers the apps under Apps & Features, creates Start Menu and Desktop shortcuts, and wires up auto-update. Portable ZIPs are still produced for both apps (CI release pipeline and local build scripts) as a fallback for advanced or air-gapped users. The Installer ZIP (CLI installer + SQL scripts) is unchanged
- **Shared `servers.json` location** â€” Dashboard and Lite now store `servers.json` under `%ProgramData%\PerformanceMonitor{Dashboard,Lite}\` so every Windows user on the same machine shares one server list. First run migrates an existing per-user `servers.json` to the new location and grants Authenticated Users Modify on the directory. SQL credentials remain per-user in Windows Credential Manager â€” each DBA re-enters SQL passwords on first connect; Windows Auth works with no re-entry

### Added

- **One-click snooze from the alert tray popup** in Lite â€” snooze an alert directly from the tray notification balloon without opening the main window ([#944])
- **Snooze hint in email and Teams/Slack alert payloads** â€” alert messages now show the snooze duration / scheduled wake time when an alert is fired while a snooze is active ([#944])
- **Process memory logging per collection cycle** in Lite â€” the collector now logs working set and private bytes at the end of each cycle, making it easier to track memory growth in long-running sessions

### Changed

- **Lite compaction memory tuning** ([#933]) â€” multiple changes to make parquet compaction robust on wide-row tables and large datasets:
    - Cap the main collector connection's `memory_limit` and raise it transiently only for the `COPY` step
    - Detect compaction `EXCLUDE` columns per merge step instead of once up front
    - Raise the compaction `memory_limit` floor to 4 GB
    - Set DuckDB `temp_directory` explicitly so spill files don't blow the OS temp drive
    - Compact parquet in size-budgeted batches instead of one mega-batch
- **Trace collectors honor `config.collector_database_exclusions`** ([#887] follow-up) â€” the trace-file based collectors now filter against the exclusions table, matching the behavior of the eight DMV-based per-database collectors shipped in v2.9.0
- **InstallerGui project directory removed** â€” the WPF InstallerGui was retired in v2.9.0 in favor of the Dashboard's integrated Add Server dialog. The project directory has now been deleted from the repo
- **Build warnings cleaned up** across Lite, Dashboard, and Installer ([#945])
- **GitHub Actions runners bumped** to Node 24-compatible major versions to silence deprecation warnings

### Fixed

- **Re-run `installation_history` column widening** for servers that crossed v2.4.0 â†’ v2.5.0 before PR #828's fix shipped in v2.7.0. Those servers ran the original widen script as a no-op against `master`, then advanced their installer_version past 2.5, so the now-fixed script never reapplied. Adds an idempotent ALTER under an `IF EXISTS` guard checking `max_length = 510` ([#828])
- **Mute rules preserved across size-triggered DuckDB reset** in Lite â€” when the local DuckDB exceeded the configured size budget and was reset, mute rules were being lost. They now survive the reset ([#938])
- **Chart tooltips break after tab switch** â€” root-cause fix for the popup-wedge issue first patched in v2.10.0. Both the Memory tab handlers and `CorrelatedCrosshairManager` are now resilient to tab churn ([#916], [#937])
- **Stale `Monitor_LongQueries_*.trc` files cleaned up** by `config.data_retention` â€” the trace-file cleanup step previously left old `.trc` files behind on disk ([#951])
- **Nullability guards** added to the remaining comparison overlay tasks that were producing CS86xx warnings

[#916]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/916
[#933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/933
[#937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/937
[#938]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/938
[#944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/944
[#945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/945
[#951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/951
[#958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/958

## [2.10.0] - 2026-05-04

### Fixed

- **Memory tab tooltip** stops working after switching away and returning to the tab. Both Dashboard and Lite Memory tab crosshair tooltip handlers now reattach correctly on tab re-entry; the same popup-wedge fix is also applied to `CorrelatedCrosshairManager` ([#916])
- **FinOps memory recommendation** now bases sizing on a 7-day P95 of memory samples instead of a single snapshot, so recommendations no longer swing based on instantaneous workload state. Applied in both Dashboard and Lite ([#917])

### Changed

- **Per-database grants for FinOps Index Analysis** documented in the README â€” sp_IndexCleanup-backed Index Analysis requires per-database `EXECUTE` grants on each user database you want to analyze ([#915])

[#915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/915
[#917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/917

## [2.9.0] - 2026-04-29

### Important

- **Breaking change to `config.data_retention`** â€” the `@truncate_all` parameter has been removed. Pass `@retention_days = 0` for the same behavior. `@retention_days = NULL` (default) respects per-collector retention from `config.collection_schedule` with a 30-day fallback for unscheduled tables; `@retention_days = N > 0` overrides every table to N days. Any existing Agent jobs or scripts calling `data_retention @truncate_all = 1` need to be updated ([#900])
- **New `config.collector_database_exclusions` table** for per-database collector exclusions. Eight per-database collectors filter against this table; system databases remain hard-skipped by the collectors themselves. Existing installs get the table on the next upgrade â€” `install/01_install_database.sql` and `config.ensure_config_tables` both create it under an `IF OBJECT_ID â€¦ IS NULL` guard ([#887])

### Added

- **Per-database collector exclusions** â€” exclude noisy or unimportant databases from per-database collectors. Dashboard side adds `config.collector_database_exclusions` and filters 8 collectors (`query_stats`, `query_store`, `procedure_stats`, `file_io_stats`, `waiting_tasks`, `database_configuration`, `database_size_stats`, `server_properties`). Lite side adds an `ExcludedDatabases` list per server in `servers.json` and filters 9 collectors ([#887])
- **`Off` collection preset** â€” `EXECUTE config.apply_collection_preset @preset_name = N'Off'` disables every collector in one call. Pair with a second Agent job that applies a non-`Off` preset at the start of your active window for overnight / quiet-hours scoping. Non-`Off` presets now also set `enabled = 1` across the board so the switch from `Off â†’ Balanced` reliably resumes collection ([#888])
- **Purge Now action** in Manage Servers â€” confirm dialog with a mode picker (Use configured / 1 / 3 / 7 / Custom / All) drives `config.data_retention`; right-click menu on the Manage Servers grid mirrors every per-row action (Edit, Toggle Favorite, Check Server Version, Purge Now, Remove) ([#900])
- **Total non-idle CPU on Lite Overview** â€” headline value shows total CPU with the SQL-only value alongside (e.g. `64% (SQL 60%)`); new `CpuAlertMode` dropdown in Settings â†’ Alerts (Total / SqlOnly) drives both the alert evaluator and headline color; tray notifications and email alerts label the value as "Total CPU" or "SQL CPU" ([#899])
- **Resume gap detection** â€” `query_stats`, `procedure_stats`, and `query_store` collectors skip the historical sweep on first run after an Off preset, Agent stoppage, or server reboot. When the last successful run is older than 5Ã— the configured `frequency_minutes` (floored at 30 minutes), the cutoff clamps to `SYSDATETIME()` so only forward-going data is collected on resume â€” preventing the tempdb blowout that hit the original reporter ([#892])
- **Right-click View Plan** on Dashboard Blocked Process Reports (View Blocked Plan + View Blocking Plan), Dashboard Deadlocks, and Lite Deadlocks grids. Plan lookup hits `sys.dm_exec_query_stats` + `sys.dm_exec_text_query_plan` on the monitored server, falling back to `executionStack/frame` entries when the process-level `sql_handle` is empty or evicted ([#880])
- **Open Log Folder** sidebar button in Lite â€” opens `%LocalAppData%\PerformanceMonitorLite\logs\` in Explorer for grabbing historical logs to attach to bug reports. Sits below View Log, which retains its existing behavior of opening today's log file ([#873])
- **Installed Version column** in the Manage Servers grid for both Dashboard and Lite. Dashboard shows the PerformanceMonitor database version on each server (probed in parallel via `GetInstalledVersionAsync`, with `Not installed` / `Unavailable` fallbacks). Lite shows the running app's own version on every row, mirroring Full's column header for consistency.
- **Lite-style server card indicators in Full** â€” back-ported the Ellipse-with-DataTriggers status dot (Online/Offline/Warning/Unknown) and the right-aligned favorite star from Lite to the Full Dashboard's server list, matching Lite's visual treatment.
- **Architecture overview** at `docs/how-collection-works.md` covering the minute loop, dispatcher, collector shape, `config.collection_schedule`, retention, and the Dashboard read path

### Changed

- **PlanIconMapper synced** with PerformanceStudio v1.9.0 improvements â€” columnstore storage type on scan/delete/insert/update/merge operators routes to `columnstore_index_*` icons (covers CCI and NCCI); `Parallelism` operator subtypes (Repartition Streams, Distribute Streams, Gather Streams) get their own icons
- **`Microsoft.Data.SqlClient` 6.1.4 â†’ 7.0.1** â€” major-version bump. Azure/Entra dependencies were split out of the core package in 7.0; `Microsoft.Data.SqlClient.Extensions.Azure 1.0.0` added to Dashboard, Lite, and Installer.Core for `ActiveDirectoryInteractive` connections
- **`ModelContextProtocol` 0.7.0-preview.1 â†’ 1.2.0** â€” off the preview tag and onto stable 1.x in Dashboard and Lite
- **`DuckDB.NET` 1.5.0 â†’ 1.5.2** in Lite â€” fixes unbounded row group growth on indexed tables under repeated load+insert cycles, memory leaks and race conditions in prepared statements, WAL checkpoint marking, and Windows UTF-8/UTF-16 handling
- **`Microsoft.Extensions.*` 10.0.5 â†’ 10.0.7**, **`System.Text.Json` 10.0.5 â†’ 10.0.7**, **`ScottPlot.WPF` 5.1.57 â†’ 5.1.58** â€” patch-level bumps with no expected behavioral change
- **Theme polish** on grids and plan viewer in Dashboard and Lite â€” thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva) ([#889])

### Fixed

- **Install loop timeout** raised from 5 minutes to 1 hour. `install/98_validate_installation.sql` runs every enabled collector with `@debug = 1` in a single batch; on large databases (reporter had 7.2M rows in `collect.query_stats`, 4.4M in `collect.query_store_data`) this took ~9 minutes and was blowing the 5-minute timeout, failing the install or upgrade ([#884])

[#873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/873
[#880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/880
[#884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/884
[#887]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/887
[#888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/888
[#889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/889
[#892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/892
[#899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/899
[#900]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/900

## [2.8.0] - 2026-04-22

### Important

- **New nonclustered indexes** on `collect.query_stats`, `collect.procedure_stats`, and `collect.query_store_data` to eliminate Eager Index Spools in Dashboard grid queries. On large installations these indexes may take several minutes to build; the upgrade script uses `ONLINE = ON` on Enterprise/Developer/Azure editions and falls back to offline on Standard/Web ([#835])

### Added

- **Memory Pressure Events in Lite** â€” the collector, chart, and `get_memory_pressure_events` MCP tool previously only in the Full Edition are now available in Lite ([#865])
- **Grid auto-scrolling** in Lite and Dashboard ([#843]) â€” thanks [@ClaudioESSilva](https://github.com/ClaudioESSilva)

### Changed

- **PlanAnalyzer and BenefitScorer** synced with PerformanceStudio's Apr 9â€“16 improvements
- **Query/Procedure/Query Store stats** refactored to a phased DECOMPRESS approach; removed unhelpful `WAITFOR DECOMPRESS` filters
- **Query/Procedure/Query Store grids** capped to TOP 500 to prevent UI freezes on large datasets
- **Server tabs lazy-load** â€” only the visible server tab loads on startup; remaining tabs load on first visit
- **Webhook URLs (Dashboard)** encrypted with DPAPI via Windows Credential Manager â€” Lite webhook URLs remain in plaintext settings for now
- **DuckDB queries hardened** â€” parameterized values, escaped paths, fixed `IsArchiving` race
- **Lite chart axes and sub-tab styling** polished, then ported to Dashboard

### Fixed

- **Memory Pressure Events chart filter** was dropping valid rows; added MCP interpretation guidance ([#865])
- **FinOps recommendation severity sort order** in Lite and Dashboard ([#872])
- **Overview crosshair** disappearing after tab switches or layout passes
- **Blocked process report plan lookup** returning the wrong plan ([#867])
- **FinOps TDE recommendation** flagging Standard edition on SQL Server 2019+ where TDE is free ([#854])
- **Azure SQL DB collector** falls back to single-database mode when `master` is inaccessible ([#857])
- **Azure SQL DB query snapshots** scoped to the current database ([#857])
- **Azure SQL DB query snapshot prefilter** â€” request set is narrowed into `#temp` before joining DMVs to avoid Azure-specific execution plan issues ([#857])
- **Azure SQL DB live query plans** â€” now skipped gracefully instead of erroring ([#857])
- **Azure SQL DB memory_stats collector** â€” dropped `sys.dm_os_schedulers` which is blocked on elastic-pool contained users regardless of DB-scoped grants ([#857])
- **Non-transient permission denials** now stop collector retries instead of looping forever ([#857])

[#835]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/835
[#843]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/843
[#854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/854
[#857]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/857
[#865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/865
[#867]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/867
[#872]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/872

## [2.7.0] - 2026-04-13

### Added

- **Host OS column** in Server Inventory for both Dashboard and Lite ([#748], [#823])
- **Offline community script support** via `community/` directory for user-contributed scripts ([#814], [#822])
- **MultiSubnetFailover connection option** in Dashboard and Lite for Always On availability groups ([#813], [#821])

### Changed

- **PlanAnalyzer and ShowPlanParser** synced from PerformanceStudio with latest improvements ([#816])
- **MCP query tools** optimized for large databases ([#826])
- **Add Server dialog UX** improved with inline connection status and full-height window
- **"CPUs" renamed to "Logical CPUs"** for clarity in Lite ([#825])

### Fixed

- **Dashboard auto-refresh stalling under load** â€” replaced DispatcherTimer with async Task.Delay loop to prevent priority starvation during heavy chart rendering ([#833], [#834])
- **Lite auto-refresh silently skipping** every tick ([#824])
- **Deadlock count not resetting** between collections ([#803], [#820])
- **Upgrade filter skipping patch versions** during version comparison ([#817], [#819])
- **Upgrade script executing against master** instead of PerformanceMonitor database ([#828])
- **Duplicate release builds** triggering on both created and published events

[#748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/748
[#803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/803
[#813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/813
[#814]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/814
[#816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/816
[#817]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/817
[#819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/819
[#820]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/820
[#821]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/821
[#822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/822
[#823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/823
[#824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/824
[#825]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/825
[#826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/826
[#828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/828
[#833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/833
[#834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/834

## [2.6.0] - 2026-04-08

### Added

- **Correlated timeline lanes** on Lite Overview and Dashboard â€” synchronized CPU, memory, waits, and TempDB trend lanes for at-a-glance correlation ([#688])
- **Dynamic baselines and anomaly detection** in Lite and Dashboard â€” automatic baseline calculation with anomaly highlighting on key metrics ([#692], [#693])
- **Query grid comparison** â€” before/after comparison mode for query grids in Lite and Dashboard with global Compare dropdown ([#687])
- **Nonclustered index count badge** on modification operators in plan viewer ([#788])
- **Upgrade detection in Edit Server** dialog â€” see pending upgrades without adding a new server ([#772])
- **CLI installer interactive mode** prompts for trust-cert and encryption settings ([#784])
- **SignPath code signing** â€” release binaries are now digitally signed via the [SignPath FOSS](https://signpath.io) program

### Changed

- **PlanAnalyzer Rule 3 (Serial Plan)** comprehensively refined â€” severity demotion for TRIVIAL and 0ms plans, `CouldNotGenerateValidParallelPlan` treated as actionable, all 25 `NonParallelPlanReason` values now covered
- **PlanAnalyzer warning rules** ported from PerformanceStudio improvements
- **Text readability** â€” replaced all muted/dim text colors with full foreground colors for readability

### Fixed

- **Embedded resource upgrade discovery** broken â€” upgrades silently returned zero results for Dashboard installs ([#772])
- **Archive compaction OOM** on large parquet groups
- **CLI installer argument parsing** treating flags as positional args ([#786])
- **Lite long-running query alerts** firing on stale DuckDB snapshots
- **FinOps Enterprise feature detection** now queries all databases and filters to TDE only ([#780])
- **Second launch error** â€” now brings existing window to foreground instead ([#769])
- **Overview tab Memory Grant** showing 0 for all timestamps ([#776])
- **Lite FinOps Enterprise features** query error on servers without `database_id` column ([#777])
- **Collector health status** incorrect for on-load collectors
- **CSV and clipboard exports** writing `System.Windows.Controls.StackPanel` as column headers instead of actual header text ([#805])

[#687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/687
[#688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/688
[#692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/692
[#693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/693
[#769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/769
[#772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/772
[#776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/776
[#777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/777
[#780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/780
[#784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/784
[#786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/786
[#788]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/788
[#805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/805

## [2.5.0] - 2026-03-30

### Important

- **InstallerGui retired**: The standalone GUI installer has been removed. Installation, upgrade, and uninstall are now handled directly from the Dashboard's Add Server dialog, powered by the new Installer.Core shared library. The CLI installer continues to work as before. ([#755])

### Added

- **Dashboard integrated installer** â€” Add Server dialog now installs, upgrades, and uninstalls PerformanceMonitor directly, replacing the standalone InstallerGui ([#755])
- **Installer.Core shared library** â€” shared installation logic used by both the CLI installer and Dashboard ([#755])
- **Overview tab** for Lite with 2x2 resource chart grid (CPU, Memory, Wait Stats, TempDB) ([#689])
- **Chart drill-down** on CPU, Memory, TempDB, Blocking, and Deadlock charts in both Dashboard and Lite â€” right-click any chart point to jump to Active Queries for that time window ([#682])
- **Grid-to-slicer overlay** for Query Stats, Procedure Stats, and Query Store tabs â€” click a row to overlay its trend on the slicer chart ([#683])
- **Query heatmap** tab in both Dashboard and Lite â€” visual heat map of query activity over time ([#739], [#743])
- **Webhook notifications** for alerts â€” configurable webhook endpoint for alert delivery ([#725])
- **Per-server collector schedule intervals** â€” customize collection frequency per server ([#703])
- **Investigate button** in Critical Issues grid â€” jump directly to relevant tab from an alert ([#684])
- **Dismiss Selected** context menu and View Log sidebar button for alert management ([#718], [#740])
- **Alert archival awareness** â€” dismissed_archive_alerts sidecar table, source column for live vs archived alerts, stale-data indicator, structured telemetry ([#718])
- **Dashboard read-only connection intent** â€” connections use `ApplicationIntent=ReadOnly` where supported ([#728])
- FUNDING.yml for GitHub Sponsors ([#752])

### Changed

- **Installer architecture** refactored: CLI installer is now a thin wrapper over Installer.Core ([#755])
- **DuckDB memory capped** at 2 GB during parquet compaction to prevent out-of-memory on large archives ([#758])
- **Text rendering** improved with `TextOptions.TextFormattingMode="Display"` for sharper text ([#710])
- **installation_history version columns** widened from nvarchar(255) to nvarchar(512) to handle long @@VERSION strings ([#712])

### Fixed

- **Memory leaks in Lite** â€” delta cache, event handlers, and chart helpers properly disposed ([#758])
- **Doomed transaction errors** in delta framework and ensure_collection_table â€” ROLLBACK now occurs before error logging ([#756])
- **XACT_STATE check** added after third-party stored procedure calls (sp_HumanEventsBlockViewer, sp_BlitzLock) to prevent doomed transaction errors ([#695])
- **CREATE DATABASE failure** when model database has large default file sizes ([#676])
- **CPU metrics mixed** for different Azure SQL databases on the same logical server ([#680])
- **Azure SQL DB vCore** FinOps calculations incorrect for serverless/vCore tiers ([#736])
- **Webhook alert recording** not persisting correctly ([#726])
- **Drill-down timezone** misalignment between chart and detail view ([#747], [#750])
- **Drill-down refresh** losing context on auto-refresh ([#744])
- **Drill-down target** incorrectly routing Memory to Memory Grants instead of Active Queries ([#706])
- **Heatmap colorbar stacking** when switching between servers ([#746])
- **Display mode pickers** not reflecting current state on tab switch ([#751])
- **Slicer custom range** handling and sub-hour display issues ([#704])
- **Overlay selection** lost on Dashboard auto-refresh ([#683])
- **Numeric values** in alert details treated as strings instead of numbers ([#732])
- **FinOps VM right-sizing** query error â€” `PERCENTILE_CONT` missing required `OVER()` clause
- **FinOps Enterprise features** query error on AWS RDS â€” `database_id` column not present in `sys.dm_db_persisted_sku_features` on RDS
- **FinOps right-click copy** broken on all Dashboard FinOps grids â€” context menu walked to row instead of grid
- **FinOps recommendation error logs** now include server name for easier troubleshooting

### Deprecated

- **InstallerGui** â€” removed from the solution and build pipeline. Use the Dashboard or CLI installer instead. ([#755])

[#676]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/676
[#680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/680
[#682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/682
[#683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/683
[#684]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/684
[#689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/689
[#695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/695
[#703]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/703
[#704]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/704
[#706]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/706
[#710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/710
[#712]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/712
[#718]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/718
[#725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/725
[#726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/726
[#728]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/728
[#732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/732
[#736]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/736
[#739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/739
[#740]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/740
[#743]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/743
[#744]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/744
[#746]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/746
[#747]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/747
[#750]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/750
[#751]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/751
[#752]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/752
[#755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/755
[#756]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/756
[#758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/758

## [2.4.0] - 2026-03-23

### Important

- **Lite data directory moved**: Lite now stores all data (config, DuckDB, archives, logs) in `%LOCALAPPDATA%\PerformanceMonitorLite\` instead of alongside the executable. This enables auto-update support. Existing users upgrading from the zip should use **Import Settings** and **Import Data** to bring over their configuration and historical data from the old install folder.
- **Auto-update (Windows)**: Both Dashboard and Lite now include Velopack auto-update. Users who install via the new Setup.exe will receive update notifications and can download + apply updates from within the app. Existing zip distribution continues to work as before.

### Added

- **Velopack auto-update** for Dashboard and Lite â€” check on startup, download + apply from About window with confirmation dialog before restart ([#635])
- **Per-tab time range slicers** on Dashboard and Lite query tabs â€” filter data directly on each tab without changing global time range ([#655], [#662])
- **Time display picker** (Local/UTC/Server) in Dashboard and Lite toolbars ([#646])
- **Import Settings** â€” renamed from "Import Connections", now also copies `settings.json`, `collection_schedule.json`, `ignored_wait_types.json`, and `alert_state.json` from a previous install
- **Alert muting improvements** â€” pre-fill context fields (database, query, wait type, job name) from alert detail text, configurable default expiration for new mute rules, tooltip on query text field ([#642])
- **Missing date columns** on Query Stats and Procedure Stats tabs (`creation_time`, `last_execution_time`) ([#649], [#651], [#654])
- **Trace pattern drill-down** now includes `CollectionTime` and `NtUserName` columns ([#663])
- **DataGrid sort preservation** across auto-refresh â€” sort order no longer resets when data refreshes ([#659])
- **CLI installer**: colored output (green/red/yellow) and version check on startup ([#639])
- **GUI installer**: version check on startup
- **Growth rate and VLF count** columns in Database Sizes (from v2.3.0 nightly, now in upgrade path) ([#567])
- `llms.txt` and `CITATION.cff` for project discoverability ([#630])

### Changed

- **Lite data directory** moved to `%LOCALAPPDATA%\PerformanceMonitorLite\` for Velopack compatibility
- **Delta gap detection** added to all cumulative-counter collectors (file I/O, wait stats, query stats, procedure stats, memory grants) â€” prevents inflated spikes after app restart ([#633])
- **File I/O NULL fallbacks** improved when `sys.master_files` is inaccessible â€” falls back to `DB_NAME()` and `File_{id}` instead of generic "Unknown" ([#633])
- **Running jobs collector** skipped gracefully when login lacks msdb access ([#656])
- NuGet packages updated to latest minor versions ([#653])

### Fixed

- **Installer writing SUCCESS when files fail** â€” CLI tolerated 1 failure in automated mode, GUI had a similar workaround. Now any failure = not success.
- **Query stats collector causing SQL dumps** on passive mirror servers â€” removed `dm_exec_plan_attributes` CROSS APPLY, uses temp table of ONLINE database IDs instead ([#632])
- **Trigger name extraction** fails when comment before `CREATE TRIGGER` contains " ON " ([#666])
- **FinOps expensive queries** DuckDB error â€” query referenced `statement_start_offset` column that doesn't exist in schema
- **Imported parquet files** not recognized by archive compaction â€” added regex patterns for `imported_` prefix
- **Auto-refresh after Import Data** â€” views now refresh immediately after import completes

[#630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/630
[#632]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/632
[#633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/633
[#635]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/635
[#639]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/639
[#642]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/642
[#646]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/646
[#649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/649
[#651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/651
[#653]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/653
[#654]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/654
[#655]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/655
[#656]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/656
[#659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/659
[#662]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/662
[#663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/663
[#666]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/666
[#567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/567

## [2.3.0] - 2026-03-18

### Important

- **Schema upgrade**: Six columns widened across three tables (`query_stats`, `cpu_scheduler_stats`, `waiting_tasks`, `database_size_stats`) to match DMV documentation types. These are in-place ALTER COLUMN operations â€” fast on any table size, no data migration. Upgrade scripts run automatically via the CLI/GUI installer.
- **SQL Server version check**: Both installers now reject SQL Server 2014 and earlier before running any scripts, with a clear error message. Azure MI (EngineEdition 8) is always accepted. ([#543])
- **Installer adversarial tests**: 35 automated tests covering upgrade failures, data survival, idempotency, version detection fallback, file filtering, restricted permissions, and more. These run as part of pre-release validation. ([#543])

### Added

- **ErikAI analysis engine** â€” rule-based inference engine for Lite that scores server health across wait stats, CPU, memory, I/O, blocking, tempdb, and query performance. Surfaces actionable findings with severity, detail, and recommended actions. Includes anomaly detection (baseline comparison for acute deviations), bad actor detection (per-query scoring for consistently terrible queries), and CPU spike detection for bursty workloads. ([#589], [#593])
- **ErikAI Dashboard port** â€” full analysis engine ported to Dashboard with SQL Server backend ([#590])
- **FinOps cost optimization recommendations** â€” Phase 1-4 checks: enterprise feature audit, CPU/memory right-sizing, compression savings estimator, unused index cost quantification, dormant database detection, dev/test workload detection, VM right-sizing, storage tier optimization, reserved capacity candidates ([#564])
- **FinOps High Impact Queries** â€” 80/20 analysis showing which queries consume the most resources across all dimensions ([#564])
- **FinOps dollar-denominated cost attribution** â€” per-server monthly cost setting with proportional database-level breakdown ([#564])
- **On-demand plan fetch** for bad actor and analysis findings â€” click to retrieve execution plans for flagged queries ([#604])
- **Plan analysis integration** â€” findings include execution plan analysis when plans are available ([#594])
- **Server unreachable email alerts** â€” Dashboard sends email (not just tray notification) when a monitored server goes offline or comes back online ([#529])
- **Column filters on all FinOps DataGrids** â€” filter funnel icons on every column header across all 7 FinOps grids in Lite and Dashboard ([#562])
- **Column filters on Dashboard** IdleDatabases, TempDB, and Index Analysis grids
- **Lite data import** â€” "Import Data" button brings in monitoring history from a previous Lite install via parquet files, preserving trend data across version upgrades ([#566])
- **Per-server Utility Database setting** â€” Lite can call community stored procedures (sp_IndexCleanup) from a database other than master ([#555])
- **SQL Server version check** in both CLI and GUI installers â€” rejects 2014 and earlier with a clear message ([#543])
- **Execution plan analysis MCP tools** for both Dashboard and Lite
- **Full MCP tool coverage** â€” Dashboard expanded from 28 to 57 tools, Lite from 32 to 51 tools ([#576], [#577])
- **Self-sufficient analyze_server drill-down** â€” MCP tool returns complete analysis, not breadcrumb trail ([#578])
- **NuGet package dependency licenses** in THIRD_PARTY_NOTICES.md

### Changed

- **Azure SQL DB FinOps** â€” all collectors (database sizes, query stats, file I/O) now connect to each database individually instead of only querying master. Server Inventory uses dynamic SQL to avoid `sys.master_files` dependency. ([#557])
- **Index Analysis scroll fix** â€” both summary and detail grids now use proportional heights instead of Auto, so they scroll independently with large result sets ([#554])
- **Dashboard Add Server dialog** â€” increased MaxHeight from 700 to 850px so buttons are visible when SQL auth fields are shown
- **GUI installer** â€” Uninstall button now correctly enables after a successful install
- **GUI installer** â€” fixed encryption mapping and history logging ([#612])
- **Dashboard visible sub-tab only refresh** on auto-refresh ticks ([#528])
- Analysis engine decouples data maturity check from analysis window

### Fixed

- **Installer dropping database on every upgrade** â€” `00_uninstall.sql` excluded from install file list, installer aborts on upgrade failure, version detection fallback returns "1.0.0" instead of null ([#538], [#539])
- **SQL dumps on mirroring passive servers** from FinOps collectors ([#535])
- **RetrievedFromCache** always showing False ([#536])
- **Arithmetic overflow** in query_stats collector for dop/thread columns ([#547])
- **Lite perfmon chart bugs** and Dashboard ScottPlot crash handling ([#544], [#545])
- **PLE=0 scoring bug** â€” was scored as harmless, now correctly flagged ([#543])
- **PercentRank >1.0** bug in HealthCalculator
- **6 verified Lite bugs** from code review ([#611])
- **Enterprise feature audit text** â€” partitioning is not Enterprise-only
- **FinOps collector scheduling**, server switch, and utilization bugs
- **Dashboard drill-down** Unicode arrow in story path split
- **Empty DataGrid scrollbar artifacts** â€” hide grids when empty across all FinOps tabs
- **Query preview** â€” truncated in row, full text in tooltip

[#529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/529
[#535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/535
[#536]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/536
[#538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/538
[#539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/539
[#543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/543
[#544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/544
[#545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/545
[#547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/547
[#554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/554
[#555]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/555
[#557]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/557
[#562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/562
[#564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/564
[#566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/566
[#576]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/576
[#577]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/577
[#578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/578
[#528]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/528
[#589]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/589
[#590]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/590
[#593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/593
[#594]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/594
[#604]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/604
[#611]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/611
[#612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/612

## [2.2.0] - 2026-03-11

**Contributors:** [@HannahVernon](https://github.com/HannahVernon), [@ClaudioESSilva](https://github.com/ClaudioESSilva), [@dphugo](https://github.com/dphugo), [@Orestes](https://github.com/Orestes) â€” thank you!

### Important

- **Schema upgrade**: Three large collection tables (`query_stats`, `procedure_stats`, `query_store_data`) are migrated to use `COMPRESS()` for query text and plan columns. The upgrade performs a table swap (create new â†’ migrate data â†’ rename) which may take several minutes on large tables. A `row_hash` column is added for deduplication. Three new tracking tables are also created. Volume stats columns are added to `database_size_stats`. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

  Compression results measured on a production instance:

  | Table | Compressed | Uncompressed | Ratio |
  |---|---|---|---|
  | query_stats | 18.0 MB | 339.0 MB | 18.8x |
  | query_store_data | 13.5 MB | 258.0 MB | 19.1x |
  | **Total** | **31.5 MB** | **597 MB** | **~19x** |

### Added

- **FinOps monitoring tab** â€” database size tracking, server properties, storage growth analysis (7d/30d), index analysis with unused/duplicate/compressible detection, utilization efficiency, idle database identification, and estate-level resource views ([#474])
- **Named collection presets** â€” Aggressive, Balanced, and Low-Impact schedule profiles via `config.apply_collection_preset` ([#454])
- **Entra ID interactive MFA authentication** in both CLI and GUI installers for Azure SQL MI connections ([#481])
- **MCP port validation** â€” TCP port conflict detection, range validation (1024+), Auto port button, and auto-restart on settings change ([#453])
- **Alert database exclusion filters** â€” filter blocking and deadlock alerts by database in both Dashboard and Lite ([#410], [#412])
- **Configurable alert cooldown periods** for tray notifications and email alerts
- **Wait stats query drill-down** â€” click a wait type to see the queries causing it ([#372])
- **Configurable long-running query settings** â€” max results, WAITFOR/backup/diagnostics exclusions ([#415])
- **Uninstall option** in both CLI and GUI installers ([#431])
- **Session stats collector** for active session tracking ([#474])
- **LOB compression and deduplication** for query stats tables to reduce storage ([#419])
- **Volume-level drive space** enrichment in database size stats via `dm_os_volume_stats`
- **GUI installer installation history** logging to `config.installation_history` ([#414])
- **ReadOnlyIntent connection option** â€” Lite connections can set `ApplicationIntent=ReadOnly` for automatic read routing to Always On AG readable secondaries ([#515])
- **Alert muting** â€” mute individual alerts or create pattern-based mute rules by server, metric, database, or application. Manage Mute Rules window with enable/disable toggle. Alert history detail view with double-click drill-down and context-sensitive detail text. Poison wait type documentation links. ([#512])
- **SignPath code signing** â€” all release binaries (Dashboard, Lite, Installers) are digitally signed, eliminating Windows SmartScreen warnings ([#511])
- CI version bump check on PRs to main
- Permissions section in README with least-privilege setup ([#421])

### Changed

- **Utilization tab redesigned** â€” ported to Dashboard with aligned metrics between apps ([#478])
- PlanAnalyzer rules synced from PerformanceStudio â€” Rule 5 message format, seek predicate parsing, spool labels, unmatched index detail ([#416], [#475], [#480])
- Data retention now purges processed XE staging rows
- GeneratedRegex conversion for compile-time regex patterns ([#346], [#420])
- Server health card width increased from 260 to 300 for less text truncation ([#489])
- User's locale used for date/time formatting in WPF bindings ([#459])
- XML processing instructions stripped from sql_command/sql_text display
- Parameterized queries in blocking/deadlock alert filtering
- **DuckDB 1.5.0 upgrade** â€” non-blocking checkpointing eliminates read stalls during WAL flushes, free block reuse stabilizes database file size without archive-and-reset cycles ([#516])
- **Automatic parquet compaction** â€” archive files are merged into monthly files after each archive cycle, reducing file count from 2,600+ to ~75 and eliminating per-file metadata overhead on glob scans ([#516])

  Combined with the UI responsiveness overhaul (#510), Lite's refresh cycle improved 13-26x:

  | Metric | Before | After |
  |---|---|---|
  | Lite `RefreshAllDataAsync` | 6-13s | < 500ms |
  | Parquet files scanned per query | 233 | 19 |
  | Archive-and-reset frequency | 21/day | ~0 |
  | `v_wait_stats` query time | 1,700ms | 27ms |

- **Monthly archive retention** â€” switched from 90-day file-age deletion to 3-month calendar-month rolling window, aligned with compacted monthly filenames ([#516])
- **Lite status bar** shows used data size vs file size (e.g., "Database: 175.5 / 423.8 MB") via DuckDB `pragma_database_size()` ([#517])
- **Query Store collector diagnostics** â€” reader/append/flush timing breakdown logged when collection exceeds 2 seconds, for identifying SQL Server DMV contention under heavy workloads ([#518])
- SSMS-parity edge tooltips on plan viewer operator connections and ManyToMany indicator always shown for merge join operators ([#504])
- **Lite UI responsiveness overhaul** â€” visible-tab-only refresh, sub-tab awareness, Query Store collector optimization (NULL plan XML + LOOP JOIN hint), and DuckDB write reduction ([#510])

  Timer tick improvements measured under TPC-C load on SQL2022:

  | Scenario | Before | After | Improvement |
  |---|---|---|---|
  | Lite idle | 6-13s | 546-750ms | ~90% |
  | Lite under TPC-C | 6-13s | ~3s | ~70% |
  | Dashboard idle | 5.6s | 0.6-0.8s | 86% |
  | Dashboard under TPC-C | 5.6s | 1.8-2.0s | 64% |

  Query Store collector specifically:

  | Metric | Before | After |
  |---|---|---|
  | query_store collector total | 6-18s | ~600ms |
  | query_store SQL time | 374-1,104ms | ~300ms (LOOP JOIN hint) |
  | query_store DuckDB write | 6-16s | ~75-230ms (NULL plan XML) |

### Fixed

- **UI hang** when opening Dashboard tab for offline server â€” replaced synchronous `.GetAwaiter().GetResult()` with proper `await` ([#477])
- **First-collection spike** skewing PerfMon, wait stats, file I/O, memory grant, query stats, and procedure stats charts â€” first cumulative value now treated as baseline ([#482])
- **Wait type filter TextBox** too small to read ([#488])
- **Poison wait false positives** and alert log parsing ([#445], [#448])
- **RID Lookup** analyzer rule matching new PhysicalOp label ([#429])
- **procedure_stats** plan query using DECOMPRESS after compression migration
- **database_size_stats** InvalidCastException on compatibility_level
- **Deadlock filter** using wrong column reference in `GetFilteredDeadlockCountAsync`
- **RESTORING database** filter added to waiting_tasks collector ([#430])
- Custom TrayToolTip crash â€” replaced with plain ToolTipText ([#422])
- **Lite tab switch freeze** â€” added `_isRefreshing` guard to prevent tab switch handler from competing with timer ticks for DuckDB connection, eliminating "not responding" hangs ([#510])
- DuckDB read lock acquisition resilience
- Formatted duration columns sorting alphabetically instead of numerically
- Settings window staying open on validation errors
- Deserialization clamping and validation abort issues
- **sp_IndexCleanup** summary grid column mapping off-by-one, expanded both grids to show all columns from both result sets ([#503])
- **Rule 22 table variable** false positive on modification operators â€” INSERT/UPDATE/DELETE on table variables is expected ([#513])
- **ComboBox focus steal** in plan viewer stealing keyboard focus from other controls ([#508])
- **DOP 2 skew** false positive â€” parallel skew rule no longer fires at DOP 2 ([#508])
- **ReadOnlyIntent connections** sharing server_id in DuckDB when the same server was added with and without ReadOnlyIntent ([#521])

[2.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.1.0...v2.2.0

## [2.1.0] - 2026-03-04

### Important

- **Schema upgrade**: The `config.collection_schedule` table gains two new columns (`collect_query`, `collect_plan`) for optional query text and execution plan collection. Both default to enabled to preserve existing behavior. Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Light theme and "Cool Breeze" theme** â€” full light mode support for both Dashboard and Lite with live preview in settings ([#347])
- **Standalone Plan Viewer** â€” open, paste (Ctrl+V), or drag & drop `.sqlplan` files independent of any server connection, with tabbed multi-plan support ([#359])
- **Time display mode toggle** â€” show timestamps in Server Time, Local Time, or UTC with timezone labels across all grids and tooltips ([#17])
- **30 PlanAnalyzer rules** â€” expanded from 12 to 30 rules covering implicit conversions, GetRangeThroughConvert, lazy spools, OR expansion, exchange spills, RID lookups, and more ([#327], [#349], [#356], [#379])
- **Wait stats banner** in plan viewer showing top waits for the query ([#373])
- **UDF runtime details** â€” CPU and elapsed time shown in Runtime Summary pane when UDFs are present ([#382])
- **Sortable statement grid** and canvas panning in plan viewer ([#331])
- **Comma-separated column filters** â€” enter multiple values separated by commas in text filters ([#348])
- **Optional query text and plan collection** â€” per-collector flags in `config.collection_schedule` to disable query text or plan capture ([#337])
- **`--preserve-jobs` installer flag** â€” keep existing SQL Agent job schedules during upgrade ([#326])
- **Copy Query Text** context menu on Dashboard statements grid ([#367])
- **Server list sorting** by display name in both Dashboard and Lite ([#30])
- **Warning status icon** in server health indicators ([#355])
- Reserved threads and 10 missing ShowPlan XML attributes in plan viewer ([#378])
- Nightly build workflow for CI ([#332])

### Changed

- PlanAnalyzer warning messages rewritten to be actionable with expert-guided per-rule advice ([#370], [#371])
- PlanAnalyzer rule tuning: time-based spill analysis (Rule 7), lowered parallel skew thresholds (Rule 8), memory grant floor raised to 1GB/4GB (Rule 9), skip PROBE-only bitmap predicates (Rule 11) ([#341], [#342], [#343], [#358])
- First-run collector lookback reduced from 3-7 days to 1 hour for faster initial data ([#335])
- Plan canvas aligns top-left and resets scroll on statement switch ([#366])
- Plan viewer polish: index suggestions, property panel improvements, muted brush audit ([#365])
- Add Server dialog visual parity between Dashboard and Lite with theme-driven PasswordBox styling ([#289])

### Fixed

- **OverflowException** on wait stats page with large decimal values â€” SQL Server `decimal(38,24)` exceeding .NET precision ([#395])
- **SQL dumps** on mirroring passive servers with RESTORING databases ([#384])
- **UI hang** when adding first server to Dashboard ([#387])
- **UTC/local timezone mismatch** in blocked process XML processor ([#383])
- **AG secondary filter** skipping all inaccessible databases in cross-database collectors ([#325])
- DuckDB column aliases in long-running queries ([#391])
- sp_server_diagnostics and WAITFOR excluded from long-running query alerts ([#362])
- UDF timing units corrected: microseconds to milliseconds ([#338])
- DuckDB migration ordering after archive-and-reset ([#314])
- Int16 cast error in long-running query alerts ([#313])
- Missing dark mode on 19 SystemEventsContent charts ([#321])
- Missing tooltips on charts after theme changes ([#319])
- Operator time per-thread calculation synced across all plan viewers ([#392])
- Theme StaticResource/DynamicResource binding fix for runtime theme switching
- Memory grant MB display, missing index quality scoring, wildcard LIKE detection ([#393])
- **Installer validation** reporting historical collection errors as current failures â€” now filters to current run only ([#400])
- **query_snapshots schema mismatch** after sp_WhoIsActive upgrade â€” collector auto-recreates daily table when column order changes ([#401])
- **Missing upgrade script** for `default_trace_events` columns (`duration_us`, `end_time`) on 2.0.0â†’2.1.0 upgrade path ([#400])

## [2.0.0] - 2026-02-25

### Important

- **Schema upgrade**: The `collect.memory_grant_stats` table gains new delta columns and drops unused warning columns. The `collect.session_wait_stats` table, its collector procedure, reporting view, and schedule entry are removed (zero UI coverage). Upgrade scripts run automatically via the CLI/GUI installer and use idempotent checks.

### Added

- **Graphical query plan viewer** â€” native ShowPlan rendering in both Dashboard and Lite with SSMS-parity operator icons, properties panel, tooltips, warning/parallelism badges, and tabbed plan display ([#220])
- **Actual execution plan support** â€” execute queries with SET STATISTICS XML ON to capture actual plans, with loading indicator and confirmation dialog ([#233])
- **PlanAnalyzer** â€” automated plan analysis with rules for missing indexes, eager spools, key lookups, implicit conversions, memory grants, and more
- **Current Active Queries live snapshot** â€” real-time view of running queries with estimated/live plan download ([#149])
- **Memory clerks tab** in Lite with picker-driven chart ([#145])
- **Current Waits charts** in Blocking tab for both Dashboard and Lite ([#280])
- **File I/O throughput charts** â€” read/write throughput trends, file-level latency breakdown, queued I/O overlay ([#281])
- **Memory grant stats charts** â€” standardized collection with delta framework integration and trend visualization ([#281])
- **CPU scheduler pressure status** â€” real-time scheduler, worker, runnable task counts with color-coded pressure level below CPU chart
- **Collection log drill-down** and daily summary in Lite ([#138])
- **Collector duration trends chart** in Dashboard Collection Health ([#138])
- **Themed perfmon counter packs** â€” 14 new counters with organized themed groups ([#255])
- **User-configurable connection timeout** setting ([#236])
- **Per-collector retention** â€” uses per-collector retention from `config.collection_schedule` in data retention ([#237])
- **Query identifiers** in drill-down windows â€” query hash, plan hash, SQL handle visible for identification ([#268])
- **Trace pattern drill-down** with missing columns and query text tooltips ([#273])
- **Query Store Regressions drill-down** with TVF rewrite for performance ([#274])
- **CLI `--help` flag** for installer ([#111])
- Sort arrows, right-aligned numerics, and initial sort indicators across all grids ([#110])
- Copyable plan viewer properties ([#269])
- Standardized chart save/export filenames between Dashboard and Lite ([#284])
- Full Dashboard column parity for query_stats, procedure_stats, and query_store_stats
- Min/max extremes surfaced in both apps â€” physical reads, rows, grant KB, spills, CLR time, log bytes ([#281])

### Changed

- Query Store detection uses `sys.database_query_store_options` instead of `sys.databases.is_query_store_on` for Azure SQL DB compatibility ([#287])
- Config tab consolidation, DB drop on server remove, DuckDB-first plan lookups, procedure stats parity
- Collector health status now detects consecutive recent failures â€” 5+ consecutive errors = FAILING, 3+ = WARNING
- Plan buttons now show a MessageBox when no plan is available instead of silently doing nothing
- CSV export uses locale-appropriate separators for non-US locales ([#240])
- Query Store Regressions and Query Trace Patterns migrated to popup grid filtering ([#260])
- NuGet packages updated; xUnit v3 migration

### Fixed

- **DuckDB file corruption** during maintenance â€” ReaderWriterLockSlim coordination, archive-all-and-reset at 512MB replaces compaction ([#218])
- Archive view column mismatch, wait_stats thread-safety, and percent_complete type cast ([#234])
- Collector health status bar text color ([#234])
- View Plan for Query Store and Query Store Regressions tabs ([#261])
- Query Store drill-down time filter alignment with main view ([#263])
- Execution count mismatches between main views and drill-downs
- Drill-down chart UX â€” sparse data markers, hover tooltips, window sizing ([#271])
- Truncated status text in Add Server dialog ([#257])
- Scrollbar visibility, self-filtering artifacts, missing columns, and context menus ([#245], [#246], [#247], [#248])
- query_stats and procedure_stats collectors ignoring recent queries
- Blank tooltips on warning and parallel badge icons
- Missing chart context menu on File I/O Throughput charts in Lite

### Removed

- `collect.session_wait_stats` table, `collect.session_wait_stats_collector` procedure, `report.session_wait_analysis` view, and schedule entry â€” zero UI coverage, never surfaced in Dashboard or Lite ([#281])

## [1.3.0] - 2026-02-20

### Important

- **Schema upgrade**: The `collect.memory_stats` table gains two new columns (`total_physical_memory_mb`, `committed_target_memory_mb`). The upgrade script runs automatically via the CLI/GUI installer and uses `IF NOT EXISTS` checks, so it is safe to re-run. On servers with very large `memory_stats` tables this ALTER may take a moment.

### Added

- Physical Memory, SQL Server Memory, and Target Memory columns in Memory Overview ([#140])
- Current Configuration view (Server Config, Database Config, Trace Flags) in Dashboard Overview ([#143])
- Popup column filters and right-click context menus in all drill-down history windows ([#206])
- Consistent popup column filters across all Dashboard grids â€” replaced remaining TextBox-in-header filters and added filters to Trace Flags ([#200])
- 7-day time filter option in drill-down queries ([#165])
- Alert badge/count on sidebar Alerts button ([#109])
- Missing poison wait defaults in wait stats picker ([#188])

### Changed

- Default Trace tabs moved from Resource Metrics to Overview section ([#169])
- Trends tab shown first in Locking section ([#171])
- Wait stats cap raised from 20 to 30 (Dashboard) / 50 (Lite) so poison waits are never dropped ([#139])
- Settings time range dropdown now matches dashboard button options ([#210])
- "Total Executions" label in drill-down summaries renamed to clarify meaning ([#194])
- WAITFOR sessions excluded from long-running query alerts ([#151])

### Fixed

- Deadlock XML processor timezone mismatch â€” sp_BlitzLock returning 0 results because UTC dates were passed instead of local time
- Sidebar alert badge not updating when alerts dismissed from server sub-tabs ([#214])
- Sidebar alert badge not clearing on acknowledge ([#186])
- NOC deadlock/blocking showing "just now" for stale events instead of actual timestamp ([#187])
- NOC deadlock severity using extended events timestamp ([#170])
- Newly added servers not appearing on Overview until app restart ([#199])
- Double-click on column header incorrectly triggering row drill-down ([#195])
- Squished drill-down charts â€” now use proportional sizing ([#166])
- Unreliable chart tooltips â€” now use X-axis proximity matching ([#167])
- Query Trace Patterns showing empty despite data existing ([#168])
- Drill-down windows: removed inline plan XML, added time range filtering, aggregated by collection_time ([#189])
- Row clipping in Default Trace and Current Configuration grids ([#183], [#184])
- Numeric filter negative range parsing ([#113])
- MCP shutdown deadlock risk ([#112])
- Lite DBNull cast error in database_config collector on SQL 2016 Express ([#192])
- DuckDB concurrent file access IO errors ([#164])

## [1.2.0] - 2026-02-15

### Added

- Alert types, alerts history view, column filtering, and dismiss/hide for alerts ([#52], [#56])
- Average ms per wait chart toggle in both apps ([#22])
- Collection Health tab in Lite UI ([#39])
- Collector performance diagnostics in Lite UI ([#40])
- Hover tooltips on all Dashboard charts ([#70])
- Minimize-to-tray setting added to Lite ([#53])
- Persist dismissed alerts across app restarts ([#44])
- Locale-aware date/time formatting throughout UI ([#41])
- 24-hour format in time range picker ([#41])
- CI pipelines for build validation, SQL install testing, and DuckDB schema tests
- Expanded Lite database config collector to 28 sys.databases columns ([#142])
- Parquet archive visibility and scheduled DuckDB database compaction ([#160], [#161])
- DuckDB checkpoint optimization and collection timing accuracy
- Installer `--reset-schedule` flag to reset collection schedule on re-install

### Fixed

- Deadlock charts not populating data ([#73])
- Chart X-axis double-converting custom range to server time ([#49])
- query_cost overflow in memory grant collector ([#47])
- XE ring buffer query timeouts on large buffers ([#37])
- Dashboard sub-tab badge state and DuckDB migration for dismissed column
- Lite duplicate blocking/deadlock events from missing WHERE clause ([#61])
- Procedure_stats_collector truncation on DDL triggers ([#69])
- DataGrid row height increased from 25 to 28 to fix text clipping
- Skip offline servers during Lite collection and reduce connection timeout ([#90])
- Mutex crash on Lite app exit ([#89])
- Permission denied errors handled gracefully in collector health ([#150])

## [1.1.0] - 2026-02-13

### Added

- Hover tooltips on all multi-series charts â€” Wait Stats, Sessions, Latch Stats, Spinlock Stats, File I/O, Perfmon, TempDB ([#21])
- Microsoft Entra MFA authentication for Azure SQL DB connections in Lite ([#20])
- Column-level filtering on all 11 Lite DataGrids ([#18])
- Chart visual parity â€” Material Design 300 color palette, data point markers, consistent grid styling ([#16])
- Smart Select All for wait types + expand from 12 to 20 wait types ([#12])
- Trend chart legends always visible in Dashboard ([#11])
- Per-server collector health in Lite status bar ([#5])
- Server Online/Offline status in Lite overview ([#2])
- Check for updates feature in both apps ([#1])
- High DPI support for both Dashboard and Lite

### Fixed

- Query text off-by-one truncation ([#25])
- Blocking/deadlock XML processors truncating parsed data every run ([#23])
- WAITFOR queries appearing in top queries views ([#4])
- Wait type Clear All not refreshing search filter in Dashboard

## [1.0.0] - 2026-02-11

### Added

- Full Edition: Dashboard + CLI/GUI Installer with 30+ automated SQL Agent collectors
- Lite Edition: Agentless monitoring with local DuckDB storage
- Support for SQL Server 2016-2025, Azure SQL DB, Azure SQL MI, AWS RDS
- Real-time charts and trend analysis for wait stats, CPU, memory, query performance, index usage, file I/O, blocking, deadlocks
- Email alerts for blocking, deadlocks, and high CPU
- MCP server integration for AI-assisted analysis
- System tray operation with background collection and alert notifications
- Data retention with configurable automatic cleanup
- Delta normalization for per-second rate calculations
- Dark theme UI

[2.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.3.0...v2.0.0
[1.3.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/erikdarlingdata/PerformanceMonitor/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/erikdarlingdata/PerformanceMonitor/releases/tag/v1.0.0
[#1]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1
[#2]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2
[#4]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/4
[#5]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/5
[#11]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/11
[#12]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/12
[#16]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/16
[#18]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/18
[#20]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/20
[#21]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/21
[#22]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/22
[#23]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/23
[#25]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/25
[#37]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/37
[#39]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/39
[#40]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/40
[#41]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/41
[#44]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/44
[#47]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/47
[#49]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/49
[#52]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/52
[#53]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/53
[#56]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/56
[#61]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/61
[#69]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/69
[#70]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/70
[#73]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/73
[#85]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/85
[#86]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/86
[#89]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/89
[#90]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/90
[#109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/109
[#112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/112
[#113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/113
[#139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/139
[#140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/140
[#142]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/142
[#143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/143
[#150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/150
[#151]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/151
[#160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/160
[#161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/161
[#164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/164
[#165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/165
[#166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/166
[#167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/167
[#168]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/168
[#169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/169
[#170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/170
[#171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/171
[#1724]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1724
[#1735]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1735
[#1732]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1732
[#183]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/183
[#184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/184
[#186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/186
[#187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/187
[#188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/188
[#189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/189
[#192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/192
[#194]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/194
[#195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/195
[#199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/199
[#200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/200
[#206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/206
[#210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/210
[#214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/214
[#218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/218
[#220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/220
[#233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/233
[#234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/234
[#236]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/236
[#237]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/237
[#240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/240
[#245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/245
[#246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/246
[#247]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/247
[#248]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/248
[#255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/255
[#257]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/257
[#260]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/260
[#261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/261
[#263]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/263
[#268]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/268
[#269]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/269
[#271]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/271
[#273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/273
[#274]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/274
[#280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/280
[#281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/281
[#284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/284
[#287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/287
[#313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/313
[#314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/314
[#17]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/17
[#30]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/30
[#319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/319
[#321]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/321
[#325]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/325
[#326]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/326
[#327]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/327
[#331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/331
[#332]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/332
[#335]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/335
[#337]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/337
[#338]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/338
[#341]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/341
[#342]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/342
[#343]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/343
[#347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/347
[#348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/348
[#349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/349
[#355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/355
[#356]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/356
[#358]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/358
[#359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/359
[#362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/362
[#365]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/365
[#366]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/366
[#367]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/367
[#370]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/370
[#371]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/371
[#373]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/373
[#378]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/378
[#379]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/379
[#382]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/382
[#383]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/383
[#384]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/384
[#387]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/387
[#391]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/391
[#392]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/392
[#393]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/393
[#289]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/289
[#395]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/395
[#400]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/400
[#401]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/401
[#410]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/410
[#412]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/412
[#414]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/414
[#415]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/415
[#416]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/416
[#419]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/419
[#420]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/420
[#421]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/421
[#422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/422
[#429]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/429
[#430]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/430
[#431]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/431
[#445]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/445
[#448]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/448
[#453]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/453
[#454]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/454
[#459]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/459
[#474]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/474
[#475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/475
[#477]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/477
[#478]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/478
[#480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/480
[#481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/481
[#482]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/482
[#488]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/488
[#489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/489
[#503]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/503
[#504]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/504
[#508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/508
[#510]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/510
[#512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/512
[#511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/511
[#513]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/513
[#515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/515
[#516]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/516
[#517]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/517
[#518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/518
[#521]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/521
[#1643]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1643
[#1649]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1649
[#1650]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1650
[#1591]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1591
[#1661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1661
[#1667]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1667
[#1666]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1666
[#1682]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1682
[#1681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1681
[#1680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1680
[#1688]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1688
[#1691]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1691
[#1692]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1692
[#1695]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1695
[#1699]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1699
[#1704]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1704
[#1707]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1707
[#1713]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1713
[#1711]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1711
[#1719]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1719
[#1722]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1722
[#1731]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1731
[#1709]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1709
[#1702]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1702
[#1697]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1697
[#1700]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1700
[#1703]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1703
[#1708]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1708
[#1712]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1712
[#1715]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1715
[#1716]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1716
[#1710]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1710
[#1726]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1726
[#1734]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1734
[#1750]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1750
[#1725]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1725
[#1730]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1730
[#1739]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1739
[#1744]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1744
[#1737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1737
[#1738]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1738
[#1752]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1752
[#1751]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1751
[#1749]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1749
[#1753]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1753
[#1756]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1756
[#1748]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1748
[#1747]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1747
[#1727]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1727
[#1690]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1690
[#1693]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1693
[#1694]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1694
[#1698]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1698
[#1701]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1701
[#1705]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1705
[#1718]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1718
[#1729]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1729
[#1762]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1762
[#1757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1757
[#1759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1759
[#1760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1760
[#1769]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1769
[#1776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1776
[#1767]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1767
[#1768]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1768
[#1770]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1770
[#1771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1771
[#1772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1772
[#1773]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1773
[#1774]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1774
[#1775]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1775
[#1785]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1785
[#1777]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1777
[#1780]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1780
[#1665]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1665
[#1799]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1799
[#1798]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1798
[#1821]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1821
[#1797]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1797
[#1788]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1788
[#1781]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1781
[#1782]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1782
[#1783]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1783
[#1784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1784
[#1786]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1786
[#1789]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1789
[#1792]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1792
[#1793]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1793
[#1796]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1796
[#1778]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1778
[#1779]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1779
[#1801]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1801
[#1803]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1803
[#1805]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1805
[#1808]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1808
[#1816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1816
[#1818]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1818
[#1815]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1815
[#1817]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1817
[#1812]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1812
[#1814]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1814
[#1795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1795
[#1813]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1813
[#1809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1809
[#1811]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1811
[#1794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1794
[#1810]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1810
[#1787]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1787
[#1806]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1806
[#1758]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1758
[#1807]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1807
[#1802]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1802
[#1823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823
[#1828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1828
[#1831]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1831
[#1833]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1833
[#1836]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1836
[#1837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1837
[#1830]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1830
[#1839]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1839
[#1832]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1832
[#1841]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1841
[#1849]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1849
[#1850]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1850
[#1851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1851
[#1852]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1852
[#1855]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1855
[#1847]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1847
[#1856]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1856
[#1857]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1857
[#1858]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1858
[#1859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1859
[#1862]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1862
[#1863]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1863
[#1867]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1867
[#1869]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1869
[#1846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1846
[#1881]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1881
[#1882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1882

[#1860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1860
[#1878]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1878

[#1844]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1844
[#1848]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1848
[#1871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1871
[#1872]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1872

[#1877]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1877
[#1879]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1879
[#1854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1854
[#1865]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1865
[#1875]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1875
[#1876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1876
[#1874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1874
[#1888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1888
[#1899]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1899
[#1889]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1889
[#1893]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1893
[#1845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1845
[#1853]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1853
[#1873]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1873
[#1896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1896
[#1898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1898
[#1913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1913
[#1914]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1914
[#1902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1902
[#1907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1907
[#1912]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1912
[#1905]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1905
[#1906]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1906
[#1824]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1824
[#1826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1826
[#1834]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1834
[#1840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1840
[#1842]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1842
[#1891]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1891
[#1892]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1892
[#1915]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1915
[#1925]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1925
[#1895]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1895
[#1911]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1911
[#1829]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1829
[#1922]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1922
[#1886]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1886
[#1934]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1934
[#1943]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1943
[#1937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1937
[#1939]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1939
[#1945]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1945
[#1959]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1959
[#1953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1953
[#1942]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1942
[#1957]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1957
[#1958]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1958
[#1962]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1962
[#1963]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1963
[#1965]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1965
[#110]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/110
[#111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/111
[#138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/138
[#145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/145
[#149]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/149
[#346]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/346
[#372]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/372
[#1663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1663
[#1673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1673
[#1685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1685
[#1721]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1721
[#1754]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1754
[#1755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1755
[#1940]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1940
[#1921]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1921
[#1923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1923
[#1954]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1954
[#1970]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1970
[#1972]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1972
[#1973]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1973
[#1955]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1955
[#1980]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1980
[#1949]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1949
[#1952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1952
[#1951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1951
[#1960]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1960
[#1822]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1822
[#1990]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1990
[#1988]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1988
[#1984]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1984
[#1743]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1743
[#1606]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1606
[#1996]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1996
[#2000]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2000
[#1074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1074
[#1966]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1966
[#1995]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/1995
[#2007]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2007
[#2008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2008
[#2020]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2020
[#2027]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2027
[#2028]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2028
[#2029]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2029
[#2005]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2005
[#1804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1804
[#2030]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2030
[#2031]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2031
[#2033]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2033
[#2038]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2038
[#2011]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2011
[#1986]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1986
[#2012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2012
[#1568]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1568
[#1981]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1981
[#2022]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2022
[#2054]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2054
[#2058]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2058
[#2060]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2060
[#2063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2063
[#2064]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2064
[#2068]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2068
[#2069]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2069
[#2071]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2071
[#2073]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2073
[#2076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2076
[#2087]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2087
[#2090]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2090
[#2093]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2093
[#2097]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2097
[#2101]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2101
[#2105]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2105
[#2107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2107
[#2102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2102
[#2108]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2108
[#2109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2109
[#2111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2111
[#2113]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2113
[#2114]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2114
[#2117]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2117
[#2119]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2119
[#2126]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2126
[#2129]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2129
[#2133]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2133
[#2136]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2136
[#2143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2143
[#2148]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2148
[#2154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2154
[#2157]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2157
[#2164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2164
[#2210]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2210
[#2188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2188
[#2191]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2191
[#2171]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2171
[#2170]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2170
[#2169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2169
[#2166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2166
[#2167]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2167
[#2213]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2213
[#2216]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2216
[#2138]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2138
[#2190]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2190
[#2186]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2186
[#2185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2185
[#2258]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2258
[#2219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2219
[#2280]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2280
[#2277]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2277
[#2279]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2279
[#2273]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2273
[#2220]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2220
[#2228]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2228
[#2218]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2218
[#2235]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2235
[#2165]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2165
[#2255]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2255
[#2159]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2159
[#2197]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2197
[#2203]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2203
[#2187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2187
[#2189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2189
[#2184]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2184
[#2254]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2254
[#2252]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2252
[#2256]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2256
[#2158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2158
[#2246]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2246
[#2319]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2319
[#2340]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2340
[#2344]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2344
[#2349]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2349
[#2357]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2357
[#2361]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2361
[#2362]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2362
[#2364]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2364
[#2347]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2347
[#2359]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2359
[#2348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2348
[#2350]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2350
[#2353]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2353
[#2352]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2352
[#2331]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2331
[#2181]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2181
[#2317]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2317
[#2320]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2320
[#2339]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2339
[#2316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2316
[#2324]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2324
[#2300]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2300
[#2312]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2312
[#2306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2306
[#2302]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2302
[#2501]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2501
[#2499]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2499
[#2495]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2495
[#2506]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2506
[#2511]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2511
[#2512]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2512
[#2515]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2515
[#2520]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2520
[#2525]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2525
[#2530]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530
[#2539]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2539
[#2540]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2540
[#2508]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2508
[#2541]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2541
[#2542]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2542
[#2532]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2532
[#2518]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2518
[#2533]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2533
[#2535]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2535
[#2480]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2480
[#2489]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2489
[#2481]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2481
[#2437]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2437
[#1563]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1563
[#2475]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2475
[#2422]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2422
[#2460]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2460
[#2459]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2459
[#2446]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2446
[#2296]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2296
[#2299]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2299
[#2294]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2294
[#2298]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2298
[#2293]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2293
[#2233]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2233
[#2234]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2234
[#2150]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2150
[#2484]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2484
[#2485]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2485
[#2524]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2524
[#2201]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2201
[#2205]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2205
[#2195]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2195
[#2548]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2548
[#2554]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2554
[#2546]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2546
[#2552]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2552
[#2529]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2529
[#2569]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2569
[#2572]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2572
[#2574]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2574
[#2579]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2579
[#2559]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2559
[#2562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562
[#2550]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2550
[#2578]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2578
[#2564]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2564
[#2653]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2653
[#2655]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2655
[#2658]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2658
[#2659]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2659
[#2661]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2661
[#2663]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2663
[#2665]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2665
[#2671]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2671
[#2674]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2674
[#2675]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2675
[#2677]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2677
[#2680]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2680
[#2681]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2681
[#2683]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2683
[#2685]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2685
[#2690]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2690
[#2673]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2673
[#2687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2687
[#2688]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2688
[#2691]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2691
[#2695]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2695
[#2692]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2692
[#2693]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2693
[#2694]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2694
[#2699]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2699
[#2715]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2715
[#2710]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2710
[#2711]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2711
[#2719]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2719
[#2733]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2733
[#2735]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2735
[#2736]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2736
[#2732]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2732
[#2737]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2737
[#2734]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2734
[#2759]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2759
[#2749]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2749
[#2764]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2764
[#2766]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2766
[#2785]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2785
[#2791]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2791
[#2795]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2795
[#2794]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2794
[#2473]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2473
[#2792]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2792
[#1562]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1562
[#2800]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2800
[#2801]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2801
[#2803]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2803
[#2804]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2804
[#2779]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2779
[#2784]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2784
[#2786]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2786
[#2284]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2284
[#2776]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2776
[#2772]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2772
[#2773]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2773
[#2748]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2748
[#2771]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2771
[#2761]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2761
[#2465]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2465
[#2770]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2770
[#2768]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2768
[#2780]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2780
[#2781]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2781
[#2757]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2757
[#2755]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2755
[#2753]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2753
[#1687]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1687
[#2811]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2811
[#2816]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2816
[#2796]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2796
[#2809]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2809
[#2810]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2810
[#2813]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2813
[#2819]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2819
[#2822]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2822
[#2823]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2823
[#2826]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2826
[#2827]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2827
[#2839]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2839
[#2840]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2840
[#2846]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2846
[#2841]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2841
[#2845]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2845
[#2851]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2851
[#2854]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2854
[#2859]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2859
[#2855]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2855
[#2860]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2860
[#2871]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2871
[#2828]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2828
[#2837]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2837
[#2870]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2870
[#2884]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2884
[#2864]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2864
[#2874]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2874
[#2876]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2876
[#2797]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2797
[#2490]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2490
[#2898]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2898
[#2890]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2890
[#2936]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2936
[#2935]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2935
[#2882]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2882
[#2888]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2888
[#2917]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2917
[#2918]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2918
[#2907]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2907
[#2924]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2924
[#2910]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2910
[#2896]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2896
[#2901]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2901
[#2902]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2902
[#2894]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2894
[#1696]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1696
[#1944]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/1944
[#2018]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2018
[#2026]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2026
[#2061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2061
[#2266]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2266
[#2538]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2538
[#2543]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2543
[#2544]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2544
[#2545]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2545
[#2547]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2547
[#2561]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2561
[#2565]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2565
[#2566]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2566
[#2567]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2567
[#2593]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2593
[#2595]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2595
[#2599]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2599
[#2603]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2603
[#2612]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2612
[#2617]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2617
[#2623]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2623
[#2625]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2625
[#2626]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2626
[#2629]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2629
[#2630]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2630
[#2633]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2633
[#2636]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2636
[#2638]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2638
[#2640]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2640
[#2641]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2641
[#2643]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2643
[#2645]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2645
[#2651]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2651
[#2686]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2686
[#2689]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2689
[#2760]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2760
[#2847]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2847
[#2913]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2913
[#2928]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2928
[#2932]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2932
[#2923]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2923
[#2920]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2920
[#2926]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2926
[#2951]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2951
[#2952]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2952
[#2931]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2931
[#2927]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2927
[#2929]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2929
[#2933]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2933
[#2937]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2937
[#2938]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2938
[#2940]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2940
[#2942]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2942
[#2953]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2953
[#2960]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2960
[#2964]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2964
[#2966]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2966
[#2967]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2967
[#2972]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2972
[#2973]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/2973
[#2880]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2880
[#2997]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/2997
[#3008]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3008
[#3009]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3009
[#3010]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3010
[#3012]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3012
[#3013]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3013
[#3014]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3014
[#3016]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3016
[#3017]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3017
[#3018]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3018
[#3019]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3019
[#3021]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3021
[#3022]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3022
[#3025]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3025
[#3026]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3026
[#3027]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3027
[#3030]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3030
[#3031]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3031
[#3035]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3035
[#3036]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3036
[#3038]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3038
[#3042]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3042
[#3044]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3044
[#3045]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3045
[#3047]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3047
[#3048]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3048
[#3049]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3049
[#3050]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3050
[#3052]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3052
[#3053]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3053
[#3060]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3060
[#3061]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3061
[#3062]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3062
[#3063]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3063
[#3065]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3065
[#3067]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3067
[#3069]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3069
[#3070]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3070
[#3072]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3072
[#3074]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3074
[#3076]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3076
[#3081]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3081
[#3082]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3082
[#3086]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3086
[#3087]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3087
[#3088]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3088
[#3090]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3090
[#3094]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3094
[#3095]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3095
[#3098]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3098
[#3099]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3099
[#3101]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3101
[#3102]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3102
[#3104]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3104
[#3107]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3107
[#3109]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3109
[#3111]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3111
[#3116]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3116
[#3118]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3118
[#3119]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3119
[#3121]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3121
[#3129]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3129
[#3132]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3132
[#3133]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3133
[#3134]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3134
[#3138]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3138
[#3139]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3139
[#3140]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3140
[#3143]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3143
[#3144]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3144
[#3145]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3145
[#3112]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3112
[#3153]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3153
[#3154]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3154
[#3156]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3156
[#3158]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3158
[#3160]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3160
[#3161]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3161
[#3164]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3164
[#3165]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3165
[#3166]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3166
[#3168]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3168
[#3169]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3169
[#3174]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3174
[#3175]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3175
[#3181]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3181
[#3182]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3182
[#3184]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3184
[#3185]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3185
[#3187]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3187
[#3188]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3188
[#3189]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3189
[#3192]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3192
[#3193]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3193
[#3196]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3196
[#3198]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3198
[#3199]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3199
[#3200]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3200
[#3204]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3204
[#3206]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3206
[#3207]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3207
[#3208]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3208
[#3211]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3211
[#3214]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3214
[#3217]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3217
[#3219]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3219
[#3221]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3221
[#3222]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3222
[#3223]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3223
[#3224]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3224
[#3226]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3226
[#3230]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3230
[#3231]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3231
[#3237]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3237
[#3238]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3238
[#3239]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3239
[#3240]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3240
[#3241]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3241
[#3243]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3243
[#3244]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3244
[#3245]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3245
[#3247]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3247
[#3248]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3248
[#3253]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3253
[#3261]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3261
[#3262]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3262
[#3267]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3267
[#3271]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3271
[#3288]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3288
[#3281]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3281
[#3278]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3278
[#3285]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3285
[#3307]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3307
[#3302]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3302
[#3316]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3316
[#3315]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3315
[#3313]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3313
[#3282]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3282
[#3314]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3314
[#3330]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3330
[#3297]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3297
[#3348]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3348
[#3343]: https://github.com/erikdarlingdata/PerformanceMonitor/pull/3343
[#3306]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3306
[#3355]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3355
[#3354]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3354
[#3287]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3287
[#3368]: https://github.com/erikdarlingdata/PerformanceMonitor/issues/3368
