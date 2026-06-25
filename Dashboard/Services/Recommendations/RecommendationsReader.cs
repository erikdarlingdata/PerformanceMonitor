/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Analysis;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services.Recommendations
{
    /// <summary>
    /// The data layer for the unified Recommendations surface (WS1a). Reads BOTH producers
    /// for a server — engine findings from <c>config.analysis_findings</c> (via the existing
    /// <see cref="SqlServerFindingStore.GetRecentFindingsAsync"/>) and legacy config recs from
    /// <c>config.critical_issues</c> (via the existing
    /// <see cref="DatabaseService.GetCriticalIssuesAsync"/>) — maps each row to a unified
    /// <see cref="RecommendationItem"/>, then de-dupes + sorts via the pure
    /// <see cref="RecommendationDeduper"/>. No new raw SQL is written for the reads; this class
    /// reuses the two existing read methods so it inherits their parameterization.
    ///
    /// <para>
    /// Nothing renders here — this is the foundational data layer (the XAML control + tab
    /// wiring are a later workstream). The per-row mappers are <c>internal static</c> so the
    /// engine/legacy → setting + copy-paste mapping can be unit-tested directly.
    /// </para>
    /// </summary>
    public sealed class RecommendationsReader
    {
        private readonly DatabaseService _databaseService;
        private readonly SqlServerFindingStore _findingStore;

        /// <summary>The two legacy problem-areas that carry per-database config-setting recs.</summary>
        internal const string ProblemAreaDatabaseConfiguration = "Database Configuration";
        internal const string ProblemAreaQueryStoreConfiguration = "Query Store Configuration";

        /// <summary>
        /// Legacy "pressure/growth" problem-areas that are noise — short-window deltas, no
        /// quantification, circular investigate queries — AND inferior duplicates of analysis-
        /// engine facts (RESOURCE_SEMAPHORE / SOS_SCHEDULER_YIELD / THREADPOOL / memory-grant
        /// waits). Suppressed from the Recommendations surface; also cut at the source in
        /// install/50. See the recommendations-engine-rebuild plan, "Legacy-rule curation".
        /// </summary>
        internal static readonly HashSet<string> SuppressedLegacyProblemAreas =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "Memory Pressure",
                "Memory Grant Pressure",
                "CPU Scheduling Pressure",
                // NOT "Memory Clerk Growth" — that legacy rule is the TokenAndPermUserStore
                // bloat check: quantified (GB grown), actionable (DBCC FREESYSTEMCACHE), fires
                // on a real 1GB threshold, and the engine doesn't cover it. It's a keeper.
            };

        public RecommendationsReader(DatabaseService databaseService, SqlServerFindingStore findingStore)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
            _findingStore = findingStore ?? throw new ArgumentNullException(nameof(findingStore));
        }

        /// <summary>
        /// Reads both stores for the server, maps each row, de-dupes (C3), and returns the
        /// unified list sorted by severity desc. <paramref name="serverName"/> is carried for
        /// display only (the reads key on <paramref name="serverId"/> for findings and on the
        /// connection's own server for the legacy store). <paramref name="hoursBack"/> bounds
        /// both reads to the same window. <paramref name="utcOffsetMinutes"/> is the monitored
        /// server's UTC offset; it is applied ONLY to the legacy <c>log_date</c> (server-local)
        /// to normalize it to the UTC window the engine rows already carry, so both producers
        /// expose a single consistent UTC window (the navigation handler converts back).
        /// </summary>
        public async Task<List<RecommendationItem>> GetRecommendationsAsync(
            int serverId, string serverName, int hoursBack = 24, int limit = 100, int utcOffsetMinutes = 0)
        {
            var findings = await _findingStore.GetRecentFindingsAsync(serverId, hoursBack, limit);

            // Show only the LATEST analysis run. GetRecentFindingsAsync returns EVERY batch in the
            // window (ordered analysis_time DESC), so on a server analyzed repeatedly each run would
            // otherwise add another copy of every finding — the (database, setting) de-dupe hides it
            // for config settings, but a Setting=None finding (file growth, an incident) would stack
            // one card per batch. Keep only the most-recent analysis_time. (GetLatestFindingsAsync is
            // the natural home, but it omits remediation_action_json — which Apply needs — so we trim
            // the action-carrying GetRecentFindingsAsync result here.)
            if (findings.Count > 1)
            {
                var latest = findings[0].AnalysisTime; // ordered DESC -> [0] is the most recent batch
                var latestOnly = new List<AnalysisFinding>(findings.Count);
                foreach (var f in findings)
                {
                    if (f.AnalysisTime == latest)
                        latestOnly.Add(f);
                }
                findings = latestOnly;
            }

            var legacy = await _databaseService.GetCriticalIssuesAsync(hoursBack);

            var engineItems = new List<RecommendationItem>(findings.Count);
            foreach (var finding in findings)
                engineItems.AddRange(MapEngineFindings(finding));

            // Correlate-and-focus slice 1 (review §1d): append a "what else fired in this analysis
            // window" cross-reference to each engine card's advice so related findings link to each
            // other instead of the operator hunting across cards. Engine findings only (the legacy
            // store is a separate producer). Render-time, not frozen.
            AppendCoFired(engineItems);

            var legacyItems = new List<RecommendationItem>(legacy.Count);
            foreach (var issue in legacy)
            {
                // Drop the legacy pressure/growth noise (duplicated, better, by engine facts).
                if (SuppressedLegacyProblemAreas.Contains(issue.ProblemArea ?? string.Empty))
                    continue;
                legacyItems.Add(MapLegacyIssue(issue, utcOffsetMinutes));
            }

            return RecommendationDeduper.Merge(engineItems, legacyItems);
        }

        /// <summary>
        /// Appends a "what else fired in this analysis window" cross-reference to each engine card's
        /// advice text (correlate-and-focus slice 1, review §1d). Render-time, not frozen; no-op for a
        /// single card. Mirrors the Lite reader so both apps surface the same cross-reference.
        /// </summary>
        internal static void AppendCoFired(List<RecommendationItem> engineItems)
        {
            if (engineItems.Count <= 1)
                return;

            var windowTitles = new List<(string Title, double Severity)>(engineItems.Count);
            foreach (var it in engineItems)
                windowTitles.Add((it.Title, it.RawSeverity));

            foreach (var it in engineItems)
            {
                var line = CoFiredSummary.Line(CoFiredSummary.OtherTitles(it.Title, windowTitles));
                if (line is null)
                    continue;
                it.AdviceText = string.IsNullOrEmpty(it.AdviceText) ? line : it.AdviceText + " " + line;
            }
        }

        /// <summary>
        /// Maps one engine <see cref="AnalysisFinding"/> to ONE OR MORE
        /// <see cref="RecommendationItem"/>s — the reader's per-finding entry point.
        ///
        /// <para>
        /// A <c>DB_CONFIG</c> finding is SERVER-scoped (its <see cref="AnalysisFinding.DatabaseName"/>
        /// is null) but its persisted action carries per-(database, setting) safe targets across
        /// potentially many databases. The Recommendations surface is per-(database, setting): each
        /// such pair is its own card so it (a) de-dupes with the matching per-db LEGACY row
        /// (<see cref="RecommendationDeduper"/> keys on (database, setting) — and was built so
        /// AUTO_SHRINK/AUTO_CLOSE "engine wins") and (b) carries its OWN single-target Apply that
        /// touches only that one database+setting. So a DB_CONFIG finding with safe targets fans
        /// out into one item per target. Everything else (and a DB_CONFIG finding with no safe
        /// targets) maps 1:1 via <see cref="MapEngineFinding"/>.
        /// </para>
        /// </summary>
        internal static IReadOnlyList<RecommendationItem> MapEngineFindings(AnalysisFinding finding)
        {
            // WS3 server-level config: a CONFIG_* finding whose persisted action carries
            // ServerConfigTargets fans into one card per target. MAXDOP/CTFP cards carry a
            // single-target SERVER_CONFIG Apply; the two advise-only memory cards carry only
            // CopyPasteSql (no Apply). All four are server-scoped (Setting stays None — they don't
            // de-dupe with legacy per-db rows) but are flagged IsServerConfigAdvisory so the
            // view-model treats them as Copy/Apply config fixes, not incidents.
            if (finding.Remediation is { ServerConfigTargets: { Count: > 0 } serverTargets } serverAction &&
                string.Equals(serverAction.FactKey, "SERVER_CONFIG", StringComparison.Ordinal))
            {
                var band = RecommendationDeduper.FromEngineSeverity(finding.Severity);
                var adviceText = ComposeEngineAdvice(FactAdvice.GetComposedForFinding(finding));
                var items = new List<RecommendationItem>(serverTargets.Count);

                foreach (var target in serverTargets)
                {
                    var executable = target.Setting is ServerConfigSetting.Maxdop or ServerConfigSetting.CostThreshold;
                    items.Add(new RecommendationItem
                    {
                        Source = RecommendationSource.Engine,
                        CanonicalSeverity = band,
                        RawSeverity = finding.Severity,
                        Database = null,                       // server-scoped
                        Title = ServerConfigCardTitle(target, finding.ServerName),
                        ProblemArea = finding.Category,
                        AdviceText = adviceText,
                        CopyPasteSql = FactRemediation.BuildSpConfigureStatement(target.Setting, target.RecommendedValue),
                        // MAXDOP/CTFP carry a single-target Apply (FactKey SERVER_CONFIG, routed to the
                        // server-config handler); the memory cards carry no action (advise-only → no Apply button).
                        Remediation = executable
                            ? new RemediationAction("SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
                                ServerConfigTargets: new[] { target })
                            : null,
                        StoryPathHash = finding.StoryPathHash,
                        IncidentId = finding.IncidentId,
                        StoryPath = finding.StoryPath,
                        Setting = RecommendationSetting.None,  // server-level — never de-dupes with legacy
                        IsServerConfigAdvisory = true,
                        WindowStartUtc = AsUtc(finding.TimeRangeStart),
                        WindowEndUtc = AsUtc(finding.TimeRangeEnd)
                    });
                }

                if (items.Count > 0)
                    return items;
            }

            if (string.Equals(finding.RootFactKey, "DB_CONFIG", StringComparison.Ordinal) &&
                finding.Remediation is { } remediation &&
                ((remediation.DbConfigTargets is { Count: > 0 }) || (remediation.RcsiTargets is { Count: > 0 })))
            {
                var band = RecommendationDeduper.FromEngineSeverity(finding.Severity);
                var adviceText = ComposeEngineAdvice(FactAdvice.GetComposedForFinding(finding));
                var safeTargets = remediation.DbConfigTargets ?? Array.Empty<DbConfigTarget>();
                var rcsiTargets = remediation.RcsiTargets ?? Array.Empty<RcsiTarget>();
                var items = new List<RecommendationItem>(safeTargets.Count + rcsiTargets.Count);

                // Safe (always-online) DB-config settings: one per-(db, setting) card with a
                // single-target Apply, so Copy/Apply act on ONLY this (database, setting).
                foreach (var target in safeTargets)
                {
                    var setting = SettingFromDbConfig(target.Setting);
                    if (setting == RecommendationSetting.None)
                        continue; // defensive: safe DB-config settings always map to a key.

                    items.Add(new RecommendationItem
                    {
                        Source = RecommendationSource.Engine,
                        CanonicalSeverity = band,
                        RawSeverity = finding.Severity,
                        Database = target.Database,
                        Title = DbConfigCardTitle(target),
                        ProblemArea = finding.Category,
                        AdviceText = adviceText,
                        CopyPasteSql = AlterStatementFor(target),
                        Remediation = new RemediationAction(
                            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(), new[] { target }),
                        StoryPathHash = finding.StoryPathHash,
                        IncidentId = finding.IncidentId,
                        StoryPath = finding.StoryPath,
                        Setting = setting,
                        WindowStartUtc = AsUtc(finding.TimeRangeStart),
                        WindowEndUtc = AsUtc(finding.TimeRangeEnd)
                    });
                }

                // DESTRUCTIVE per-db RCSI: one card per contended RCSI-off database. Each
                // RECONSTRUCTS a distinct FactKey="RCSI" action (the SAME shape
                // FactRemediation.BuildRcsiAction produces) so Apply routes to the destructive
                // RCSI handler (IsDestructive == true) and the two-sided informed-consent dialog
                // renders the REAL figures from this target's RcsiInactionFigures. The DB_CONFIG
                // action's own RcsiTargets are NEVER executed — they are carried only to drive
                // this fan-out.
                foreach (var t in rcsiTargets)
                {
                    // RCSI's value is relieving reader/writer blocking, so escalate the card by the
                    // contention it would address (its inaction risk) rather than the flat config
                    // band auto_shrink/page_verify get — an RCSI rec sitting at Info next to a
                    // Warning blocking-spike reads wrong.
                    var rcsiBand = RcsiSeverityBand(t.Figures);
                    items.Add(new RecommendationItem
                    {
                        Source = RecommendationSource.Engine,
                        CanonicalSeverity = rcsiBand,
                        RawSeverity = RcsiRawSeverity(rcsiBand),
                        Database = t.Database,
                        Title = $"RCSI is OFF — {t.Database}",
                        ProblemArea = finding.Category,
                        AdviceText = adviceText,
                        CopyPasteSql = AlterStatementFor(
                            new DbConfigTarget(t.Database, DbConfigSetting.ReadCommittedSnapshotOn, "OFF")),
                        Remediation = new RemediationAction(
                            "RCSI", "set", Array.Empty<ForcePlanTarget>(),
                            new[] { new DbConfigTarget(t.Database, DbConfigSetting.ReadCommittedSnapshotOn, "OFF") },
                            RcsiFigures: t.Figures),
                        StoryPathHash = finding.StoryPathHash,
                        IncidentId = finding.IncidentId,
                        StoryPath = finding.StoryPath,
                        Setting = RecommendationSetting.Rcsi,
                        WindowStartUtc = AsUtc(finding.TimeRangeStart),
                        WindowEndUtc = AsUtc(finding.TimeRangeEnd)
                    });
                }

                if (items.Count > 0)
                    return items;
                // No mappable target — fall through to the single generic card below.
            }

            return new[] { MapEngineFinding(finding) };
        }

        /// <summary>
        /// The canonical severity for an RCSI card, driven by the reader/writer contention the
        /// change would relieve (its inaction risk) — NOT the flat config-advisory band the safe
        /// settings (auto_shrink/page_verify) get. Blocking is the primary signal (RCSI directly
        /// removes reader S-lock vs writer X-lock waits); deadlocks escalate it. Counts are over
        /// the analysis window.
        /// </summary>
        internal static CanonicalSeverity RcsiSeverityBand(RcsiInactionFigures f)
        {
            if (f.BlockingEvents >= 100 || f.Deadlocks >= 10)
                return CanonicalSeverity.Critical;
            if (f.BlockingEvents >= 10 || f.Deadlocks >= 1)
                return CanonicalSeverity.Warning;
            return CanonicalSeverity.Info;
        }

        /// <summary>A raw-severity stand-in for an RCSI card's within/cross-band sort.</summary>
        private static double RcsiRawSeverity(CanonicalSeverity band) => band switch
        {
            CanonicalSeverity.Critical => 2.0,
            CanonicalSeverity.Warning => 1.0,
            _ => 0.3
        };

        /// <summary>
        /// The per-card title for a fanned-out <c>DB_CONFIG</c> safe-setting target:
        /// "&lt;SETTING&gt; — &lt;database&gt;" (e.g. "AUTO_SHRINK is ON — MyDb"), so each
        /// per-database card reads as its own specific misconfiguration.
        /// </summary>
        private static string DbConfigCardTitle(DbConfigTarget target)
        {
            var what = target.Setting switch
            {
                DbConfigSetting.AutoShrinkOff => "AUTO_SHRINK is ON",
                DbConfigSetting.AutoCloseOff => "AUTO_CLOSE is ON",
                DbConfigSetting.PageVerifyChecksum => "PAGE_VERIFY is below CHECKSUM",
                DbConfigSetting.ReadCommittedSnapshotOn => "RCSI is OFF",
                _ => "Database configuration issue"
            };
            return string.IsNullOrEmpty(target.Database) ? what : $"{what} — {target.Database}";
        }

        /// <summary>
        /// The per-card title for a fanned-out SERVER_CONFIG target (WS3):
        /// "&lt;what&gt; — &lt;server&gt;" (e.g. "MAXDOP is 0 — SQL2022"), so each server-level
        /// card reads as its own specific misconfiguration. The server name is appended when known.
        /// </summary>
        private static string ServerConfigCardTitle(ServerConfigTarget target, string? serverName)
        {
            var what = target.Setting switch
            {
                ServerConfigSetting.Maxdop => $"MAXDOP is {target.CurrentValue}",
                ServerConfigSetting.CostThreshold => $"Cost Threshold for Parallelism is {target.CurrentValue}",
                ServerConfigSetting.MaxServerMemory => "max server memory is unconfigured",
                ServerConfigSetting.MinServerMemory => "min server memory is pinned near max",
                _ => "Server configuration issue"
            };
            return string.IsNullOrEmpty(serverName) ? what : $"{what} — {serverName}";
        }

        /// <summary>
        /// Maps one engine <see cref="AnalysisFinding"/> to a SINGLE <see cref="RecommendationItem"/>
        /// (the non-fanned representation — used by <see cref="MapEngineFindings"/> for every
        /// fact key except a DB_CONFIG finding that carries safe per-database targets).
        /// Advice comes from <see cref="FactAdvice"/> for the root fact key; the de-dupe
        /// <see cref="RecommendationSetting"/> and the copy-paste SQL come from the PERSISTED
        /// <see cref="AnalysisFinding.Remediation"/> action — the drill-down that the live
        /// builders consume is ephemeral and is NOT returned by the store read, so the built
        /// action (which IS persisted) is the authoritative source on read.
        /// </summary>
        internal static RecommendationItem MapEngineFinding(AnalysisFinding finding)
        {
            var band = RecommendationDeduper.FromEngineSeverity(finding.Severity);
            // Value-stated advice frozen into StoryText at analysis time, static block as fallback.
            var advice = FactAdvice.GetComposedForFinding(finding);

            // WS4: a MISSING_INDEX action is COPY-PASTE ONLY. Render its CREATE statements as the
            // card's copy-paste SQL (via BuildCopyPasteFromAction below), but DON'T carry it as the
            // card's Remediation — creating an index is a judgement call, so there is no Apply button
            // (and no registered handler). IsMissingIndexAdvisory tells the view-model to show
            // "Copy fix" while the card stays an incident (keeps Open-in-Active-Queries / Ask-AI).
            var isMissingIndex =
                string.Equals(finding.Remediation?.FactKey, "MISSING_INDEX", StringComparison.Ordinal);

            return new RecommendationItem
            {
                Source = RecommendationSource.Engine,
                CanonicalSeverity = band,
                RawSeverity = finding.Severity,
                Database = finding.DatabaseName,
                Title = !string.IsNullOrEmpty(advice?.Headline) ? advice!.Headline : finding.RootFactKey,
                ProblemArea = finding.Category,
                AdviceText = ComposeEngineAdvice(advice),
                CopyPasteSql = BuildCopyPasteFromAction(finding.Remediation),
                Remediation = isMissingIndex ? null : finding.Remediation,
                IsMissingIndexAdvisory = isMissingIndex,
                StoryPathHash = finding.StoryPathHash,
                IncidentId = finding.IncidentId,
                StoryPath = finding.StoryPath,
                Setting = SettingFromAction(finding.Remediation),
                WindowStartUtc = AsUtc(finding.TimeRangeStart),
                WindowEndUtc = AsUtc(finding.TimeRangeEnd)
            };
        }

        /// <summary>
        /// Maps one legacy <see cref="CriticalIssueItem"/> to a
        /// <see cref="RecommendationItem"/>. Advice is the row's <c>message</c>; copy-paste is
        /// the row's <c>investigate_query</c>; the de-dupe setting is derived ONLY for the two
        /// config problem-areas, from the <c>investigate_query</c> ALTER keyword (never the
        /// free-text message). Every other legacy row gets
        /// <see cref="RecommendationSetting.None"/> and is non-deduping pass-through.
        /// </summary>
        internal static RecommendationItem MapLegacyIssue(CriticalIssueItem issue, int utcOffsetMinutes = 0)
        {
            var band = RecommendationDeduper.FromLegacySeverity(issue.Severity);
            var database = string.IsNullOrEmpty(issue.AffectedDatabase) ? null : issue.AffectedDatabase;
            var sql = string.IsNullOrEmpty(issue.InvestigateQuery) ? null : issue.InvestigateQuery;

            return new RecommendationItem
            {
                Source = RecommendationSource.Legacy,
                CanonicalSeverity = band,
                RawSeverity = RecommendationDeduper.LegacyRawSeverity(band),
                Database = database,
                Title = issue.ProblemArea,
                ProblemArea = issue.ProblemArea,
                AdviceText = string.IsNullOrEmpty(issue.Message) ? null : issue.Message,
                CopyPasteSql = sql,
                Remediation = null,           // legacy rows are advise/copy-paste only — never Apply
                StoryPathHash = null,          // the legacy store has no mute concept
                Setting = SettingFromLegacy(issue.ProblemArea, sql),
                // log_date is server-local; subtract the offset to express the same instant in UTC.
                WindowStartUtc = LegacyLogDateToUtc(issue.LogDate, utcOffsetMinutes),
                WindowEndUtc = LegacyLogDateToUtc(issue.LogDate, utcOffsetMinutes)
            };
        }

        /// <summary>
        /// Derives the de-dupe <see cref="RecommendationSetting"/> for an ENGINE finding from
        /// its persisted <see cref="RemediationAction"/>. RCSI is its own fact-key (and its own
        /// setting); DB_CONFIG carries one or more <see cref="DbConfigTarget"/>s whose
        /// <see cref="DbConfigSetting"/> maps to a setting per flagged option. Only the FIRST
        /// flagged DB-config option contributes the key (a finding's database is single, and
        /// AUTO_SHRINK/AUTO_CLOSE are the only cross-store collisions — distinct per-DB
        /// findings keep distinct rows). Returns <see cref="RecommendationSetting.None"/> when
        /// there is no action or no recognized config target (so non-config findings —
        /// CPU/waits/etc. — never de-dupe).
        /// </summary>
        internal static RecommendationSetting SettingFromAction(RemediationAction? action)
        {
            if (action is null)
                return RecommendationSetting.None;

            // RCSI is routed through a distinct fact key with a single ReadCommittedSnapshotOn
            // target; surface it as the Rcsi setting (engine-only — it never collides).
            if (string.Equals(action.FactKey, "RCSI", StringComparison.Ordinal))
                return RecommendationSetting.Rcsi;

            if (action.DbConfigTargets is { Count: > 0 } targets)
            {
                foreach (var target in targets)
                {
                    var setting = SettingFromDbConfig(target.Setting);
                    if (setting != RecommendationSetting.None)
                        return setting;
                }
            }

            return RecommendationSetting.None;
        }

        /// <summary>
        /// Maps a single <see cref="DbConfigSetting"/> to its canonical de-dupe
        /// <see cref="RecommendationSetting"/>.
        /// </summary>
        internal static RecommendationSetting SettingFromDbConfig(DbConfigSetting setting) => setting switch
        {
            DbConfigSetting.AutoShrinkOff => RecommendationSetting.AutoShrink,
            DbConfigSetting.AutoCloseOff => RecommendationSetting.AutoClose,
            DbConfigSetting.PageVerifyChecksum => RecommendationSetting.PageVerify,
            DbConfigSetting.ReadCommittedSnapshotOn => RecommendationSetting.Rcsi,
            _ => RecommendationSetting.None
        };

        /// <summary>
        /// Derives the de-dupe <see cref="RecommendationSetting"/> for a LEGACY row. ONLY the
        /// two config problem-areas are eligible; for them the setting is read from the
        /// <c>investigate_query</c> ALTER keyword (AUTO_SHRINK → AutoShrink, AUTO_CLOSE →
        /// AutoClose, QUERY_STORE → QueryStore). The free-text <c>message</c> is never parsed.
        /// Any other problem-area → <see cref="RecommendationSetting.None"/> (pass-through).
        /// </summary>
        internal static RecommendationSetting SettingFromLegacy(string? problemArea, string? investigateQuery)
        {
            var area = problemArea?.Trim();
            bool isConfigArea =
                string.Equals(area, ProblemAreaDatabaseConfiguration, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(area, ProblemAreaQueryStoreConfiguration, StringComparison.OrdinalIgnoreCase);

            if (!isConfigArea || string.IsNullOrEmpty(investigateQuery))
                return RecommendationSetting.None;

            // Match on the ALTER keyword in the copy-paste SQL only. AUTO_SHRINK / AUTO_CLOSE
            // are substrings of distinct keywords; QUERY_STORE covers both SET QUERY_STORE and
            // ALTER DATABASE ... SET QUERY_STORE forms.
            if (ContainsToken(investigateQuery, "AUTO_SHRINK"))
                return RecommendationSetting.AutoShrink;
            if (ContainsToken(investigateQuery, "AUTO_CLOSE"))
                return RecommendationSetting.AutoClose;
            if (ContainsToken(investigateQuery, "QUERY_STORE"))
                return RecommendationSetting.QueryStore;

            return RecommendationSetting.None;
        }

        /// <summary>
        /// Rebuilds the copy-paste <c>ALTER DATABASE</c> statements for an engine row from the
        /// PERSISTED action's DB-config targets (the live drill-down preview is not persisted).
        /// Mirrors the executor's SET-clause literals exactly. Returns null when the action has
        /// no DB-config targets (force-plan / clear-plan / null → no DB-config copy-paste here).
        /// </summary>
        internal static string? BuildCopyPasteFromAction(RemediationAction? action)
        {
            if (action is null)
                return null;

            // WS3: a percent-autogrowth advisory action carries per-file MODIFY FILE targets
            // (no DB-config targets). Render them via the SHARED renderer so the copy-paste is
            // byte-identical to the drill-down's alter_statement. These two target lists are
            // mutually exclusive (distinct fact keys), so order does not matter.
            if (action.FileGrowthTargets is { Count: > 0 } fileTargets)
            {
                var fsb = new StringBuilder();
                foreach (var target in fileTargets)
                {
                    if (fsb.Length > 0)
                        fsb.AppendLine();
                    fsb.Append(FactRemediation.BuildModifyFileStatement(
                        target.Database, target.LogicalFileName, target.RecommendedGrowthMb));
                }
                return fsb.Length == 0 ? null : fsb.ToString();
            }

            // WS4: a missing-index advisory action carries the SQL Server-suggested CREATE statements
            // (no DB-config targets). Surface them verbatim, one per line — copy-paste only; the
            // caller (MapEngineFinding) leaves the card's Remediation null so no Apply button appears.
            if (action.MissingIndexTargets is { Count: > 0 } indexTargets)
            {
                var isb = new StringBuilder();
                foreach (var target in indexTargets)
                {
                    if (string.IsNullOrWhiteSpace(target.CreateStatement))
                        continue;
                    if (isb.Length > 0)
                        isb.AppendLine();
                    isb.Append(target.CreateStatement);
                }
                return isb.Length == 0 ? null : isb.ToString();
            }

            if (action.DbConfigTargets is not { Count: > 0 } targets)
                return null;

            var sb = new StringBuilder();
            foreach (var target in targets)
            {
                if (sb.Length > 0)
                    sb.AppendLine();
                sb.Append(AlterStatementFor(target));
            }

            return sb.Length == 0 ? null : sb.ToString();
        }

        /// <summary>
        /// One <c>ALTER DATABASE [db] SET ...;</c> statement for a persisted
        /// <see cref="DbConfigTarget"/>. The bracketed identifier doubles any embedded
        /// close-bracket (QUOTENAME-equivalent); the SET-clause literal is selected by the
        /// hardcoded enum, matching the executor and FactRemediation's renderer.
        /// </summary>
        private static string AlterStatementFor(DbConfigTarget target)
        {
            var setClause = target.Setting switch
            {
                DbConfigSetting.AutoShrinkOff => "SET AUTO_SHRINK OFF",
                DbConfigSetting.AutoCloseOff => "SET AUTO_CLOSE OFF",
                DbConfigSetting.PageVerifyChecksum => "SET PAGE_VERIFY CHECKSUM",
                DbConfigSetting.ReadCommittedSnapshotOn => "SET READ_COMMITTED_SNAPSHOT ON",
                _ => null
            };

            if (setClause is null)
                return string.Empty;

            return $"ALTER DATABASE {QuoteName(target.Database)} {setClause};";
        }

        /// <summary>
        /// Composes the engine advice prose from a <see cref="FactAdvice"/> block: the
        /// remediation line, with the investigation line appended when present. Null when no
        /// advice matched the root fact key.
        /// </summary>
        private static string? ComposeEngineAdvice(AdviceBlock? advice)
        {
            if (advice is null)
                return null;

            var sb = new StringBuilder();
            if (!string.IsNullOrEmpty(advice.Remediation))
                sb.Append(advice.Remediation);
            if (!string.IsNullOrEmpty(advice.Investigation))
            {
                if (sb.Length > 0)
                    sb.Append(' ');
                sb.Append(advice.Investigation);
            }

            return sb.Length == 0 ? null : sb.ToString();
        }

        /// <summary>
        /// Case-insensitive substring test for an ALTER keyword in the copy-paste SQL.
        /// </summary>
        private static bool ContainsToken(string text, string token) =>
            text.Contains(token, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// QUOTENAME-equivalent: bracket an identifier, doubling any embedded close-bracket.
        /// </summary>
        private static string QuoteName(string identifier) =>
            "[" + (identifier ?? string.Empty).Replace("]", "]]") + "]";

        /// <summary>
        /// Stamps a nullable engine timestamp as UTC (the analysis pipeline records
        /// <c>TimeRangeStart/End</c> in UTC; the store read-back can return Kind=Unspecified, so
        /// fix the Kind to avoid an ambiguous later conversion). Null passes through.
        /// </summary>
        private static System.DateTime? AsUtc(System.DateTime? value) =>
            value is null ? null : System.DateTime.SpecifyKind(value.Value, System.DateTimeKind.Utc);

        /// <summary>
        /// Converts a legacy server-local <c>log_date</c> to the equivalent UTC instant by
        /// subtracting the monitored server's UTC offset, and stamps it Utc. With
        /// <paramref name="utcOffsetMinutes"/> == 0 (unit tests) this is an identity stamp.
        /// </summary>
        private static System.DateTime LegacyLogDateToUtc(System.DateTime logDate, int utcOffsetMinutes) =>
            System.DateTime.SpecifyKind(logDate.AddMinutes(-utcOffsetMinutes), System.DateTimeKind.Utc);
    }
}
