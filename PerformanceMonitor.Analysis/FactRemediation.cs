using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Builds copy-paste-ready T-SQL remediation snippets for findings whose
/// drill-down detail carries the data needed to construct a safe, parameterised
/// EXEC statement. Today this is PLAN_REGRESSION only — generates one
/// sp_query_store_force_plan block per top regressed query (up to 5), with a
/// header comment showing the regression factor and a commented unforce
/// statement for back-out.
///
/// <para>
/// PARAMETER_SENSITIVITY is intentionally excluded. Forcing the worst
/// sensitive plan locks in a plan that is bad for some parameter values; the
/// remediation is OPTION(RECOMPILE), OPTIMIZE FOR, plan guides, or query
/// rewrite — not plan force. The advice prose says this; this builder returns
/// null for that fact key.
/// </para>
/// </summary>
public static class FactRemediation
{
    /// <summary>
    /// Returns generated T-SQL for the finding, or null if no remediation
    /// shape applies. Inspects the finding's DrillDown for the data needed
    /// to fill in the EXEC parameters. The output is raw T-SQL with no
    /// markup — renderers wrap it as needed (Slack mrkdwn code block, HTML
    /// &lt;pre&gt;, etc.).
    /// </summary>
    public static string? GenerateForFinding(AnalysisFinding finding)
    {
        if (finding is null || string.IsNullOrEmpty(finding.RootFactKey))
            return null;

        return finding.RootFactKey switch
        {
            "PLAN_REGRESSION" => GenerateForPlanRegression(finding),
            "DB_CONFIG" => GenerateForDbConfig(finding),
            "FILE_AUTOGROWTH_PERCENT" => GenerateForFileAutogrowth(finding),
            _ => null
        };
    }

    /// <summary>
    /// Builds the structured, typed remediation action for the finding, or null
    /// if no execution shape applies. Mirrors <see cref="GenerateForFinding"/>'s
    /// switch on <see cref="AnalysisFinding.RootFactKey"/>: PLAN_REGRESSION yields
    /// a "force" action over the extracted targets (when any are valid); every
    /// other fact key — including PARAMETER_SENSITIVITY — yields null (no handler,
    /// no Apply affordance), consistent with the "do not force" advice.
    /// </summary>
    public static RemediationAction? BuildAction(AnalysisFinding finding)
    {
        if (finding is null || string.IsNullOrEmpty(finding.RootFactKey))
            return null;

        switch (finding.RootFactKey)
        {
            case "PLAN_REGRESSION":
                var targets = ExtractPlanRegressionTargets(finding);
                return targets.Count == 0
                    ? null
                    : new RemediationAction("PLAN_REGRESSION", "force", targets);
            case "DB_CONFIG":
                var dbConfigTargets = ExtractDbConfigTargets(finding);
                // Per-db RCSI targets are CARRIED on the safe DB_CONFIG action so the
                // Recommendations reader can fan per-db RCSI cards on read. They are NEVER
                // executed from here (DbConfigHandler only runs DbConfigTargets). Returning the
                // action when ONLY RCSI targets exist matters: a finding whose only issue is
                // contended RCSI-off databases would otherwise persist nothing (no safe target),
                // and the persisted action — not the ephemeral drill-down — is what the reader
                // fans from. This also lets the DB_CONFIG action win AnalysisService's
                // BuildAction ?? BuildRcsiAction chain, so the persisted action carries the
                // fan-out data (BuildRcsiAction's singular action does not carry RcsiTargets).
                var rcsiTargets = CollectRcsiTargets(finding);
                return dbConfigTargets.Count == 0 && rcsiTargets.Count == 0
                    ? null
                    : new RemediationAction("DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(), dbConfigTargets,
                                            RcsiTargets: rcsiTargets);
            default:
                return null;
        }
    }

    /// <summary>
    /// Builds the percent-autogrowth action for a FILE_AUTOGROWTH_PERCENT finding, or null when
    /// no offending file is present (WS3). Parallel to <see cref="BuildAction"/> — a SEPARATE
    /// entry point so neither switch grows. The action carries FactKey "FILE_AUTOGROWTH_PERCENT"
    /// and the "set" verb: it is APPLY-able (FileAutogrowthHandler runs one
    /// <c>ALTER DATABASE … MODIFY FILE (… FILEGROWTH = N MB)</c> per offending file — a
    /// metadata-only, online, non-destructive change), AND it carries the per-file
    /// <see cref="FileGrowthTarget"/>s through the persisted-action round-trip so the
    /// Recommendations reader can also render the copy-paste MODIFY FILE statements on read (the
    /// drill-down the targets come from is ephemeral).
    /// </summary>
    public static RemediationAction? BuildFileAutogrowthAction(AnalysisFinding finding)
    {
        if (finding is null || !string.Equals(finding.RootFactKey, "FILE_AUTOGROWTH_PERCENT", StringComparison.Ordinal))
            return null;

        var fileTargets = ExtractFileGrowthTargets(finding);
        return fileTargets.Count == 0
            ? null
            : new RemediationAction("FILE_AUTOGROWTH_PERCENT", "set", Array.Empty<ForcePlanTarget>(),
                                    FileGrowthTargets: fileTargets);
    }

    /// <summary>
    /// Builds the missing-index advisory action for a MISSING_INDEX finding (WS4), or null when the
    /// drill-down carries no suggested index. Parallel to <see cref="BuildAction"/> — a SEPARATE
    /// entry point so neither switch grows. The action carries FactKey "MISSING_INDEX" and is
    /// COPY-PASTE ONLY: there is deliberately NO registered handler, so it never drives Apply
    /// (creating an index is a judgement call — over-indexing, write + storage cost — so the operator
    /// copies the suggested statement and decides). It carries the SQL Server-suggested CREATE
    /// statements through the persisted-action round-trip so the Recommendations reader can render
    /// them on read (the <c>missing_indexes</c> drill-down they come from is ephemeral). The reader
    /// surfaces them as the card's copy-paste SQL and leaves the card's Remediation null (no Apply).
    /// </summary>
    public static RemediationAction? BuildMissingIndexAction(AnalysisFinding finding)
    {
        if (finding is null || !string.Equals(finding.RootFactKey, "MISSING_INDEX", StringComparison.Ordinal))
            return null;

        var indexTargets = ExtractMissingIndexTargets(finding);
        return indexTargets.Count == 0
            ? null
            : new RemediationAction("MISSING_INDEX", "advise", Array.Empty<ForcePlanTarget>(),
                                    MissingIndexTargets: indexTargets);
    }

    // ── WS3: server-level config (MAXDOP / CTFP / max & min server memory) ──────────
    //
    // The per-setting CONFIG_* facts (CONFIG_MAXDOP / CONFIG_CTFP / CONFIG_MAX_MEMORY_MB /
    // CONFIG_MIN_MAX_MEMORY_NARROW) each root their OWN advisory finding. The drill-down
    // attaches a `server_config` array carrying ONLY the bad setting(s) for that finding plus
    // the edition + cores-per-socket needed to compute the recommended MAXDOP. The recommended
    // value is computed HERE (not in the collector) so the edition-aware MAXDOP cap is unit-
    // testable without a server. CTFP is a flat 50; the two memory settings are advise-only and
    // carry their current value as "recommended" (the executor refuses to apply them).

    /// <summary>The CTFP value WS3 recommends — the canonical AuditConfig figure.</summary>
    public const long RecommendedCostThreshold = 50;

    /// <summary>The MAXDOP default (0) WS3 flags as bad — unlimited parallelism.</summary>
    public const long BadMaxdopValue = 0;

    /// <summary>The "max server memory (MB)" default (~2 PB) WS3 flags as unconfigured.</summary>
    public const long UnconfiguredMaxMemoryMb = 2147483647;

    /// <summary>The MAXDOP cap Microsoft's guidance applies to a single NUMA node.</summary>
    public const long MaxdopCap = 8;

    /// <summary>
    /// The recommended MAXDOP, derived from CPU topology — NOT from edition. Microsoft's guidance is
    /// driven by logical processors per NUMA node, not the SQL Server SKU: keep MAXDOP at or under
    /// the processors in a single NUMA node, capped at 8 (only large multi-NUMA hardware with
    /// &gt; 16 logical processors per node goes higher — half the per-node count, max 16 — which we
    /// can't detect without NUMA topology). We use cores-per-socket as the best available proxy for
    /// NUMA-node size (numa_node_count isn't collected) and apply the cap: MAXDOP =
    /// min(cores-per-socket, 8). With cores unknown (0) the safe general cap of 8 stands.
    /// </summary>
    public static long RecommendedMaxdop(int coresPerSocket)
    {
        if (coresPerSocket <= 0)
            return MaxdopCap;
        return Math.Min(coresPerSocket, MaxdopCap);
    }

    /// <summary>
    /// Builds the server-level config action for a CONFIG_* finding (WS3), or null when the
    /// drill-down carries no bad server-config setting. Parallel to <see cref="BuildAction"/> —
    /// a SEPARATE entry point so neither switch grows. The action carries FactKey
    /// "SERVER_CONFIG" and the "set" verb. MAXDOP / CTFP targets are APPLY-able
    /// (ServerConfigHandler runs <c>sp_configure</c>+<c>RECONFIGURE</c>); the two memory targets
    /// are advise-only (the executor refuses them) but still ride the persisted-action round-trip
    /// so the Recommendations reader can render their copy-paste on read (the drill-down is
    /// ephemeral). Each finding roots on a single CONFIG_* key, so its drill-down carries a
    /// single bad setting → a single target; the reader's fan loop turns each into one card.
    /// </summary>
    public static RemediationAction? BuildServerConfigAction(AnalysisFinding finding)
    {
        var targets = ExtractServerConfigTargets(finding);
        return targets.Count == 0
            ? null
            : new RemediationAction("SERVER_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
                                    ServerConfigTargets: targets);
    }

    /// <summary>
    /// Extracts the server-config target(s) from a CONFIG_* finding's drill-down
    /// <c>server_config</c> array. Each row carries the structured fields <c>setting</c>
    /// (canonical id), <c>current_value</c>, and (for MAXDOP) <c>cores_per_socket</c> — never
    /// parsing human prose. The recommended value is computed per setting (topology-based MAXDOP =
    /// min(cores-per-socket, 8) / flat CTFP 50 / current for the advise-only memory settings). A
    /// defensive cap of 8 mirrors the other extractors.
    /// </summary>
    public static IReadOnlyList<ServerConfigTarget> ExtractServerConfigTargets(AnalysisFinding finding)
    {
        var targets = new List<ServerConfigTarget>();

        if (finding?.DrillDown is null ||
            !finding.DrillDown.TryGetValue("server_config", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 8) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var settingId = GetString(row, "setting");
            if (string.IsNullOrEmpty(settingId))
                continue;

            var current = GetInt64(row, "current_value");
            var cores = GetInt(row, "cores_per_socket");

            switch (settingId)
            {
                case "maxdop":
                    targets.Add(new ServerConfigTarget(
                        ServerConfigSetting.Maxdop, current, RecommendedMaxdop(cores)));
                    break;
                case "ctfp":
                    targets.Add(new ServerConfigTarget(
                        ServerConfigSetting.CostThreshold, current, RecommendedCostThreshold));
                    break;
                case "max_memory":
                    // Advise-only: the right value is RAM-dependent. Carry current as
                    // "recommended" — the executor never applies it; the card is copy-paste only.
                    targets.Add(new ServerConfigTarget(
                        ServerConfigSetting.MaxServerMemory, current, current));
                    break;
                case "min_memory":
                    // Advise-only (pinned near max). Carry current as "recommended".
                    targets.Add(new ServerConfigTarget(
                        ServerConfigSetting.MinServerMemory, current, current));
                    break;
            }
        }

        return targets;
    }

    /// <summary>
    /// The fraction of max server memory at/above which min server memory is "pinned near max"
    /// (WS3): min &gt;= 80% of max. Pinning the floor that high stops SQL releasing memory back to
    /// the OS. Single source of truth so Dashboard and Lite collectors agree.
    /// </summary>
    public const double MinMaxMemoryNarrowFraction = 0.80;

    /// <summary>
    /// Builds the CONFIG_MIN_MAX_MEMORY_NARROW fact, or null when it does not apply — shared by
    /// the Dashboard and Lite collectors so the "pinned near max" rule never drifts. Emitted ONLY
    /// when max server memory is CONFIGURED (not the ~2 PB default) AND min &gt;=
    /// <see cref="MinMaxMemoryNarrowFraction"/> of max. Carries the configured min + max in
    /// metadata for the advice/disclosure. The fact's presence is itself the "bad" flag
    /// (FactScorer scores it 0.4 unconditionally).
    /// </summary>
    public static Fact? BuildNarrowMemoryFact(int serverId, double? maxMemoryMb, double? minMemoryMb)
    {
        if (maxMemoryMb is not double max || minMemoryMb is not double min)
            return null;

        // Only meaningful once max is actually configured below the 2 PB default; an unconfigured
        // max is its OWN finding (CONFIG_MAX_MEMORY_MB) and min-vs-2PB is never "narrow".
        if (max >= UnconfiguredMaxMemoryMb || max <= 0)
            return null;

        if (min < max * MinMaxMemoryNarrowFraction)
            return null;

        return new Fact
        {
            Source = "config",
            Key = "CONFIG_MIN_MAX_MEMORY_NARROW",
            Value = min,
            ServerId = serverId,
            Metadata = new Dictionary<string, double>
            {
                ["min_memory_mb"] = min,
                ["max_memory_mb"] = max
            }
        };
    }

    /// <summary>The hardcoded <c>sp_configure</c> name for a server-config setting (display + executor share the taxonomy).</summary>
    public static string SpConfigureName(ServerConfigSetting setting) => setting switch
    {
        ServerConfigSetting.Maxdop => "max degree of parallelism",
        ServerConfigSetting.CostThreshold => "cost threshold for parallelism",
        ServerConfigSetting.MaxServerMemory => "max server memory (MB)",
        ServerConfigSetting.MinServerMemory => "min server memory (MB)",
        _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown ServerConfigSetting")
    };

    /// <summary>
    /// Renders one copy-paste <c>EXEC sys.sp_configure N'&lt;name&gt;', &lt;value&gt;; RECONFIGURE;</c>
    /// statement for a server-config target. The name is a hardcoded literal selected by the enum;
    /// the value is the recommended value. Advisory text only — the executor builds its OWN
    /// statement with a bound @value parameter and never executes this rendered string.
    /// </summary>
    public static string BuildSpConfigureStatement(ServerConfigSetting setting, long value)
    {
        return $"EXEC sys.sp_configure N'{SpConfigureName(setting)}', {value};\nRECONFIGURE;";
    }
    /// <summary>
    /// Builds the DESTRUCTIVE RCSI remediation action for a DB_CONFIG finding, or null
    /// when it does not apply (B3 Phase 3). Parallel to <see cref="BuildAction"/>, which
    /// stays EXACTLY as-is — this is a SEPARATE entry point so the singular-Remediation
    /// pipeline (one RemediationAction per detail item) is unchanged; the caller emits
    /// the RCSI action on a SECOND "Enable RCSI (advanced)" detail item (PR-B).
    ///
    /// <para>
    /// Emits ONLY when a <c>config_issues</c> row is RCSI-OFF (the JSON <c>rcsi</c> value
    /// is <c>false</c> — M2-1 boolean polarity, mirroring the rcsiOffDatabases scan in
    /// <see cref="GenerateForDbConfig"/>) AND the §3.3 enrichment is present (the
    /// <c>rcsi_blocking_events</c>/<c>rcsi_deadlocks</c>/<c>rcsi_reader_writer_pct</c>
    /// fields exist on that row). The action carries FactKey "RCSI" (a distinct handler)
    /// and reuses a single <see cref="DbConfigTarget"/> with
    /// <see cref="DbConfigSetting.ReadCommittedSnapshotOn"/> and CurrentValue "OFF".
    /// </para>
    /// </summary>
    public static RemediationAction? BuildRcsiAction(AnalysisFinding finding)
    {
        // Single source of truth for the qualifying-db scan + the real-contention gate:
        // build the alert-path action from the FIRST collected target (or null when none),
        // so the alert path and the per-db Recommendations cards apply the SAME gate.
        var rcsiTargets = CollectRcsiTargets(finding);
        if (rcsiTargets.Count == 0)
            return null;

        var first = rcsiTargets[0];
        var target = new DbConfigTarget(first.Database, DbConfigSetting.ReadCommittedSnapshotOn, "OFF");

        // The risk-of-NOT-changing figures were captured by CollectRcsiTargets (the finding
        // was available) and are carried on the persisted action, so the informed-consent
        // dialog renders the REAL numbers at apply time — when only the persisted action
        // survives (the UI apply call site passes no finding). FactRiskDisclosure reads these
        // from the action in preference to the finding.
        return new RemediationAction("RCSI", "set", Array.Empty<ForcePlanTarget>(), new[] { target }, first.Figures);
    }

    /// <summary>
    /// Collects EVERY per-database RCSI target a DB_CONFIG finding qualifies for (B3 Phase 3,
    /// recommendations rebuild). Scans <c>config_issues</c> EXACTLY like
    /// <see cref="BuildRcsiAction"/> — a row qualifies only when its <c>rcsi</c> value is
    /// <c>false</c> (M2-1 polarity: RCSI is OFF) AND the §3.3 enrichment is present
    /// (<see cref="HasRcsiEnrichment"/>) — but additionally GATES on REAL contention: a row is
    /// included only when it carries a positive <c>rcsi_blocking_events</c> OR
    /// <c>rcsi_deadlocks</c> count. An RCSI-off database with NO observed blocking/deadlocks is
    /// NOT recommended: enabling RCSI there only adds tempdb version-store cost for no
    /// concurrency benefit. Unlike <see cref="BuildRcsiAction"/> (which stops at the first
    /// qualifying db for the singular alert action) this returns ALL qualifying databases, each
    /// carrying its own <see cref="RcsiInactionFigures"/>, so the Recommendations reader can fan
    /// one per-database RCSI card. A defensive cap of 50 mirrors the other extractors.
    /// </summary>
    public static IReadOnlyList<RcsiTarget> CollectRcsiTargets(AnalysisFinding finding)
    {
        var targets = new List<RcsiTarget>();

        if (finding is null || !string.Equals(finding.RootFactKey, "DB_CONFIG", StringComparison.Ordinal))
            return targets;

        if (finding.DrillDown is null ||
            !finding.DrillDown.TryGetValue("config_issues", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 50) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var database = GetString(row, "database");
            if (string.IsNullOrEmpty(database))
                continue;

            // M2-1: rcsi == true means RCSI is ON. Collect ONLY when RCSI is OFF
            // (JsonValueKind.False) — mirrors the rcsiOffDatabases scan exactly.
            if (!row.TryGetProperty("rcsi", out var r) || r.ValueKind != JsonValueKind.False)
                continue;

            // The §3.3 enrichment must be present (these fields are emitted only for
            // RCSI-off databases). Without it the inaction side cannot be quantified
            // and no RCSI affordance should appear.
            if (!HasRcsiEnrichment(row))
                continue;

            // Reader/writer-contention gate: RCSI only relieves blocking BETWEEN readers and
            // writers (a reader's S lock waiting on a writer's X lock, or a writer waiting behind
            // a reader's S lock). Writer/writer blocking (X/IX/U) and raw deadlock counts are NOT
            // helped by RCSI — recommending it there only adds tempdb version-store overhead. So
            // gate on the reader/writer SHARE (classified from the blocked-process report's lock
            // modes in CollectRcsiInactionFigures), using the SAME threshold the consent disclosure
            // uses to say "RCSI eliminates this". An rcsi-off db whose contention is writer/writer-
            // dominant (pct below the threshold) or where no reader/writer blocking was captured
            // (pct null) gets NO card. The alert path and the cards share this gate (BuildRcsiAction
            // builds from this same list). The raw blocking/deadlock counts are still carried for
            // the consent dialog's magnitude context.
            var readerWriterPct = GetNullableInt(row, "rcsi_reader_writer_pct");
            if (readerWriterPct is not int pct || pct < FactRiskDisclosure.ReaderWriterMeaningfulPct)
                continue;

            targets.Add(new RcsiTarget(
                database,
                new RcsiInactionFigures(
                    BlockingEvents: GetInt(row, "rcsi_blocking_events"),
                    Deadlocks: GetInt(row, "rcsi_deadlocks"),
                    ReaderWriterPct: pct)));
        }

        return targets;
    }

    /// <summary>
    /// Renders the display-only RCSI code block for the "Enable RCSI (advanced)" detail
    /// item (B3 Phase 3, §4.3): the enabling ALTER with a "was OFF" comment, a commented
    /// back-out statement, and the test-on-a-copy note. Returns null when no RCSI action
    /// applies (RCSI on / no enrichment). The Dashboard executor builds its OWN validated
    /// + bracketed statement and never executes this rendered text.
    /// </summary>
    public static string? GenerateRcsiPreview(AnalysisFinding finding)
    {
        var action = BuildRcsiAction(finding);
        if (action?.DbConfigTargets is not { Count: > 0 } targets)
            return null;

        var db = targets[0].Database;
        var quoted = QuoteName(db);
        var sb = new StringBuilder();
        sb.AppendLine($"-- Database: {db}");
        sb.AppendLine($"ALTER DATABASE {quoted} SET READ_COMMITTED_SNAPSHOT ON;   -- was OFF");
        sb.AppendLine();
        sb.AppendLine("-- To back out (itself a destructive change — blocking returns):");
        sb.AppendLine($"-- ALTER DATABASE {quoted} SET READ_COMMITTED_SNAPSHOT OFF;");
        sb.AppendLine();
        sb.Append("-- Test on a copy first if the application relies on default locking behavior or uses NOLOCK hints.");
        return sb.ToString();
    }

    /// <summary>
    /// Renders the display-only clear-cached-plan code block for the "Clear cached plan
    /// (advanced)" detail item (§5/§6, PR-B). It shows, per qualifying query hash, the
    /// anomaly figures the detector captured and a NOTE that the actual plan handle(s)
    /// are LIVE-RESOLVED at apply (the snapshot handle is display-only, never executed).
    /// Returns null when no CLEAR_PLAN action applies. The Dashboard executor builds its
    /// OWN single-`@plan_handle` DBCC statements at apply against the live-resolved handles
    /// and never executes this rendered text.
    /// </summary>
    public static string? GenerateClearPlanPreview(AnalysisFinding finding)
    {
        var action = BuildClearPlanAction(finding);
        if (action?.ClearPlanTargets is not { Count: > 0 } targets)
            return null;

        var sb = new StringBuilder();
        var emitted = 0;
        foreach (var t in targets)
        {
            if (emitted > 0)
                sb.AppendLine();

            var dbLabel = string.IsNullOrEmpty(t.Database) ? "(unknown — resolved live)" : t.Database;
            sb.AppendLine($"-- Database: {dbLabel}");
            sb.AppendLine($"-- query_hash = {t.QueryHash}");
            sb.AppendLine(
                $"--   ~{t.AnomalyRatio.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture)}x normal per-exec CPU " +
                $"({t.CurrentCpuPerExecMs.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture)} ms vs baseline " +
                $"{t.BaselineCpuPerExecMs.ToString("0.0", System.Globalization.CultureInfo.InvariantCulture)} ms)");
            if (!string.IsNullOrEmpty(t.LatestPlanHandle))
                sb.AppendLine($"--   last-seen plan handle: {t.LatestPlanHandle} (display only — apply RE-RESOLVES live)");
            sb.AppendLine("-- Apply live-resolves the currently-cached plan_handle(s) for this query hash and runs:");
            sb.AppendLine("--   DBCC FREEPROCCACHE(<resolved plan_handle>);   -- per surviving handle");
            sb.AppendLine("-- There is NO un-clear: the prior plan is gone. The recompile is not guaranteed better.");
            emitted++;
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Builds the DESTRUCTIVE clear-cached-plan remediation action for a CPU finding
    /// (CPU_SQL_PERCENT / CPU_SPIKE), or null when it does not apply. PARALLEL to
    /// <see cref="BuildAction"/>, which stays EXACTLY as-is (it never emits CLEAR_PLAN) —
    /// this is a SEPARATE entry point, mirroring <see cref="BuildRcsiAction"/>, so the
    /// CPU finding's normal actions are unchanged and the CLEAR_PLAN action rides a
    /// SECOND "Clear cached plan (advanced)" detail item (emitted in PR-B).
    ///
    /// <para>
    /// Emits ONLY when the finding carries an <c>abnormal_cpu_plans</c> drill-down (the
    /// §2 detector enrichment) with at least one qualifying row (the detector has
    /// already applied the per-exec anomaly threshold + materiality gate + the §2a
    /// row-level first-collection/restart exclusion server-side; this builder does NOT
    /// re-derive the math). The action carries FactKey "CLEAR_PLAN" (a distinct
    /// destructive handler), one <see cref="ClearPlanTarget"/> per qualifying row (the
    /// stable <c>query_hash</c> is the only execution input — the executor re-resolves
    /// the live <c>plan_handle(s)</c> at apply), and the <see cref="ClearPlanFigures"/>
    /// from the FIRST qualifying row so the informed-consent dialog renders the REAL
    /// numbers at apply time even with no finding in hand.
    /// </para>
    /// </summary>
    public static RemediationAction? BuildClearPlanAction(AnalysisFinding finding)
    {
        if (finding is null ||
            (!string.Equals(finding.RootFactKey, "CPU_SQL_PERCENT", StringComparison.Ordinal) &&
             !string.Equals(finding.RootFactKey, "CPU_SPIKE", StringComparison.Ordinal)))
            return null;

        if (finding.DrillDown is null ||
            !finding.DrillDown.TryGetValue("abnormal_cpu_plans", out var raw) ||
            raw is null)
            return null;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return null;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return null;

        var targets = new List<ClearPlanTarget>();
        ClearPlanFigures? figures = null;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 5) break;       // defensive cap, sibling of the force-plan cap discipline
            if (row.ValueKind != JsonValueKind.Object) continue;

            var queryHash = GetString(row, "query_hash");
            // query_hash is the ONLY execution input. It must be present and look like a
            // hex handle (0x...); the detector emits it via CONVERT(VARCHAR, .., 1). A
            // blank/garbage value cannot become a target (it would never resolve a handle).
            if (string.IsNullOrEmpty(queryHash) ||
                !queryHash.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ||
                queryHash.Length <= 2)
                continue;

            var database = GetString(row, "database");
            var current = GetDouble(row, "current_cpu_per_exec_ms");
            var baseline = GetDouble(row, "baseline_cpu_per_exec_ms");
            var ratio = GetDouble(row, "anomaly_ratio");

            targets.Add(new ClearPlanTarget(
                Database: database,
                QueryHash: queryHash,
                CurrentCpuPerExecMs: current,
                BaselineCpuPerExecMs: baseline,
                AnomalyRatio: ratio,
                LatestPlanHandle: GetString(row, "latest_plan_handle")));

            // Capture the risk-of-NOT-changing figures from the FIRST qualifying row,
            // carried on the persisted action so the dialog shows REAL numbers at apply
            // time (the RcsiInactionFigures precedent). The co-fired flags steer the
            // honest tool-choice disclosure (§5).
            figures ??= new ClearPlanFigures(
                CurrentCpuPerExecMs: current,
                BaselineCpuPerExecMs: baseline,
                AnomalyRatio: ratio,
                CpuPercent: GetInt(row, "cpu_percent"),
                PlanRegressionCoFired: GetBool(row, "plan_regression_cofired"),
                ParameterSensitivityCoFired: GetBool(row, "parameter_sensitivity_cofired"));
        }

        if (targets.Count == 0)
            return null;

        return new RemediationAction(
            "CLEAR_PLAN",
            "clear",
            Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: null,
            RcsiFigures: null,
            ClearPlanTargets: targets,
            ClearPlanFigures: figures);
    }

    /// <summary>
    /// True when the RCSI inaction-risk enrichment fields are present on the row (the
    /// collector emits them only for RCSI-off databases). At least one of the three
    /// structured fields must exist.
    /// </summary>
    private static bool HasRcsiEnrichment(JsonElement row) =>
        row.TryGetProperty("rcsi_blocking_events", out _) ||
        row.TryGetProperty("rcsi_deadlocks", out _) ||
        row.TryGetProperty("rcsi_reader_writer_pct", out _);

    /// <summary>
    /// The <c>replica_role</c> value that means "the replica a force-plan statement acts on by
    /// default" — <c>sys.query_store_replicas.replica_name</c> for the primary. Compared
    /// case-insensitively because the collector passes the server's own casing through verbatim
    /// ("Primary"), while hand-built and seeded drill-downs use "PRIMARY".
    /// </summary>
    private const string PrimaryReplicaRole = "Primary";

    /// <summary>
    /// A defensive bound on how many <c>regressed_queries</c> rows are examined. The producers already
    /// <c>LIMIT 5</c>, and the target cap is 5, but the replica preference below has to keep looking
    /// after the cap is reached (a primary's row can arrive AFTER a secondary's and must still be able
    /// to take the slot), so the loop no longer stops at the cap and needs its own ceiling.
    /// </summary>
    private const int MaxPlanRegressionRowsScanned = 50;

    /// <summary>True when the row was attributed to the PRIMARY replica, or when the server did not
    /// attribute replicas at all — which is every standalone server, every AG without Query Store for
    /// secondary replicas, and everything below SQL Server 2022. An unattributed row IS the primary's:
    /// the Query Store collector only ever reads from primaries.</summary>
    private static bool IsPrimaryReplicaRow(string? replicaRole) =>
        string.IsNullOrEmpty(replicaRole) ||
        string.Equals(replicaRole, PrimaryReplicaRole, StringComparison.OrdinalIgnoreCase);

    /// <summary>True when the row names a replica OTHER than the primary, i.e. the disclosure below
    /// applies. Distinct from <c>!IsPrimaryReplicaRow</c> only in intent, but the two read very
    /// differently at the call sites.</summary>
    private static bool IsNonPrimaryReplicaRow(string? replicaRole) =>
        !string.IsNullOrEmpty(replicaRole) && !IsPrimaryReplicaRow(replicaRole);

    /// <summary>
    /// Extracts the typed force-plan targets from a PLAN_REGRESSION finding's
    /// drill-down. This is the single parse: the preview renderer
    /// (<see cref="GenerateForPlanRegression"/>) renders entirely from this list,
    /// and <see cref="BuildAction"/> persists it. Applies the same guards as the
    /// renderer always has — database non-empty, query_id &gt; 0, best_plan_id
    /// &gt; 0 — and the same cap of 5 targets. Reads every value the preview
    /// renders (including the two cpu/exec numbers) so the renderer needs no
    /// second drill-down read.
    ///
    /// <para><b>Which replica's regression wins (#1882).</b> #1850 made the drill-down per-REPLICA: on
    /// an AG with Query Store for secondary replicas enabled, one query can regress on the primary AND
    /// on a secondary and arrive as two rows differing in <c>replica_role</c>, each with its own
    /// <c>best_plan_id</c>. #1850 de-duped on (database, query_id) keeping the FIRST row — which, since
    /// the SQL orders <c>regression_factor DESC</c>, is the worst regression on any replica — and
    /// deliberately left the product question open. It is answered here: <b>the primary's row wins when
    /// both exist</b>, and regression factor only breaks ties among rows of the same replica class.</para>
    ///
    /// <para>That is forced by what the emitted statement actually does, which turns out to be narrower
    /// than "forcing is per query". Per
    /// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql">
    /// sp_query_store_force_plan</see>, forcing IS scopeable per replica — there is a fourth
    /// <c>@replica_group_id</c> argument, and the docs say "You can force plans on a secondary replica
    /// when Query Store for readable secondaries is enabled. Execute sp_query_store_force_plan and
    /// sp_query_store_unforce_plan on the primary replica. Using the @replica_group_id argument defaults
    /// to the primary replica". Two consequences, and the second is the defect:</para>
    /// <list type="number">
    /// <item>Every force runs ON the primary regardless of which replica it targets, so a secondary's
    /// regression does not need to be discarded — it is actionable.</item>
    /// <item>The statement this class renders omits <c>@replica_group_id</c>, so <b>it forces on the
    /// PRIMARY</b> by that documented default. Letting a secondary's regression win the de-dup therefore
    /// did not produce a secondary-scoped recommendation; it produced a primary-scoped one justified by
    /// evidence from a different replica's workload, with nothing on screen saying so. Preferring the
    /// primary's row makes the evidence match the scope the statement already had.</item>
    /// </list>
    ///
    /// <para>The omission stays deliberate rather than being filled in: <c>@replica_group_id</c> is a
    /// replica SET NUMBER from
    /// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-query-store-replicas">
    /// sys.query_store_replicas</see>, and the collector stores that view's <c>replica_name</c> (a ROLE
    /// — "Primary", "Secondary", "Geo Secondary") rather than the id, so there is no correct number to
    /// emit. The id would be the wrong thing to persist anyway: that catalog view accumulates one row per
    /// (replica, role) across failovers, so a group id captured at analysis time can silently re-point
    /// afterwards. A secondary-only regression is instead rendered WITH a disclosure that names the
    /// replica and tells the operator how to scope it themselves — see
    /// <see cref="GenerateForPlanRegression"/>.</para>
    ///
    /// <para>Zero effect on any server without replica attribution: <c>replica_role</c> is absent or
    /// empty on every standalone server, every non-AG server and everything below SQL Server 2022, one
    /// row per query arrives exactly as before, and the preference is a no-op.</para>
    /// </summary>
    public static IReadOnlyList<ForcePlanTarget> ExtractPlanRegressionTargets(AnalysisFinding finding)
    {
        var targets = new List<ForcePlanTarget>();

        if (finding?.DrillDown is null ||
            !finding.DrillDown.TryGetValue("regressed_queries", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        /* Insertion order is kept separately from the winners so the emitted order stays what it always
           was — worst regression first, since a query's FIRST row is its worst and the rows arrive
           regression_factor DESC. Upgrading a query to its primary row changes WHICH plan is forced, never
           where the query sits in the list. */
        var order = new List<(string Database, long QueryId)>();
        var winners = new Dictionary<(string Database, long QueryId), ForcePlanTarget>();
        var scanned = 0;

        foreach (var row in element.EnumerateArray())
        {
            if (++scanned > MaxPlanRegressionRowsScanned) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var database = GetString(row, "database");
            var queryId = GetInt64(row, "query_id");
            var bestPlanId = GetInt64(row, "best_plan_id");
            if (string.IsNullOrEmpty(database) || queryId <= 0 || bestPlanId <= 0)
                continue;

            var key = (database, queryId);
            var replicaRole = GetString(row, "replica_role");
            var isIncumbent = winners.TryGetValue(key, out var incumbent);

            /* A query already holding a slot only changes plan when the newcomer is the PRIMARY's row and
               the incumbent is not — the whole preference, in one condition. Everything else (a second
               secondary, a worse primary row, a repeat) leaves the incumbent alone, which preserves
               #1850's "first wins" among rows of equal standing. */
            if (isIncumbent && !(IsPrimaryReplicaRow(replicaRole) && !IsPrimaryReplicaRow(incumbent!.ReplicaRole)))
                continue;

            /* The cap counts DISTINCT queries, as it always has. Applied only to a query that does not
               already hold a slot, so a late primary row can still upgrade one of the five. */
            if (!isIncumbent && order.Count >= 5)
                continue;

            if (!isIncumbent)
                order.Add(key);

            winners[key] = new ForcePlanTarget(
                Database: database,
                QueryId: queryId,
                PlanId: bestPlanId,
                BestPlanHash: GetString(row, "best_plan_hash"),
                LatestPlanHash: GetString(row, "latest_plan_hash"),
                LatestCpuPerExecUs: GetDouble(row, "latest_cpu_per_exec_us"),
                BestCpuPerExecUs: GetDouble(row, "best_cpu_per_exec_us"),
                RegressionFactor: GetDouble(row, "regression_factor"),
                ReplicaRole: string.IsNullOrEmpty(replicaRole) ? null : replicaRole,
                ParameterSensitivityCoFired: GetBool(row, "parameter_sensitivity_cofired"));
        }

        foreach (var key in order)
            targets.Add(winners[key]);

        return targets;
    }

    /// <summary>
    /// Thin renderer over <see cref="ExtractPlanRegressionTargets"/>. The output
    /// is byte-for-byte the same preview the inline parse produced before the
    /// extract-once refactor (guarded by the render-stability golden test),
    /// including the two "(cpu/exec ... us)" comment lines.
    ///
    /// <para>#1882 adds the replica lines, and adds NOTHING when the target carries no
    /// <c>replica_role</c> — which is every standalone server, every non-AG server, everything below
    /// SQL Server 2022, and the deprecated Dashboard's drill-down, whose SQL has no replica column at
    /// all. That is what keeps the byte-for-byte golden meaningful rather than merely re-baselined:
    /// the no-replica rendering is still the one it pins.</para>
    /// </summary>
    private static string? GenerateForPlanRegression(AnalysisFinding finding)
    {
        var targets = ExtractPlanRegressionTargets(finding);
        if (targets.Count == 0)
            return null;

        var sb = new StringBuilder();
        var emitted = 0;

        foreach (var target in targets)
        {
            if (emitted > 0)
                sb.AppendLine();

            sb.AppendLine($"-- Database: {target.Database}");
            sb.AppendLine($"-- query_id = {target.QueryId}, forcing plan_id = {target.PlanId}");
            if (!string.IsNullOrEmpty(target.LatestPlanHash))
                sb.AppendLine($"--   latest plan hash: {target.LatestPlanHash} (cpu/exec {target.LatestCpuPerExecUs:F0} us)");
            if (!string.IsNullOrEmpty(target.BestPlanHash))
                sb.AppendLine($"--   best plan hash:   {target.BestPlanHash}   (cpu/exec {target.BestCpuPerExecUs:F0} us)");
            sb.AppendLine($"--   regression factor: {target.RegressionFactor:F1}x");
            if (!string.IsNullOrEmpty(target.ReplicaRole))
                sb.AppendLine($"--   measured on replica: {target.ReplicaRole}");
            AppendSecondaryReplicaDisclosure(sb, target);
            AppendParameterSensitivityCaution(sb, target);
            sb.AppendLine($"USE {QuoteName(target.Database)};");
            sb.AppendLine($"EXEC sys.sp_query_store_force_plan @query_id = {target.QueryId}, @plan_id = {target.PlanId};");
            sb.AppendLine();
            sb.AppendLine($"-- To back out:");
            sb.AppendLine($"-- EXEC sys.sp_query_store_unforce_plan @query_id = {target.QueryId}, @plan_id = {target.PlanId};");
            AppendUnforceScopeNote(sb, target);

            emitted++;
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// The #1882 disclosure, emitted only for a target whose regression was measured on a replica OTHER
    /// than the primary. The rendered <c>sp_query_store_force_plan</c> call omits
    /// <c>@replica_group_id</c>, which per
    /// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql">
    /// the docs</see> "defaults to the primary replica" — so the statement is primary-scoped while its
    /// evidence is not, and an operator has no way to see that from the numbers above it. Saying so is
    /// the whole fix: the recommendation stays actionable (forcing is executed on the primary in every
    /// case, so a secondary's regression is not something to discard), it just stops being silent about
    /// what it is recommending.
    ///
    /// <para>The scoped form is described rather than spelled out as a paste-ready statement, and #1914
    /// probed the reason live rather than leaving it as the doubt #1882 recorded. <c>sp_query_store_force_plan</c>
    /// is an <c>EXTENDED_STORED_PROCEDURE</c>, so <c>sys.system_parameters</c> is EMPTY for it and its
    /// argument surface can only be established by execution. Measured on SQL Server 2022 (16.0.4255.1)
    /// and SQL Server 2025 (17.0.4045.5), each call form run from a freshly-unforced plan:</para>
    /// <list type="bullet">
    /// <item><b>The documented four-argument order does not work.</b>
    /// <c>@query_id, @plan_id, @disable_optimized_plan_forcing = 0, @replica_group_id = 1</c> fails on
    /// BOTH versions with error 12463, "Role id should be between (including) 1 and 4" — for a role id of
    /// 1, which is in that range. The same call with <c>@disable_optimized_plan_forcing = 1</c> succeeds.
    /// So the form the reference page's syntax block prescribes is the one that errors.</item>
    /// <item><b>The three-argument named call, skipping the middle argument, is the form that works</b> —
    /// on both versions. Named arguments are honored and may even be REVERSED
    /// (<c>@plan_id</c> before <c>@query_id</c> succeeds), so the doc's ordering warning does not describe
    /// named-argument behavior at all.</item>
    /// <item><b>Nothing protects a typo.</b> A misspelled <c>@replica_groupid</c> still reached the
    /// replica-group logic, and an entirely invented parameter name was accepted silently with no error.
    /// An operator hand-writing this call gets no feedback that it did something other than it says.</item>
    /// <item><b>SQL Server 2025 does not validate the group id.</b> <c>@replica_group_id = 99</c> — not a
    /// role at all — SUCCEEDED, and wrote a row into <c>sys.query_store_plan_forcing_locations</c> naming
    /// replica group 99 on a standalone server with one replica. SQL Server 2022 rejects the same call
    /// (12463). So on the version where the feature is GA, a wrong id fails silently instead of loudly.</item>
    /// </list>
    ///
    /// <para>That is why this text names the lookup and stops. Emitting a computed
    /// <c>@replica_group_id</c> would mean shipping a statement whose documented form errors, whose
    /// working form silently tolerates a wrong value on 2025, and whose correct value re-points across
    /// failovers (<c>sys.query_store_replicas</c> accumulates a row per (replica, role) — the docs' own
    /// remark). The operator reading the current group ids off the live server, at the moment they act,
    /// is the only reliable version of this. #1914 closed on that evidence.</para>
    /// </summary>
    private static void AppendSecondaryReplicaDisclosure(StringBuilder sb, ForcePlanTarget target)
    {
        if (!IsNonPrimaryReplicaRow(target.ReplicaRole))
            return;

        sb.AppendLine("--");
        sb.AppendLine($"-- HEADS UP: this regression was measured on the {target.ReplicaRole} replica's workload,");
        sb.AppendLine("-- but the statement below forces on the PRIMARY. sp_query_store_force_plan is executed on");
        sb.AppendLine("-- the primary in every case, and its @replica_group_id argument defaults to the primary");
        sb.AppendLine("-- when omitted -- so running this as written changes the plan the primary's WRITE workload");
        sb.AppendLine("-- gets, on the strength of what a read-only replica did. Decide that on purpose.");
        sb.AppendLine("-- To scope the force to that replica instead, read the CURRENT group ids off this server");
        sb.AppendLine("-- and add @replica_group_id as a THIRD named argument:");
        sb.AppendLine("--   SELECT replica_group_id, replica_name, role_type FROM sys.query_store_replicas;");
        sb.AppendLine($"--   EXEC sys.sp_query_store_force_plan @query_id = {target.QueryId}, @plan_id = {target.PlanId}, @replica_group_id = <id>;");
        sb.AppendLine("-- Check the id against that SELECT first. SQL Server 2025 accepts a group id that does");
        sb.AppendLine("-- not exist WITHOUT error and records the forcing against it; 2022 rejects it. Do not add");
        sb.AppendLine("-- @disable_optimized_plan_forcing to reach it -- the four-argument form the reference page");
        sb.AppendLine("-- documents fails on both versions (error 12463) unless that argument is 1.");
        sb.AppendLine("--   https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql");
        sb.AppendLine("--   https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-query-store-replicas");
        sb.AppendLine("--");
    }

    /// <summary>
    /// The #2138 gap-3 caution, emitted only for a target whose query ALSO carried the
    /// PARAMETER_SENSITIVITY detector's plan-cache signature in the same analysis window (the
    /// regressed_queries drill-down computes the flag with the detector's own thresholds, so this text
    /// can never appear without the detector's evidence). Forcing under parameter sensitivity is the
    /// one case where the recommendation itself can become the regression: the "best" and "regressed"
    /// plans may each be right for DIFFERENT parameter populations, and pinning the cheap one hands the
    /// other population the wrong plan permanently — quietly, because a forced plan no longer
    /// recompiles away. So the gentler levers are named first, and the future auto-force bot treats
    /// this flag as a hard gate: a flagged target is never auto-forced, it gets an investigate verdict.
    /// Emits NOTHING when the flag is false, which keeps the render-stability golden meaningful — the
    /// unflagged rendering is still the one it pins (the #1882 replica-disclosure discipline).
    /// </summary>
    private static void AppendParameterSensitivityCaution(StringBuilder sb, ForcePlanTarget target)
    {
        if (!target.ParameterSensitivityCoFired)
            return;

        sb.AppendLine("--");
        sb.AppendLine("-- CAUTION: this query also shows the parameter-sensitivity signature in the plan cache");
        sb.AppendLine("-- (one cached plan whose per-execution cost varies >= 10x across parameter values). The");
        sb.AppendLine("-- regressed plan and the best plan may each be right for DIFFERENT parameter values, and");
        sb.AppendLine("-- forcing pins one shape for all of them -- the population that preferred the other plan");
        sb.AppendLine("-- inherits the wrong one permanently. Before forcing, consider the gentler levers first:");
        sb.AppendLine("-- update statistics on the tables involved and watch whether the plan settles, or on");
        sb.AppendLine("-- SQL Server 2022+ evaluate PSP optimization / a Query Store hint instead of a hard force.");
        sb.AppendLine("-- If you do force, re-check the per-parameter cost spread afterwards, not just the average.");
    }

    /// <summary>
    /// THE force-plan policy gate (#2138): the named reasons this target must not be force-planned
    /// without a human. Fills <see cref="StructuredForcePlanTarget.Blockers"/> on the MCP surfaces
    /// today, and is the function the Phase 1+ auto-force bot consults before acting — one
    /// implementation, so what agents inspect is what the bot enforces. Deliberately built ONLY from
    /// fields the persisted target carries; a gate that re-derives evidence at judgment time can
    /// disagree with the evidence the finding displayed.
    /// <list type="bullet">
    /// <item><b>parameter_sensitivity_cofired</b> — the query's plan-cache history shows the PSP
    /// signature (#2140); forcing pins ONE shape for every parameter value, so the "best" plan may be
    /// the wrong plan for the population that preferred the other one.</item>
    /// <item><b>secondary_replica_evidence</b> — the regression was measured on a non-primary replica,
    /// but the statement forces on the PRIMARY (#1882's disclosure, as data): acting on it changes the
    /// primary's write workload on the strength of what a read-only replica did.</item>
    /// </list>
    /// </summary>
    public static IReadOnlyList<string> ForcePlanBlockers(ForcePlanTarget target)
    {
        if (target is null)
        {
            return Array.Empty<string>();
        }

        var blockers = new List<string>();
        if (target.ParameterSensitivityCoFired)
        {
            blockers.Add("parameter_sensitivity_cofired");
        }

        if (IsNonPrimaryReplicaRow(target.ReplicaRole))
        {
            blockers.Add("secondary_replica_evidence");
        }

        return blockers;
    }

    /// <summary>
    /// The machine-first remediation projection (#2138) — see <see cref="StructuredRemediation"/> for
    /// why it exists and why it is built at read time rather than persisted. Null when the action is
    /// null or carries no force-plan targets (other verbs can gain shapes when a consumer needs them).
    /// </summary>
    public static StructuredRemediation? BuildStructuredRemediation(RemediationAction? action)
    {
        if (action?.Targets is not { Count: > 0 } targets)
        {
            return null;
        }

        var structured = new List<StructuredForcePlanTarget>(targets.Count);
        foreach (var t in targets)
        {
            var blockers = ForcePlanBlockers(t);
            structured.Add(new StructuredForcePlanTarget(
                t.Database,
                t.QueryId,
                t.PlanId,
                t.LatestPlanHash,
                t.BestPlanHash,
                string.IsNullOrEmpty(t.ReplicaRole) ? null : t.ReplicaRole,
                Eligible: blockers.Count == 0,
                blockers,
                new StructuredForcePlanEvidence(
                    t.RegressionFactor,
                    t.LatestCpuPerExecUs,
                    t.BestCpuPerExecUs,
                    t.ParameterSensitivityCoFired),
                ForceSql: $"USE {QuoteName(t.Database)};{Environment.NewLine}" +
                    $"EXEC sys.sp_query_store_force_plan @query_id = {t.QueryId}, @plan_id = {t.PlanId};",
                UnforceSql: $"USE {QuoteName(t.Database)};{Environment.NewLine}" +
                    $"EXEC sys.sp_query_store_unforce_plan @query_id = {t.QueryId}, @plan_id = {t.PlanId};",
                VerifySql: BuildForcePlanVerifySql(t)));
        }

        return new StructuredRemediation(action.FactKey, action.Action, structured);
    }

    /// <summary>
    /// The post-force verification an agent (or the future bot's self-review window) runs: did the force
    /// STICK (<c>is_forced_plan</c>, <c>force_failure_count</c>, and the failure reason when it did not),
    /// and what has the per-interval cost looked like SINCE — the same two questions the #2141 arc's
    /// "re-check the spread, not just the average" advice asks, as runnable statements.
    /// </summary>
    private static string BuildForcePlanVerifySql(ForcePlanTarget t)
    {
        var nl = Environment.NewLine;
        return
            $"USE {QuoteName(t.Database)};{nl}" +
            $"SELECT qsp.plan_id, qsp.is_forced_plan, qsp.force_failure_count, qsp.last_force_failure_reason_desc{nl}" +
            $"FROM sys.query_store_plan AS qsp{nl}" +
            $"WHERE qsp.query_id = {t.QueryId};{nl}" +
            $"{nl}" +
            $"SELECT TOP (24) rs.plan_id, rs.runtime_stats_interval_id, rs.count_executions, rs.avg_cpu_time, rs.avg_duration, rs.max_cpu_time{nl}" +
            $"FROM sys.query_store_runtime_stats AS rs{nl}" +
            $"JOIN sys.query_store_plan AS qsp{nl}" +
            $"  ON qsp.plan_id = rs.plan_id{nl}" +
            $"WHERE qsp.query_id = {t.QueryId}{nl}" +
            $"ORDER BY rs.runtime_stats_interval_id DESC;";
    }

    /// <summary>
    /// The back-out counterpart of <see cref="AppendSecondaryReplicaDisclosure"/>, emitted for any target
    /// the server attributed to a replica at all — including the primary's own rows, because the trap it
    /// warns about is one an operator hits on a correctly-scoped primary force too.
    ///
    /// <para>The two procedures do NOT default the same way, and the asymmetry is easy to miss because
    /// the argument has the same name on both.
    /// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-force-plan-transact-sql">
    /// Force</see> says @replica_group_id "defaults to the primary replica";
    /// <see href="https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-query-store-unforce-plan-transact-sql">
    /// unforce</see> says it "defaults to the local replica where the command is being executed". So the
    /// unforce line above, which reads like the exact inverse of the force line above it, silently is not
    /// one when it is run anywhere but the primary — it would un-force a different replica's plan and
    /// leave the forced plan in place. Nothing warns the operator; this does.</para>
    /// </summary>
    private static void AppendUnforceScopeNote(StringBuilder sb, ForcePlanTarget target)
    {
        if (string.IsNullOrEmpty(target.ReplicaRole))
            return;

        sb.AppendLine("-- Run the unforce ON THE PRIMARY: unlike force (which defaults @replica_group_id to the");
        sb.AppendLine("-- primary), unforce defaults it to whichever replica you are connected to.");
    }

    /// <summary>
    /// Extracts the always-safe DB-config targets from a DB_CONFIG finding's
    /// drill-down <c>config_issues</c> array. For each row with a non-empty
    /// <c>database</c>, emits one target per safe setting currently in the wrong
    /// state, reading the STRUCTURED typed fields (<c>auto_shrink</c> bool,
    /// <c>auto_close</c> bool, <c>page_verify</c> string) — never parsing the human
    /// <c>issues</c> strings (which are display wording defined in two collectors
    /// and would drift). RCSI is NEVER emitted (destructive — excluded);
    /// recovery_model / query_store are out of scope. This is the single parse:
    /// <see cref="GenerateForDbConfig"/> renders entirely from this list and
    /// <see cref="BuildAction"/> persists it. A defensive cap of 50 targets mirrors
    /// the force-plan cap discipline.
    /// </summary>
    public static IReadOnlyList<DbConfigTarget> ExtractDbConfigTargets(AnalysisFinding finding)
    {
        var targets = new List<DbConfigTarget>();

        if (finding?.DrillDown is null ||
            !finding.DrillDown.TryGetValue("config_issues", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 50) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var database = GetString(row, "database");
            if (string.IsNullOrEmpty(database))
                continue;

            // Each setting is an independent (db, setting) target. RCSI is never
            // emitted — it is intentionally absent from DbConfigSetting.
            if (GetBool(row, "auto_shrink"))
            {
                if (targets.Count >= 50) break;
                targets.Add(new DbConfigTarget(database, DbConfigSetting.AutoShrinkOff, "ON"));
            }
            if (GetBool(row, "auto_close"))
            {
                if (targets.Count >= 50) break;
                targets.Add(new DbConfigTarget(database, DbConfigSetting.AutoCloseOff, "ON"));
            }
            var pageVerify = GetString(row, "page_verify");
            if (!string.IsNullOrEmpty(pageVerify) &&
                !string.Equals(pageVerify, "CHECKSUM", StringComparison.OrdinalIgnoreCase))
            {
                if (targets.Count >= 50) break;
                targets.Add(new DbConfigTarget(database, DbConfigSetting.PageVerifyChecksum, pageVerify));
            }
        }

        return targets;
    }

    /// <summary>
    /// Thin renderer over <see cref="ExtractDbConfigTargets"/>. Emits the exact
    /// <c>ALTER DATABASE [db] SET ...;</c> statements that will run (matching the
    /// executor and the audited generated_sql), grouped by database, with a "was X"
    /// comment per statement and an explicit note when a database ALSO has RCSI OFF
    /// (intentionally NOT auto-fixed). The bracketed identifier uses the same
    /// QUOTENAME doubling as the force-plan renderer; the displayed text is NEVER
    /// executed (the executor builds its own statement from the validated identifier
    /// + the enum literal).
    /// </summary>
    private static string? GenerateForDbConfig(AnalysisFinding finding)
    {
        var targets = ExtractDbConfigTargets(finding);
        if (targets.Count == 0)
            return null;

        // Which databases also carry RCSI OFF (so we can append the note). Read the
        // structured rcsi flag from the same drill-down; never parse issues strings.
        var rcsiOffDatabases = new HashSet<string>(StringComparer.Ordinal);
        if (finding?.DrillDown is not null &&
            finding.DrillDown.TryGetValue("config_issues", out var raw) && raw is not null)
        {
            try
            {
                var element = JsonSerializer.SerializeToElement(raw);
                if (element.ValueKind == JsonValueKind.Array)
                {
                    foreach (var row in element.EnumerateArray())
                    {
                        if (row.ValueKind != JsonValueKind.Object) continue;
                        var db = GetString(row, "database");
                        // rcsi == false means RCSI is OFF (the wrong-state we exclude).
                        if (!string.IsNullOrEmpty(db) && row.TryGetProperty("rcsi", out var r)
                            && r.ValueKind == JsonValueKind.False)
                            rcsiOffDatabases.Add(db);
                    }
                }
            }
            catch { /* note is best-effort */ }
        }

        var sb = new StringBuilder();
        string? currentDb = null;

        foreach (var target in targets)
        {
            if (!string.Equals(currentDb, target.Database, StringComparison.Ordinal))
            {
                if (currentDb is not null)
                {
                    // Close out the previous database group with its RCSI note (if any).
                    if (rcsiOffDatabases.Contains(currentDb))
                        sb.AppendLine($"-- NOTE: {QuoteName(currentDb)} also has RCSI OFF — intentionally NOT auto-fixed (test on a copy first).");
                    sb.AppendLine();
                }
                currentDb = target.Database;
                sb.AppendLine($"-- Database: {target.Database}");
            }

            sb.AppendLine($"{StatementFor(target.Setting, target.Database)}   -- was {target.CurrentValue}");
        }

        if (currentDb is not null && rcsiOffDatabases.Contains(currentDb))
            sb.AppendLine($"-- NOTE: {QuoteName(currentDb)} also has RCSI OFF — intentionally NOT auto-fixed (test on a copy first).");

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// The full <c>ALTER DATABASE [db] SET ...;</c> statement text for display
    /// rendering, built from the QUOTENAME'd identifier + a hardcoded SET-clause
    /// literal selected by the enum. The Dashboard executor builds its OWN
    /// byte-identical statement and never executes this rendered string.
    /// </summary>
    private static string StatementFor(DbConfigSetting setting, string database)
    {
        var setClause = setting switch
        {
            DbConfigSetting.AutoShrinkOff => "SET AUTO_SHRINK OFF",
            DbConfigSetting.AutoCloseOff => "SET AUTO_CLOSE OFF",
            DbConfigSetting.PageVerifyChecksum => "SET PAGE_VERIFY CHECKSUM",
            DbConfigSetting.ReadCommittedSnapshotOn => "SET READ_COMMITTED_SNAPSHOT ON",
            _ => throw new ArgumentOutOfRangeException(nameof(setting), setting, "Unknown DbConfigSetting")
        };
        return $"ALTER DATABASE {QuoteName(database)} {setClause};";
    }

    /// <summary>
    /// Extracts the percent-autogrowth file targets from a FILE_AUTOGROWTH_PERCENT
    /// finding's drill-down <c>autogrowth_percent_files</c> array (WS3). For each row with a
    /// non-empty <c>database</c> and <c>logical_file_name</c>, emits one
    /// <see cref="FileGrowthTarget"/> carrying the structured fields (<c>total_size_mb</c>,
    /// <c>growth_pct</c>) — never parsing the human <c>issue</c>/<c>alter_statement</c>
    /// strings. The recommended fixed-MB step is computed once here via
    /// <see cref="RecommendedGrowthMbFor"/> so the collector's rendered statement and the
    /// reader's rendered statement agree. A defensive cap of 50 mirrors the other extractors.
    /// </summary>
    public static IReadOnlyList<FileGrowthTarget> ExtractFileGrowthTargets(AnalysisFinding finding)
    {
        var targets = new List<FileGrowthTarget>();

        if (finding?.DrillDown is null ||
            !finding.DrillDown.TryGetValue("autogrowth_percent_files", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 50) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var database = GetString(row, "database");
            var logical = GetString(row, "logical_file_name");
            if (string.IsNullOrEmpty(database) || string.IsNullOrEmpty(logical))
                continue;

            var sizeMb = GetDouble(row, "total_size_mb");
            var growthPct = GetInt(row, "growth_pct");
            var fileType = GetString(row, "file_type");
            targets.Add(new FileGrowthTarget(
                database, logical, sizeMb, growthPct, RecommendedGrowthMbFor(fileType)));
        }

        return targets;
    }

    /// <summary>
    /// Extracts the missing-index suggestions from a MISSING_INDEX finding's drill-down
    /// <c>missing_indexes</c> array (WS4). For each row with a non-empty <c>create_statement</c> it
    /// emits one <see cref="MissingIndexTarget"/> carrying the schema-qualified <c>table</c>, the
    /// <c>impact</c> estimate, and the SQL Server-suggested <c>create_statement</c> (the copy-paste
    /// payload — never parsed or executed, only surfaced). The structured fields are exactly those
    /// the drill-down collectors emit (<c>SqlServerDrillDownCollector</c> / Lite
    /// <c>DrillDownCollector</c> "missing_indexes"). A defensive cap of 5 mirrors the drill-down's
    /// own Take(5) and the other extractors.
    /// </summary>
    public static IReadOnlyList<MissingIndexTarget> ExtractMissingIndexTargets(AnalysisFinding finding)
    {
        var targets = new List<MissingIndexTarget>();

        if (finding?.DrillDown is null ||
            !finding.DrillDown.TryGetValue("missing_indexes", out var raw) ||
            raw is null)
            return targets;

        JsonElement element;
        try
        {
            element = JsonSerializer.SerializeToElement(raw);
        }
        catch
        {
            return targets;
        }

        if (element.ValueKind != JsonValueKind.Array)
            return targets;

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 5) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            // The CREATE statement is the whole point — skip a row that has none to copy.
            var createStatement = GetString(row, "create_statement");
            if (string.IsNullOrWhiteSpace(createStatement))
                continue;

            targets.Add(new MissingIndexTarget(
                Table: GetString(row, "table"),
                Impact: GetDouble(row, "impact"),
                CreateStatement: createStatement));
        }

        return targets;
    }

    /// <summary>
    /// Thin renderer over <see cref="ExtractFileGrowthTargets"/> for the read-only surfaces
    /// (email / webhook / MCP): the exact copy-paste <c>ALTER DATABASE ... MODIFY FILE</c>
    /// statements, one per file, with a "was N% on X GB" comment. Nothing executes this — it
    /// is advisory text (there is no handler for the fact key). Null when no file applies.
    /// </summary>
    private static string? GenerateForFileAutogrowth(AnalysisFinding finding)
    {
        var targets = ExtractFileGrowthTargets(finding);
        if (targets.Count == 0)
            return null;

        var sb = new StringBuilder();
        foreach (var t in targets)
        {
            var gb = t.CurrentSizeMb / 1024.0;
            sb.AppendLine($"-- {QuoteName(t.Database)}.{QuoteName(t.LogicalFileName)}: was {t.CurrentGrowthPercent}% growth on {gb:N1} GB");
            sb.AppendLine(BuildModifyFileStatement(t.Database, t.LogicalFileName, t.RecommendedGrowthMb));
        }

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// The fixed-MB FILEGROWTH recommended for a file BY TYPE: 1024 MB for data (ROWS) files,
    /// 64 MB for LOG files. A flat, predictable step — every growth is a bounded allocation
    /// instead of an ever-larger percentage of the file. Single source of truth so the
    /// drill-down collector and the Recommendations reader render byte-identical statements.
    /// </summary>
    public static int RecommendedGrowthMbFor(string fileType) =>
        string.Equals(fileType, "LOG", StringComparison.OrdinalIgnoreCase) ? 64 : 1024;

    /// <summary>
    /// Renders one copy-paste <c>ALTER DATABASE [db] MODIFY FILE (NAME = [logical],</c>
    /// <c>FILEGROWTH = NMB);</c> statement with both identifiers QUOTENAME-bracketed. Shared by
    /// the drill-down collectors (the per-file <c>alter_statement</c>) and the reader's
    /// copy-paste rebuild so they never drift. Advisory text only — nothing executes it.
    /// </summary>
    public static string BuildModifyFileStatement(string database, string logicalFileName, int growthMb)
    {
        return $"ALTER DATABASE {QuoteName(database)} MODIFY FILE (NAME = {QuoteName(logicalFileName)}, FILEGROWTH = {growthMb}MB);";
    }

    // ── Copy-paste command rendering (shared: Darling viewer today; available to Dashboard/Lite) ──
    //
    // The Darling viewer is advise-only (Postgres read-through, no in-app remediation executor), so a
    // runnable copy-paste command IS its remediation surface: for EVERY remediable finding it must
    // hand the operator T-SQL they can run themselves. RenderCopyPasteCommand turns a PERSISTED
    // RemediationAction back into that runnable T-SQL for all seven shapes — the single source of
    // truth so no surface (Darling / Dashboard reader / email / MCP) can drift from another. The three
    // always-safe shapes render bare (percent-autogrowth MODIFY FILE, missing-index CREATE, safe
    // DB-config ALTER); the four that were previously copy-paste dead ends now render too — force-plan
    // (USE + sp_query_store_force_plan, which is database-scoped so each target carries its own USE),
    // server-config (sp_configure + RECONFIGURE), RCSI, and clear-plan. The two DESTRUCTIVE shapes
    // (RCSI, clear-plan — both IsDestructive) prepend their two-sided FactRiskDisclosure text as a
    // /* ... */ header so the copy-paste itself states the risk of changing AND of not changing.
    // Reuses BuildModifyFileStatement / BuildSpConfigureStatement / StatementFor / FactRiskDisclosure.

    /// <summary>
    /// Renders runnable, copy-paste T-SQL for a persisted <see cref="RemediationAction"/> — one command
    /// (or block) per target across all seven remediation shapes — or null when the action carries no
    /// renderable target. This is the shared source the Darling viewer's copy-paste affordance delegates
    /// to; it operates purely on the persisted typed targets (the ephemeral drill-down is gone on
    /// read-back), so it never parses preview prose. The always-safe shapes render bare; the two
    /// destructive shapes (RCSI, clear-plan) prepend the two-sided <see cref="FactRiskDisclosure"/> text
    /// as a <c>/* ... */</c> comment header. Nothing here executes — it is advisory text the operator runs.
    /// </summary>
    public static string? RenderCopyPasteCommand(RemediationAction? action)
    {
        if (action is null)
            return null;

        var nl = Environment.NewLine;

        // FILE_AUTOGROWTH_PERCENT — one MODIFY FILE per file (shared builder), one per line. Safe.
        if (action.FileGrowthTargets is { Count: > 0 } fileTargets)
        {
            var sb = new StringBuilder();
            foreach (var t in fileTargets)
            {
                if (sb.Length > 0) sb.Append(nl);
                sb.Append(BuildModifyFileStatement(t.Database, t.LogicalFileName, t.RecommendedGrowthMb));
            }
            return sb.Length == 0 ? null : sb.ToString();
        }

        // MISSING_INDEX — the SQL Server-suggested CREATE, verbatim, one per line. Copy-paste only.
        if (action.MissingIndexTargets is { Count: > 0 } indexTargets)
        {
            var sb = new StringBuilder();
            foreach (var t in indexTargets)
            {
                if (string.IsNullOrWhiteSpace(t.CreateStatement)) continue;
                if (sb.Length > 0) sb.Append(nl);
                sb.Append(t.CreateStatement);
            }
            return sb.Length == 0 ? null : sb.ToString();
        }

        // SERVER_CONFIG — one sp_configure + RECONFIGURE per setting (shared builder), incl. the
        // advise-only memory settings (the operator wants the sp_configure scaffold regardless). Safe.
        if (action.ServerConfigTargets is { Count: > 0 } serverTargets)
        {
            var blocks = new List<string>(serverTargets.Count);
            foreach (var t in serverTargets)
                blocks.Add(BuildSpConfigureStatement(t.Setting, t.RecommendedValue));
            return blocks.Count == 0 ? null : string.Join(nl + nl, blocks);
        }

        // PLAN_REGRESSION (force-plan) — sp_query_store_force_plan is DATABASE-scoped, so each target
        // is a standalone USE + EXEC block (the copy-paste runs without an ambient database). Safe.
        //
        // #1882: a target measured on a NON-PRIMARY replica gets a two-line disclosure ahead of its
        // block. This surface matters more than the preview's fuller version, not less — the preview is
        // read, this is PASTED, and the omitted @replica_group_id makes the statement primary-scoped
        // while its evidence is not. Kept to two lines because a paste target buried in prose stops
        // being a paste target; the preview carries the lookup query and the doc link. Nothing is
        // emitted when the target has no replica_role, so the byte-exact goldens over this renderer
        // still pin the shape every non-AG server gets.
        if (action.Targets is { Count: > 0 } forceTargets)
        {
            var blocks = new List<string>(forceTargets.Count);
            foreach (var t in forceTargets)
            {
                var disclosure = IsNonPrimaryReplicaRow(t.ReplicaRole)
                    ? $"-- Measured on the {t.ReplicaRole} replica; this forces on the PRIMARY (@replica_group_id" + nl +
                      "-- defaults there when omitted). Scope it with @replica_group_id to target that replica." + nl
                    : string.Empty;

                // #2138 gap 3: two lines, same discipline as the replica disclosure — this surface is
                // PASTED, so the parameter-sensitivity warning matters here at least as much as in the
                // preview, and it matters that it stays short enough to survive the paste.
                var pspCaution = t.ParameterSensitivityCoFired
                    ? "-- CAUTION: parameter-sensitive (plan-cache cost varies >= 10x across parameter values)." + nl +
                      "-- Forcing pins ONE shape for every value; consider stats updates first (see the preview)." + nl
                    : string.Empty;

                blocks.Add(
                    disclosure +
                    pspCaution +
                    $"USE {QuoteName(t.Database)};" + nl +
                    $"EXEC sys.sp_query_store_force_plan @query_id = {t.QueryId}, @plan_id = {t.PlanId};");
            }

            return blocks.Count == 0 ? null : string.Join(nl + nl, blocks);
        }

        // CLEAR_PLAN — DESTRUCTIVE. Two-sided disclosure header + a self-contained resolve-and-free
        // script (live-resolve the currently-cached plan handles for the abnormal query hashes, then
        // DBCC FREEPROCCACHE each).
        if (action.ClearPlanTargets is { Count: > 0 } clearTargets)
            return RenderClearPlanScript(action, clearTargets, nl);

        // RCSI reconstructed action (FactKey "RCSI") — DESTRUCTIVE. Carries a ReadCommittedSnapshotOn
        // DbConfigTarget (the alert-path / Dashboard-reader shape); render header + ALTER per target.
        if (string.Equals(action.FactKey, "RCSI", StringComparison.Ordinal) &&
            action.DbConfigTargets is { Count: > 0 } reconstructedRcsi)
        {
            var blocks = new List<string>(reconstructedRcsi.Count);
            foreach (var t in reconstructedRcsi)
            {
                if (t.Setting != DbConfigSetting.ReadCommittedSnapshotOn) continue;
                blocks.Add(RenderRcsiBlock(t.Database, action.RcsiFigures, nl));
            }
            return blocks.Count == 0 ? null : string.Join(nl + nl, blocks);
        }

        // DB_CONFIG — the always-safe settings (bare, no header) and/or the per-database RCSI targets
        // carried on the same action (DESTRUCTIVE, each with its own two-sided disclosure header). Both
        // can be present; render the safe block first, then one RCSI block per target.
        var configBlocks = new List<string>();
        if (action.DbConfigTargets is { Count: > 0 } dbTargets)
        {
            var safe = new StringBuilder();
            foreach (var t in dbTargets)
            {
                // RCSI is destructive — it never renders bare here; it rides the header path below.
                if (t.Setting == DbConfigSetting.ReadCommittedSnapshotOn) continue;
                if (safe.Length > 0) safe.Append(nl);
                safe.Append(StatementFor(t.Setting, t.Database));
            }
            if (safe.Length > 0) configBlocks.Add(safe.ToString());
        }
        if (action.RcsiTargets is { Count: > 0 } rcsiTargets)
        {
            foreach (var t in rcsiTargets)
                configBlocks.Add(RenderRcsiBlock(t.Database, t.Figures, nl));
        }
        return configBlocks.Count == 0 ? null : string.Join(nl + nl, configBlocks);
    }

    /// <summary>
    /// Renders one destructive RCSI enable block: the two-sided <see cref="FactRiskDisclosure"/> comment
    /// header (reconstructed as a distinct FactKey "RCSI" action so the shared disclosure renders the REAL
    /// carried figures) above the enabling <c>ALTER DATABASE … SET READ_COMMITTED_SNAPSHOT ON;</c>.
    /// </summary>
    private static string RenderRcsiBlock(string database, RcsiInactionFigures? figures, string nl)
    {
        var rcsiAction = new RemediationAction(
            "RCSI", "set", Array.Empty<ForcePlanTarget>(),
            new[] { new DbConfigTarget(database, DbConfigSetting.ReadCommittedSnapshotOn, "OFF") },
            RcsiFigures: figures);

        var sb = new StringBuilder();
        var disclosure = FactRiskDisclosure.GetForAction(rcsiAction, null);
        if (disclosure is not null)
            sb.Append(RenderRiskDisclosureComment(disclosure, nl)).Append(nl);
        sb.Append($"ALTER DATABASE {QuoteName(database)} SET READ_COMMITTED_SNAPSHOT ON;");
        return sb.ToString();
    }

    /// <summary>
    /// Renders the destructive clear-cached-plan script: the two-sided disclosure header, a per-hash
    /// comment (database + anomaly figures), a review SELECT (the currently-cached plans the free will
    /// affect — a query_hash is not unique to one query, so the operator confirms the blast radius
    /// first), then a cursor that frees each live-resolved <c>plan_handle</c> via DBCC FREEPROCCACHE.
    /// Only query hashes that look like a hex literal (<c>0x…</c>) are inlined (they were validated at
    /// build time; re-checked here because this text is runnable SQL); null when none qualify.
    /// </summary>
    private static string? RenderClearPlanScript(RemediationAction action, IReadOnlyList<ClearPlanTarget> targets, string nl)
    {
        var hashes = new List<string>(targets.Count);
        var lines = new List<string>(targets.Count);
        foreach (var t in targets)
        {
            if (!IsHexLiteral(t.QueryHash)) continue;
            hashes.Add(t.QueryHash);
            var dbLabel = string.IsNullOrEmpty(t.Database) ? "(resolved live)" : QuoteName(t.Database);
            lines.Add(t.AnomalyRatio > 0
                ? $"--   {dbLabel} {t.QueryHash} (~{t.AnomalyRatio.ToString("0.0", CultureInfo.InvariantCulture)}x normal per-exec CPU: " +
                  $"{t.CurrentCpuPerExecMs.ToString("0.0", CultureInfo.InvariantCulture)} ms vs baseline " +
                  $"{t.BaselineCpuPerExecMs.ToString("0.0", CultureInfo.InvariantCulture)} ms)"
                : $"--   {dbLabel} {t.QueryHash}");
        }

        if (hashes.Count == 0)
            return null;

        var inList = string.Join(", ", hashes);
        var sb = new StringBuilder();

        var disclosure = FactRiskDisclosure.GetForAction(action, null);
        if (disclosure is not null)
            sb.Append(RenderRiskDisclosureComment(disclosure, nl)).Append(nl);

        sb.Append("-- Clear the currently-cached plan(s) for these abnormal-CPU query hashes:").Append(nl);
        foreach (var line in lines)
            sb.Append(line).Append(nl);
        sb.Append("-- STEP 1 (review): run this SELECT first — a query_hash can map to more than one query,").Append(nl);
        sb.Append("-- so confirm the databases/queries below are the ones you intend to clear.").Append(nl);
        sb.Append("SELECT DISTINCT").Append(nl);
        sb.Append("    DB_NAME(st.dbid) AS database_name,").Append(nl);
        sb.Append("    qs.plan_handle,").Append(nl);
        sb.Append("    qs.execution_count,").Append(nl);
        sb.Append("    st.text AS batch_text").Append(nl);
        sb.Append("FROM sys.dm_exec_query_stats AS qs").Append(nl);
        sb.Append("CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st").Append(nl);
        sb.Append($"WHERE qs.query_hash IN ({inList});").Append(nl);
        sb.Append(nl);
        sb.Append("-- STEP 2 (apply): free every currently-cached plan for those hashes. There is NO un-clear —").Append(nl);
        sb.Append("-- the prior plans are gone and the recompile is not guaranteed to be better.").Append(nl);
        sb.Append("DECLARE @plan_handle VARBINARY(64);").Append(nl);
        sb.Append("DECLARE plan_cursor CURSOR LOCAL FAST_FORWARD FOR").Append(nl);
        sb.Append("    SELECT DISTINCT qs.plan_handle").Append(nl);
        sb.Append("    FROM sys.dm_exec_query_stats AS qs").Append(nl);
        sb.Append($"    WHERE qs.query_hash IN ({inList});").Append(nl);
        sb.Append("OPEN plan_cursor;").Append(nl);
        sb.Append("FETCH NEXT FROM plan_cursor INTO @plan_handle;").Append(nl);
        sb.Append("WHILE @@FETCH_STATUS = 0").Append(nl);
        sb.Append("BEGIN").Append(nl);
        sb.Append("    DBCC FREEPROCCACHE(@plan_handle);").Append(nl);
        sb.Append("    FETCH NEXT FROM plan_cursor INTO @plan_handle;").Append(nl);
        sb.Append("END;").Append(nl);
        sb.Append("CLOSE plan_cursor;").Append(nl);
        sb.Append("DEALLOCATE plan_cursor;");
        return sb.ToString();
    }

    /// <summary>
    /// Renders a two-sided <see cref="RiskDisclosure"/> as a <c>/* … */</c> T-SQL comment header, so a
    /// destructive copy-paste command states the risk of changing AND of not changing inline. The prose
    /// is fixed/reviewed (only validated identifiers + numeric figures are substituted); any embedded
    /// <c>*/</c> is defused so a pathological identifier cannot close the comment early.
    /// </summary>
    private static string RenderRiskDisclosureComment(RiskDisclosure disclosure, string nl)
    {
        var sb = new StringBuilder();
        sb.Append("/*").Append(nl);
        sb.Append(" * REVIEW BEFORE RUNNING - this is a destructive change with trade-offs BOTH ways.").Append(nl);
        sb.Append(" *").Append(nl);
        sb.Append(" * Risks of MAKING this change:").Append(nl);
        foreach (var r in disclosure.RisksOfChanging)
            sb.Append(" *   - ").Append(SanitizeForComment(r.Text)).Append(nl);
        sb.Append(" *").Append(nl);
        sb.Append(" * Risks of NOT making this change:").Append(nl);
        foreach (var r in disclosure.RisksOfNotChanging)
            sb.Append(" *   - ").Append(SanitizeForComment(r.Text)).Append(nl);
        sb.Append(" */");
        return sb.ToString();
    }

    /// <summary>Defuses an embedded C-style comment terminator so disclosure prose cannot close the header early.</summary>
    private static string SanitizeForComment(string text) =>
        string.IsNullOrEmpty(text) ? string.Empty : text.Replace("*/", "* /");

    /// <summary>True when the value is a non-empty hex literal (<c>0x</c> + at least one hex digit) safe to inline into SQL.</summary>
    private static bool IsHexLiteral(string? value)
    {
        if (value is null || value.Length <= 2 ||
            !(value[0] == '0' && (value[1] == 'x' || value[1] == 'X')))
            return false;
        for (var i = 2; i < value.Length; i++)
        {
            var c = value[i];
            var isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!isHex) return false;
        }
        return true;
    }

    /// <summary>
    /// QUOTENAME-equivalent: wrap an identifier in square brackets and double
    /// any embedded close-bracket. The database name comes from
    /// sys.databases (via the drill-down collector), so it is already a valid
    /// SQL identifier — this guards against pathologically bracketed names
    /// without trusting that guarantee.
    /// </summary>
    private static string QuoteName(string identifier)
    {
        return "[" + identifier.Replace("]", "]]") + "]";
    }

    private static string GetString(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return string.Empty;
        return v.ValueKind == JsonValueKind.String ? (v.GetString() ?? string.Empty) : string.Empty;
    }

    private static long GetInt64(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return 0;
        return v.ValueKind switch
        {
            JsonValueKind.Number when v.TryGetInt64(out var i) => i,
            JsonValueKind.Number => (long)v.GetDouble(),
            _ => 0
        };
    }

    private static double GetDouble(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return 0.0;
        return v.ValueKind == JsonValueKind.Number ? v.GetDouble() : 0.0;
    }

    private static int GetInt(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return 0;
        return v.ValueKind switch
        {
            JsonValueKind.Number when v.TryGetInt32(out var i) => i,
            JsonValueKind.Number => (int)v.GetDouble(),
            _ => 0
        };
    }

    private static int? GetNullableInt(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return null;
        return v.ValueKind switch
        {
            JsonValueKind.Number when v.TryGetInt32(out var i) => i,
            JsonValueKind.Number => (int)v.GetDouble(),
            _ => null
        };
    }

    private static bool GetBool(JsonElement row, string property)
    {
        if (!row.TryGetProperty(property, out var v)) return false;
        return v.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            // Defensive: a collector that emitted a "1"/"0" string or a number still
            // reads correctly, but both collectors emit a JSON bool (§4.1 parity).
            JsonValueKind.String => string.Equals(v.GetString(), "1", StringComparison.Ordinal)
                                     || string.Equals(v.GetString(), "true", StringComparison.OrdinalIgnoreCase),
            JsonValueKind.Number => v.TryGetInt64(out var n) && n != 0,
            _ => false
        };
    }
}
