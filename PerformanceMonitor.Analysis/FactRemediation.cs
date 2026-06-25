using System;
using System.Collections.Generic;
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
    /// Extracts the typed force-plan targets from a PLAN_REGRESSION finding's
    /// drill-down. This is the single parse: the preview renderer
    /// (<see cref="GenerateForPlanRegression"/>) renders entirely from this list,
    /// and <see cref="BuildAction"/> persists it. Applies the same guards as the
    /// renderer always has — database non-empty, query_id &gt; 0, best_plan_id
    /// &gt; 0 — and the same cap of 5 targets. Reads every value the preview
    /// renders (including the two cpu/exec numbers) so the renderer needs no
    /// second drill-down read.
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

        foreach (var row in element.EnumerateArray())
        {
            if (targets.Count >= 5) break;
            if (row.ValueKind != JsonValueKind.Object) continue;

            var database = GetString(row, "database");
            var queryId = GetInt64(row, "query_id");
            var bestPlanId = GetInt64(row, "best_plan_id");
            if (string.IsNullOrEmpty(database) || queryId <= 0 || bestPlanId <= 0)
                continue;

            targets.Add(new ForcePlanTarget(
                Database: database,
                QueryId: queryId,
                PlanId: bestPlanId,
                BestPlanHash: GetString(row, "best_plan_hash"),
                LatestPlanHash: GetString(row, "latest_plan_hash"),
                LatestCpuPerExecUs: GetDouble(row, "latest_cpu_per_exec_us"),
                BestCpuPerExecUs: GetDouble(row, "best_cpu_per_exec_us"),
                RegressionFactor: GetDouble(row, "regression_factor")));
        }

        return targets;
    }

    /// <summary>
    /// Thin renderer over <see cref="ExtractPlanRegressionTargets"/>. The output
    /// is byte-for-byte the same preview the inline parse produced before the
    /// extract-once refactor (guarded by the render-stability golden test),
    /// including the two "(cpu/exec ... us)" comment lines.
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
            sb.AppendLine($"USE {QuoteName(target.Database)};");
            sb.AppendLine($"EXEC sys.sp_query_store_force_plan @query_id = {target.QueryId}, @plan_id = {target.PlanId};");
            sb.AppendLine();
            sb.AppendLine($"-- To back out:");
            sb.AppendLine($"-- EXEC sys.sp_query_store_unforce_plan @query_id = {target.QueryId}, @plan_id = {target.PlanId};");

            emitted++;
        }

        return sb.ToString().TrimEnd();
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
