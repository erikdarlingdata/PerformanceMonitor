using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// A single block of operator-facing advice for a fact-key.
/// <para>
/// <see cref="Headline"/> is the one-line summary used as a section heading.
/// <see cref="Investigation"/> tells the operator where to look first.
/// <see cref="Remediation"/> tells them what to consider doing.
/// <see cref="RemediationTsql"/> is populated by <see cref="FactRemediation"/>
/// when a finding's drill-down carries the IDs needed to generate a
/// copy-paste-ready statement.
/// </para>
/// </summary>
public sealed record AdviceBlock(
    string Headline,
    string Investigation,
    string Remediation,
    string? RemediationTsql = null,
    RiskDisclosure? Risks = null);

/// <summary>
/// Static lookup mapping fact-keys (the same constants emitted by
/// <see cref="FactScorer"/>) to operator-facing advice prose. Both Lite and
/// Dashboard render the same content from this table so the user reads the
/// same diagnosis regardless of which app surfaced the finding.
///
/// <para>
/// Dynamic keys (BAD_ACTOR_{hash}, ANOMALY_WAIT_{type}) are resolved by
/// prefix in <see cref="GetForFactKey"/>. The wait-anomaly composer reuses
/// the inner wait's prose with an anomaly-framing prelude.
/// </para>
/// </summary>
public static class FactAdvice
{
    /// <summary>
    /// Looks up advice for a fact-key. Returns null if the key is unknown.
    /// </summary>
    public static AdviceBlock? GetForFactKey(string? factKey)
    {
        if (string.IsNullOrEmpty(factKey))
            return null;

        if (_byKey.TryGetValue(factKey, out var direct))
            return direct;

        if (factKey.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase))
            return _byKey.GetValueOrDefault("BAD_ACTOR");

        if (factKey.StartsWith("ANOMALY_WAIT_", StringComparison.OrdinalIgnoreCase))
        {
            var inner = factKey.Substring("ANOMALY_WAIT_".Length);
            if (string.IsNullOrEmpty(inner))
                return null;
            return ComposeAnomalyWaitAdvice(inner);
        }

        return null;
    }

    /// <summary>
    /// Convenience: looks up the advice for a finding's root fact and composes
    /// it with any generated remediation T-SQL. Returns null if no advice
    /// matches the root fact key.
    /// </summary>
    public static AdviceBlock? GetForFinding(AnalysisFinding finding)
    {
        if (finding is null)
            return null;

        // Value-stated prose first (frozen StoryText), static fallback for legacy findings; then
        // overlay the generated copy-paste T-SQL and the two-sided risk disclosure as before.
        var advice = GetComposedForFinding(finding);
        if (advice is null)
            return null;

        var tsql = FactRemediation.GenerateForFinding(finding);
        if (tsql is not null)
            advice = advice with { RemediationTsql = tsql };

        // B3 Phase 3 (§6) + Clear-cached-plan (§5): when the finding offers a DESTRUCTIVE
        // remediation, append the two-sided RiskDisclosure so every read-only surface
        // (email / webhook / MCP) SHOWS the operator the same risks they'd see in-app —
        // they simply cannot click Apply off-app (no consent gate there; consent is
        // enforced only by the dialog). A finding carries exactly one root fact key, so at
        // most ONE of these parallel builders returns non-null: a DB_CONFIG finding may
        // offer RCSI; a CPU finding (CPU_SQL_PERCENT / CPU_SPIKE) may offer CLEAR_PLAN.
        var destructiveAction = FactRemediation.BuildRcsiAction(finding)
                                ?? FactRemediation.BuildClearPlanAction(finding);
        if (destructiveAction is not null)
        {
            var risks = FactRiskDisclosure.GetForAction(destructiveAction, finding);
            if (risks is not null)
                advice = advice with { Risks = risks };
        }

        return advice;
    }

    /// <summary>
    /// Render-time entry point for a persisted or live finding. Prefers the value-stated advice
    /// FROZEN into <see cref="AnalysisFinding.StoryText"/> at analysis time (see
    /// <see cref="PopulateStoryText"/>), and falls back to the static <see cref="GetForFactKey"/>
    /// block for findings persisted before this field carried advice. Every read surface (Lite and
    /// Dashboard recommendation cards, MCP get_analysis_findings, analyze_server) calls this so the
    /// operator reads the SAME numbers the engine observed — current MAXDOP, CTFP, cores — instead
    /// of generic folklore. The composer ran where the facts live; the card just displays the result.
    /// </summary>
    public static AdviceBlock? GetComposedForFinding(AnalysisFinding finding)
    {
        if (finding is null)
            return null;
        return TryReadStoryText(finding.StoryText) ?? GetForFactKey(finding.RootFactKey);
    }

    /// <summary>
    /// Composes value-stated advice for a story's root fact key from the FULL scored fact set.
    /// The full set matters: a healthy setting (e.g. MAXDOP already at 8) scores 0 and is therefore
    /// ABSENT from the engine's >0 working set, so the composer must see every fact to state the
    /// current value. Value-bearing keys (THREADPOOL_PARALLEL / _MIXED, CONFIG_MAXDOP, CONFIG_CTFP)
    /// interpolate the server's actual settings and tailor the recommendation to how far they are
    /// from guidance — including the "already within guidance and still exhausting" override. Every
    /// other key returns its static block unchanged.
    /// </summary>
    public static AdviceBlock? Compose(string? rootFactKey, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (string.IsNullOrEmpty(rootFactKey))
            return null;
        return rootFactKey switch
        {
            "THREADPOOL_PARALLEL" => ComposeThreadpoolParallel(factsByKey),
            "THREADPOOL_MIXED" => ComposeThreadpoolMixed(factsByKey),
            "CONFIG_MAXDOP" => ComposeConfigMaxdop(factsByKey),
            "CONFIG_CTFP" => ComposeConfigCtfp(factsByKey),
            // Parallelism value-gap blocks: state the server's actual MAXDOP/CTFP instead of the
            // generic "raise CTFP to 50, cap MAXDOP" guidance (or, for CXPACKET, an audit_config
            // deferral). All three reuse the same current-values prefix.
            "THREADPOOL" => ComposeWithMaxdopCtfpPrefix("THREADPOOL", factsByKey),
            "CXPACKET" => ComposeWithMaxdopCtfpPrefix("CXPACKET", factsByKey),
            "QUERY_HIGH_DOP" => ComposeWithMaxdopCtfpPrefix("QUERY_HIGH_DOP", factsByKey),
            // Memory value blocks: state the server's actual max server memory + physical RAM
            // instead of deferring to audit_config (B2). The two config-rooted blocks state the
            // configured values; the wait/anomaly blocks append the current cap.
            "CONFIG_MAX_MEMORY_MB" => ComposeConfigMaxMemory(factsByKey),
            "CONFIG_MIN_MAX_MEMORY_NARROW" => ComposeConfigMinMaxNarrow(factsByKey),
            "PAGEIOLATCH_SH" => ComposeWithMaxMemorySuffix("PAGEIOLATCH_SH", factsByKey),
            "QUERY_SPILLS" => ComposeWithMaxMemorySuffix("QUERY_SPILLS", factsByKey),
            "ANOMALY_MEMORY_PRESSURE" => ComposeWithMaxMemorySuffix("ANOMALY_MEMORY_PRESSURE", factsByKey),
            // RCSI-deferral blocks: state how many databases actually have RCSI off (from the
            // co-fired DB_CONFIG fact) instead of deferring to audit_config (B3).
            "BLOCKING_EVENTS" => ComposeWithRcsiSuffix("BLOCKING_EVENTS", factsByKey),
            "DEADLOCKS" => ComposeWithRcsiSuffix("DEADLOCKS", factsByKey),
            _ => GetForFactKey(rootFactKey)
        };
    }

    /// <summary>
    /// Freezes each story's value-stated advice into <see cref="AnalysisStory.StoryText"/> as a
    /// compact JSON {h,i,r} blob, BEFORE the finding stores copy StoryText onto the persisted
    /// finding. The full <paramref name="facts"/> list (every severity) backs the value reads.
    /// Read-back surfaces deserialize it via <see cref="GetComposedForFinding"/>; the static blocks
    /// remain the fallback. No schema change — StoryText already round-trips in both stores and was
    /// previously written empty.
    /// </summary>
    public static void PopulateStoryText(IEnumerable<AnalysisStory> stories, IReadOnlyList<Fact> facts)
    {
        if (stories is null)
            return;
        var byKey = (facts ?? Array.Empty<Fact>()).ToFactLookup();
        foreach (var story in stories)
        {
            if (story is null || story.IsAbsolution)
                continue;
            var advice = Compose(story.RootFactKey, byKey);
            if (advice is not null)
                story.StoryText = SerializeForStoryText(advice);
        }
    }

    /// <summary>Serializes an advice block's prose to the compact {h,i,r} JSON stored in StoryText.</summary>
    public static string SerializeForStoryText(AdviceBlock? advice) =>
        advice is null
            ? string.Empty
            : JsonSerializer.Serialize(new StoryAdvice(advice.Headline, advice.Investigation, advice.Remediation));

    /// <summary>
    /// Reads back the frozen advice from a finding's StoryText. Returns null for empty or legacy
    /// (non-JSON) StoryText so the caller falls back to the static block, and is defensive against
    /// malformed JSON.
    /// </summary>
    public static AdviceBlock? TryReadStoryText(string? storyText)
    {
        if (string.IsNullOrEmpty(storyText) || storyText[0] != '{')
            return null;
        try
        {
            var s = JsonSerializer.Deserialize<StoryAdvice>(storyText);
            if (s is null || (string.IsNullOrEmpty(s.h) && string.IsNullOrEmpty(s.i) && string.IsNullOrEmpty(s.r)))
                return null;
            return new AdviceBlock(s.h ?? string.Empty, s.i ?? string.Empty, s.r ?? string.Empty);
        }
        catch
        {
            return null;
        }
    }

    private sealed record StoryAdvice(string? h, string? i, string? r);

    // ── Value readers: the fact's RAW collected value (the setting/metric), not its severity ──

    /// <summary>The fact's raw collected value rounded to a whole number, or null when the fact is absent.</summary>
    private static long? FactValue(IReadOnlyDictionary<string, Fact> facts, string key) =>
        facts.TryGetValue(key, out var f) ? (long)Math.Round(f.Value) : (long?)null;

    /// <summary>
    /// Cores-per-socket from SERVER_HARDWARE metadata — the per-NUMA-node proxy MAXDOP guidance keys
    /// on (NUMA node count itself is not collected). 0 when absent.
    /// </summary>
    private static int CoresPerSocket(IReadOnlyDictionary<string, Fact> facts) =>
        facts.TryGetValue("SERVER_HARDWARE", out var hw)
            && hw.Metadata.TryGetValue("cores_per_socket", out var c) ? (int)c : 0;

    /// <summary>
    /// The collection-gap caveat appended to every THREADPOOL-family block: under live thread
    /// exhaustion the collector is itself a query waiting for a worker, so a gap in Collection Health
    /// around the window corroborates the event rather than being a separate problem.
    /// </summary>
    private const string CollectionGapNote =
        " Note on the data: under live thread exhaustion the collector is itself a query waiting for " +
        "a worker, so expect a gap in collection during the worst of it — a missing interval in " +
        "Collection Health around this window corroborates the event, it is not a separate problem.";

    /// <summary>
    /// THREADPOOL_PARALLEL composed with the server's ACTUAL MAXDOP, CTFP, and cores-per-socket, so
    /// the card states the numbers and tailors the guard to how far they are from guidance —
    /// including the workload-aware override when both are already within guidance and the pool still
    /// exhausted (the cause is the volume of concurrent parallel queries, so guard harder). Falls
    /// back to the static block when neither config fact was collected this window.
    /// </summary>
    private static AdviceBlock ComposeThreadpoolParallel(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["THREADPOOL_PARALLEL"];
        var maxdop = FactValue(facts, "CONFIG_MAXDOP");
        var ctfp = FactValue(facts, "CONFIG_CTFP");
        if (maxdop is null && ctfp is null)
            return fallback;

        var cores = CoresPerSocket(facts);
        var rec = FactRemediation.RecommendedMaxdop(cores);
        return fallback with { Remediation = ParallelGuardCore(maxdop, ctfp, cores, rec) + CollectionGapNote };
    }

    /// <summary>
    /// THREADPOOL_MIXED composed: collapse the blocking first (workers parked on locks are not
    /// running, so it is the faster win), THEN the value-stated parallelism guard. Falls back to the
    /// static block when neither config fact was collected this window.
    /// </summary>
    private static AdviceBlock ComposeThreadpoolMixed(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["THREADPOOL_MIXED"];
        var maxdop = FactValue(facts, "CONFIG_MAXDOP");
        var ctfp = FactValue(facts, "CONFIG_CTFP");
        if (maxdop is null && ctfp is null)
            return fallback;

        var cores = CoresPerSocket(facts);
        var rec = FactRemediation.RecommendedMaxdop(cores);
        var remediation =
            "Collapse the blocking first — workers parked on locks are not running, so it is the " +
            "faster win: if the chain was headed by a sleeping/abandoned transaction, fix the code " +
            "path that leaves a BEGIN TRAN open (and SET XACT_ABORT ON so an aborted batch rolls " +
            "back); otherwise fix the slow operation under the held lock (usually a missing index " +
            "turning a seek into a scan). Then guard parallelism. " +
            ParallelGuardCore(maxdop, ctfp, cores, rec) + CollectionGapNote;
        return fallback with { Remediation = remediation };
    }

    /// <summary>
    /// The value-stated parallelism-guard recommendation shared by THREADPOOL_PARALLEL and
    /// THREADPOOL_MIXED. States the server's current MAXDOP and CTFP (no "if") and recommends the
    /// specific change: raise CTFP toward 50 and bring MAXDOP to the per-NUMA-node proxy (≤ 8) when
    /// either is loose; or, when both are already within guidance and the pool still exhausted,
    /// guard harder for the concurrency level. Does NOT include the collection-gap note (the caller
    /// appends it once).
    /// </summary>
    private static string ParallelGuardCore(long? maxdop, long? ctfp, int cores, long rec)
    {
        var sb = new StringBuilder();

        // 1. State what was observed — no conditional about settings the engine collected.
        sb.Append("This server's MAXDOP is ")
          .Append(maxdop?.ToString() ?? "not readable this window")
          .Append(" and cost threshold for parallelism is ")
          .Append(ctfp?.ToString() ?? "not readable this window")
          .Append(cores > 0 ? $" (cores per socket {cores})." : ".");

        var ctfpGuarded = ctfp is >= 50;
        var maxdopGuarded = maxdop is > 0 && maxdop <= rec;

        if (ctfpGuarded && maxdopGuarded)
        {
            // Workload-aware override: settings are sane, so the driver is the VOLUME of concurrent
            // parallel queries — guard harder and go after the specific offenders.
            var harder = Math.Max(2, rec / 2);
            sb.Append(" Both are already within topology guidance, so the exhaustion is the volume of ")
              .Append("concurrent parallel queries, not loose settings: guard harder for this concurrency ")
              .Append($"level by lowering MAXDOP from {maxdop} to {harder} and/or raising cost threshold ")
              .Append($"for parallelism above {ctfp}, and go after the specific high-DOP offenders");
        }
        else
        {
            sb.Append(" Guard parallelism:");
            if (ctfp is null || ctfp <= 5)
                sb.Append(" raise cost threshold for parallelism to 50 so trivial queries stop going parallel");
            else if (ctfp < 50)
                sb.Append($" raise cost threshold for parallelism from {ctfp} toward 50");
            else
                sb.Append(" cost threshold for parallelism is already past the trivial-query cutoff");

            if (maxdop is 0)
                sb.Append($", and cap MAXDOP at {rec} (this server's per-NUMA-node processor count, capped at 8) instead of unlimited");
            else if (maxdop > rec)
                sb.Append($", and lower MAXDOP from {maxdop} to {rec} (the per-NUMA-node processor count, capped at 8)");
            else
                sb.Append($"; MAXDOP at {maxdop} is already within the ≤ {rec} guidance");
            sb.Append(". Then go after the specific high-DOP offenders");
        }

        sb.Append(" — `get_top_queries_by_cpu` with `parallel_only=true` ranks them; the usual shapes ")
          .Append("are a parallel scan of a large table or a skewed plan where one branch does all the work.");

        // 2. The one factor not collected (workload type) + the don't-mask rule.
        sb.Append(" Thread exhaustion means concurrency is already high enough to drain the pool, so ")
          .Append("guarding is warranted here; if these are deliberately large reporting/DW queries ")
          .Append("rather than OLTP, prefer limiting how many run at once over clamping MAXDOP (which ")
          .Append("slows each one). Do NOT raise `max worker threads` to mask it — that trades thread ")
          .Append("exhaustion for memory pressure without fixing the cause.");

        return sb.ToString();
    }

    /// <summary>
    /// CONFIG_MAXDOP composed with the ACTUAL value, so the headline states the real number (the
    /// static block hard-coded "MAXDOP is 0", wrong whenever the finding fired for 1 or an
    /// above-guidance value) and the remediation is tailored to 0 / 1 / above-guidance. Falls back
    /// to the static block when the fact was not collected this window.
    /// </summary>
    private static AdviceBlock ComposeConfigMaxdop(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CONFIG_MAXDOP"];
        var maxdop = FactValue(facts, "CONFIG_MAXDOP");
        if (maxdop is null)
            return fallback;

        var cores = CoresPerSocket(facts);
        var rec = FactRemediation.RecommendedMaxdop(cores);
        var coresNote = cores > 0 ? $" (cores per socket {cores})" : string.Empty;

        string headline, remediation;
        if (maxdop == 0)
        {
            headline = "MAXDOP is 0 — a single query can fan out across every scheduler (up to 64)";
            remediation =
                $"Set MAXDOP to {rec} — this server's cores-per-socket capped at 8{coresNote}, the per-NUMA-node " +
                "proxy; the SKU is irrelevant to the right value. The Apply button runs sp_configure + " +
                "RECONFIGURE, an online metadata change. On hardware with more than 16 logical processors " +
                "per NUMA node you can raise it by hand. Raise Cost Threshold for Parallelism in the same pass " +
                "if its companion finding fired.";
        }
        else if (maxdop == 1)
        {
            headline = "MAXDOP is 1 — every query is forced to run single-threaded";
            remediation =
                $"MAXDOP 1 forces every query serial: large analytical queries, index rebuilds, and DBCC run " +
                $"far slower. Unless this was set deliberately to fix a specific parallelism problem, set MAXDOP " +
                $"to {rec} (cores-per-socket capped at 8{coresNote}) via sp_configure + RECONFIGURE, an online change.";
        }
        else
        {
            headline = $"MAXDOP is {maxdop} — above this server's topology-based guidance of {rec}";
            remediation =
                $"Lower MAXDOP from {maxdop} to {rec} (cores-per-socket capped at 8{coresNote}, the per-NUMA-node " +
                "proxy; the SKU is irrelevant). The Apply button runs sp_configure + RECONFIGURE, an online " +
                "metadata change. On hardware with more than 16 logical processors per NUMA node a higher value " +
                "can be justified by hand. Pair it with a sane Cost Threshold for Parallelism if that finding fired.";
        }

        return fallback with { Headline = headline, Remediation = remediation };
    }

    /// <summary>
    /// CONFIG_CTFP composed with the ACTUAL value so the headline and remediation state the current
    /// number. Falls back to the static block when the fact was not collected this window.
    /// </summary>
    private static AdviceBlock ComposeConfigCtfp(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CONFIG_CTFP"];
        var ctfp = FactValue(facts, "CONFIG_CTFP");
        if (ctfp is null)
            return fallback;

        string headline = ctfp <= 5
            ? $"Cost Threshold for Parallelism is {ctfp} — at (or below) the 1990s default"
            : $"Cost Threshold for Parallelism is {ctfp}";
        string remediation = ctfp <= 5
            ? $"CTFP is {ctfp}, the shipped default — on modern hardware it sends trivial queries parallel, paying " +
              "thread-coordination overhead (CXPACKET) for no gain. Raise it to 50 as a starting point, then tune " +
              "up if CXPACKET persists on genuinely large queries. The Apply button runs sp_configure + RECONFIGURE, " +
              "an online metadata change. Pair it with a sane MAXDOP if that companion finding fired."
            : $"CTFP is {ctfp}. Raise it toward 50 so only genuinely expensive queries go parallel, then tune up if " +
              "CXPACKET persists on large queries. The Apply button runs sp_configure + RECONFIGURE, an online change.";

        return fallback with { Headline = headline, Remediation = remediation };
    }

    /// <summary>
    /// A current-MAXDOP/CTFP sentence for the parallelism wait blocks (CXPACKET, QUERY_HIGH_DOP,
    /// generic THREADPOOL) — states the server's actual values and names the specific change where
    /// either is off topology guidance, so the card stops saying a generic "raise CTFP to 50, cap
    /// MAXDOP" (or deferring to audit_config). Empty when neither config fact was collected.
    /// </summary>
    private static string MaxdopCtfpClause(IReadOnlyDictionary<string, Fact> facts)
    {
        var maxdop = FactValue(facts, "CONFIG_MAXDOP");
        var ctfp = FactValue(facts, "CONFIG_CTFP");
        if (maxdop is null && ctfp is null)
            return string.Empty;

        var cores = CoresPerSocket(facts);
        var rec = FactRemediation.RecommendedMaxdop(cores);
        var sb = new StringBuilder("This server's MAXDOP is ")
            .Append(maxdop?.ToString() ?? "not readable this window")
            .Append(" and cost threshold for parallelism is ")
            .Append(ctfp?.ToString() ?? "not readable this window")
            .Append(". ");

        var recs = new List<string>();
        if (ctfp is not null && ctfp <= 5)
            recs.Add("raise cost threshold for parallelism to 50");
        else if (ctfp is not null && ctfp < 50)
            recs.Add($"raise cost threshold for parallelism from {ctfp} toward 50");
        if (maxdop is 0)
            recs.Add($"cap MAXDOP at {rec} (the per-NUMA-node processor count, ≤ 8)");
        else if (maxdop is not null && maxdop > rec)
            recs.Add($"lower MAXDOP from {maxdop} to {rec}");

        if (recs.Count > 0)
            sb.Append("Bring them to guidance: ").Append(string.Join(", ", recs)).Append(". ");
        else if (maxdop is not null && ctfp is not null)
            sb.Append("Both are within topology guidance, so the parallelism is coming from the plans themselves (below). ");

        return sb.ToString();
    }

    /// <summary>
    /// Returns the static block for <paramref name="key"/> with the current-MAXDOP/CTFP clause
    /// prepended to its remediation (CXPACKET / QUERY_HIGH_DOP / generic THREADPOOL). Falls back to
    /// the static block unchanged when the config facts were not collected this window.
    /// </summary>
    private static AdviceBlock ComposeWithMaxdopCtfpPrefix(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey[key];
        var clause = MaxdopCtfpClause(facts);
        return clause.Length == 0
            ? fallback
            : fallback with { Remediation = clause + fallback.Remediation };
    }

    /// <summary>A fact's metadata value, or null when the fact or the metadata key is absent.</summary>
    private static double? FactMeta(IReadOnlyDictionary<string, Fact> facts, string key, string metaKey) =>
        facts.TryGetValue(key, out var f) && f.Metadata.TryGetValue(metaKey, out var v) ? v : (double?)null;

    /// <summary>
    /// A standalone sentence stating the server's actual max server memory and physical RAM, for the
    /// memory-pressure wait/anomaly blocks (PAGEIOLATCH_SH, QUERY_SPILLS, ANOMALY_MEMORY_PRESSURE)
    /// that otherwise deferred the value to audit_config. Empty when the cap was not collected.
    /// </summary>
    private static string MaxMemorySentence(IReadOnlyDictionary<string, Fact> facts)
    {
        var cap = FactValue(facts, "CONFIG_MAX_MEMORY_MB");
        if (cap is null)
            return string.Empty;
        var total = FactValue(facts, "MEMORY_TOTAL_PHYSICAL_MB");
        var sb = new StringBuilder(cap.Value >= 2147483647L
            ? "This server's max server memory is at its unlimited default"
            : $"This server's max server memory is set to {cap.Value:N0} MB");
        if (total is > 0)
            sb.Append($" of {total.Value:N0} MB physical RAM");
        sb.Append('.');
        return sb.ToString();
    }

    /// <summary>
    /// Returns the static block for <paramref name="key"/> with the current max-server-memory sentence
    /// appended to its remediation. Falls back to the static block when the cap was not collected.
    /// </summary>
    private static AdviceBlock ComposeWithMaxMemorySuffix(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey[key];
        var sentence = MaxMemorySentence(facts);
        return sentence.Length == 0
            ? fallback
            : fallback with { Remediation = fallback.Remediation + " " + sentence };
    }

    /// <summary>
    /// Returns the static block for <paramref name="key"/> (BLOCKING_EVENTS / DEADLOCKS) with a
    /// sentence stating how many databases actually have RCSI off — read from the co-fired DB_CONFIG
    /// fact's `rcsi_off_count` — appended to its remediation. Falls back to the static block when
    /// DB_CONFIG did not co-fire or no database has RCSI off (nothing concrete to state).
    /// </summary>
    private static AdviceBlock ComposeWithRcsiSuffix(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey[key];
        var rcsiOff = FactMeta(facts, "DB_CONFIG", "rcsi_off_count");
        if (rcsiOff is not > 0)
            return fallback;

        var n = (long)rcsiOff.Value;
        var sentence = n == 1
            ? "One database on this server currently has RCSI off — enable it there if its blocking is readers-vs-writers."
            : $"{n:N0} databases on this server currently have RCSI off — enable it on the ones whose blocking is readers-vs-writers.";
        return fallback with { Remediation = fallback.Remediation + " " + sentence };
    }

    /// <summary>
    /// CONFIG_MAX_MEMORY_MB composed with the server's physical RAM, so the card states a concrete
    /// suggested cap (total − max(4 GB, 10%)) instead of a formula and an audit_config deferral. The
    /// fact only fires when max server memory is at its ~2 PB default. Falls back to the static block
    /// when total RAM was not collected (nothing concrete to compute).
    /// </summary>
    private static AdviceBlock ComposeConfigMaxMemory(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CONFIG_MAX_MEMORY_MB"];
        var total = FactValue(facts, "MEMORY_TOTAL_PHYSICAL_MB");
        if (total is not > 0)
            return fallback;

        var osReserve = Math.Max(4096, (long)(total.Value * 0.10));
        var suggested = total.Value - osReserve;
        return fallback with
        {
            Investigation =
                $"`max server memory (MB)` is at its ~2 PB default, so SQL Server will grow its buffer pool until the OS is under memory pressure — which can page out SQL's own working set and destabilize the whole host (and any other instances). This server has {total.Value:N0} MB of physical RAM.",
            Remediation =
                $"Cap max server memory below total RAM, leaving headroom for the OS and the SQL Server thread stacks — a sensible starting point here is ~{suggested:N0} MB ({total.Value:N0} MB total minus {osReserve:N0} MB for the OS), then adjust for anything else on the box. This is intentionally NOT auto-applied — the correct value is workload- and host-specific. Run `sp_configure 'max server memory (MB)', {suggested}` + RECONFIGURE once you've settled on a number."
        };
    }

    /// <summary>
    /// CONFIG_MIN_MAX_MEMORY_NARROW composed with the configured min and max (from the fact's
    /// metadata) so the card states the actual figures instead of deferring to audit_config.
    /// </summary>
    private static AdviceBlock ComposeConfigMinMaxNarrow(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CONFIG_MIN_MAX_MEMORY_NARROW"];
        var min = FactMeta(facts, "CONFIG_MIN_MAX_MEMORY_NARROW", "min_memory_mb");
        var max = FactMeta(facts, "CONFIG_MIN_MAX_MEMORY_NARROW", "max_memory_mb");
        if (min is null || max is null)
            return fallback;

        return fallback with
        {
            Investigation =
                $"`min server memory (MB)` is set to {min.Value:N0} MB, within ~20% of `max server memory (MB)` at {max.Value:N0} MB. min server memory is a floor SQL will not release BELOW once reached — pinning it near max means SQL effectively never gives memory back to the OS, which starves other processes and defeats the OS's ability to reclaim under pressure."
        };
    }

    /// <summary>
    /// Wraps the inner wait-type's advice with an anomaly-framing prelude so
    /// an ANOMALY_WAIT_THREADPOOL finding reads as "this wait is anomalously
    /// elevated vs. baseline" rather than "this wait crossed a static
    /// threshold". Falls back to the bare anomaly framing if the inner wait
    /// has no first-class advice.
    /// </summary>
    private static AdviceBlock ComposeAnomalyWaitAdvice(string innerWaitType)
    {
        const string prelude =
            "This wait is anomalously elevated compared to its own baseline for this " +
            "time bucket — it is a deviation from normal, not necessarily a sustained " +
            "problem. Check whether the elevation coincides with a workload change, a " +
            "deploy, or an external event before treating it as a chronic issue.";

        if (_byKey.TryGetValue(innerWaitType, out var inner))
        {
            return new AdviceBlock(
                Headline: $"Anomalous spike in {innerWaitType} vs. baseline",
                Investigation: prelude + " " + inner.Investigation,
                Remediation: inner.Remediation);
        }

        return new AdviceBlock(
            Headline: $"Anomalous spike in {innerWaitType} vs. baseline",
            Investigation:
                prelude + " Open the Wait Stats tab and zoom to the analysis window to see " +
                "the wait type's series against the rest of the breakdown. Call `get_wait_trend` " +
                "with this wait type for the longer-window trajectory, and `compare_analysis` " +
                "to see what other facts changed between the comparison window and the baseline.",
            Remediation:
                "Resolve the underlying wait the way you would normally — the anomaly framing " +
                "only tells you the wait is unusual for this bucket, not what to do about it. " +
                "If the wait persists across multiple analysis windows, the threshold-based " +
                "finding for that wait type will fire and the standard playbook applies.");
    }

    private static readonly Dictionary<string, AdviceBlock> _byKey = BuildAdviceTable();

    private static Dictionary<string, AdviceBlock> BuildAdviceTable()
    {
        var t = new Dictionary<string, AdviceBlock>(StringComparer.OrdinalIgnoreCase);

        // ─────────────────────────────────────────────────────────────────
        // CPU pressure
        // ─────────────────────────────────────────────────────────────────

        t["SOS_SCHEDULER_YIELD"] = new AdviceBlock(
            Headline:
                "Scheduler yields are dominating waits — workers keep exhausting their CPU quantum and yielding, the signature of sustained CPU-bound work",
            Investigation:
                "SOS_SCHEDULER_YIELD is logged when a worker voluntarily yields after using its full 4 ms scheduling quantum (cooperative scheduling), so a high count means workers are running CPU-bound for long stretches — most often large in-memory scans chewing through pages that are already in the buffer pool. The amount alone does not settle whether the box is short of CPU: a high SOS total can be a few queries each using a whole core productively. What separates that from genuine oversubscription is the RUNNABLE QUEUE — tasks that are ready to run but waiting their turn on a scheduler. Check `get_cpu_scheduler_pressure` (Dashboard) for the runnable-task queue depth (runnable tasks per scheduler) and the signal-wait time: a deep runnable queue alongside the SOS means demand IS exceeding CPU capacity; a shallow or empty queue means the CPUs are simply busy doing real work. (Worked example: 10 minutes of SOS over a 5-minute window on 2 schedulers is 100% of CPU-time in yields — but if nothing is queuing it's just two queries each owning a core; only long runnable lines make it 'demand exceeds capacity'.) The drill-down `top_cpu_queries` is attached: those five account for the CPU being burned. `get_cpu_utilization` shows the per-minute trend, `get_top_queries_by_cpu` the cached-plan view of the offenders.",
            Remediation:
                "Tune the queries in `top_cpu_queries` first — the wait points at queries burning CPU (typically scans that should be seeks), and cutting that work is almost always cheaper than adding cores, whether or not the box is oversubscribed. Fix the missing indexes, scans, and parameter sniffing the plan analysis surfaces. If the runnable queue is genuinely deep (see Investigation) and tuning cannot bring CPU demand back under capacity, the server is legitimately CPU-bound for its workload and more cores are justified. If CXPACKET is co-elevated, trivial queries are likely going parallel: raise `cost threshold for parallelism` to 50 and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8).");

        // THREADPOOL (thread exhaustion) is attribution-keyed by InferenceEngine.ClassifyThreadpool:
        // the finding roots on THREADPOOL_PARALLEL / THREADPOOL_BLOCKING / THREADPOOL_MIXED when the
        // co-cause is clear, else the generic THREADPOOL below. Only the parallel flavor is a
        // MAXDOP/CTFP problem; blocking-driven exhaustion is fixed by clearing the blocking.
        t["THREADPOOL"] = new AdviceBlock(
            Headline:
                "THREADPOOL waits — SQL Server has run out of worker threads, with no clear parallel or blocking cause this window",
            Investigation:
                "Sustained THREADPOOL (it cleared the wait-time + per-wait-average gate, so this isn't pool grow/shrink noise) means new sessions cannot start until a worker frees up. Neither CXPACKET/high-DOP (parallelism) nor blocking co-fired above their own thresholds this window, so the engine could not attribute it — which happens when a co-cause stayed just under threshold or, commonly, the collector could not sample the peak. Decide it by hand: if CXPACKET / high-DOP are present treat it as parallelism (below); if a blocking chain is present treat it as blocking. `get_waiting_tasks` (Lite) / `get_cpu_scheduler_pressure` (Dashboard) show what is queued.",
            Remediation:
                "Resolve the dominant cause — parallelism (raise `cost threshold for parallelism` to 50, cap `MAXDOP` at the logical processors in a single NUMA node, ≤ 8) or blocking. This is a report on a window that has already passed, so the worker pool has long since recovered: aim at stopping the recurrence, not a one-off `KILL`. For the blocking case that means fixing the abandoned-transaction code path (a BEGIN TRAN left open on an error/timeout path; SET XACT_ABORT ON) or the slow operation under the held lock (usually a missing index turning a seek into a scan). Do NOT raise `max worker threads` to mask it — that trades thread exhaustion for memory pressure (each worker reserves a thread stack outside max server memory) without fixing the cause. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_PARALLEL"] = new AdviceBlock(
            Headline:
                "Thread exhaustion driven by parallelism — too many parallel queries each reserving worker threads",
            Investigation:
                "THREADPOOL fired alongside CXPACKET and/or high-DOP queries: a parallel query reserves up to DOP workers per parallel branch, so a moderate-concurrency workload at high DOP can drain the pool with zero blocking. This is the MAXDOP/CTFP flavor of thread exhaustion. `get_top_queries_by_cpu` with `parallel_only=true` ranks the parallel offenders; check whether they genuinely benefit from the degree of parallelism they are getting.",
            Remediation:
                "Guard parallelism: raise `cost threshold for parallelism` to 50 so trivial queries stop going parallel, and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). When both are already at guidance and the pool still exhausts, the driver is the volume of concurrent parallel queries — lower MAXDOP further and/or raise CTFP for that concurrency level, and tune the specific high-DOP queries (a parallel scan of a large table, or a skewed plan where one branch does all the work). Do NOT raise `max worker threads` to mask it (trades thread exhaustion for memory pressure). Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_BLOCKING"] = new AdviceBlock(
            Headline:
                "Thread exhaustion driven by blocking — workers are pinned on blocked (not running) sessions",
            Investigation:
                "THREADPOOL fired alongside blocking: every session waiting on a lock keeps its worker thread assigned, so a wide or deep blocking chain ties up one worker per blocked session and can exhaust the pool. Lowering MAXDOP frees nothing here — the workers are not running parallel work, they are parked on locks. The BLOCKING_CHAIN drill-down `reconstructed_blocking_chains` shows `apex_spid`, `apex_sleeping`, `depth`, and `victim_count`; `get_blocked_process_reports` (Lite) / `get_blocking` (Dashboard) show the live chain.",
            Remediation:
                "Clearing the blocking is what frees the workers — but this is a report on a window that has already passed, so the pile-up is gone and a `KILL` now buys nothing. Aim at the recurrence. If the chain was headed by a sleeping apex (`apex_sleeping = true`), that is the abandoned-transaction signature: fix the code path that opens a BEGIN TRAN and never reaches COMMIT/ROLLBACK on the error or client-timeout path, and SET XACT_ABORT ON so an aborted batch rolls back automatically. If the apex was active, there is one slow operation everyone queued behind — fix it (a missing index turning a seek into a scan under a held lock is the usual shape), and enable RCSI where the contention is readers blocked by writers. MAXDOP/CTFP are not the levers for this. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_MIXED"] = new AdviceBlock(
            Headline:
                "Thread exhaustion with both parallelism and blocking elevated",
            Investigation:
                "THREADPOOL fired with BOTH CXPACKET/high-DOP and blocking co-elevated, so workers are being consumed from two directions: parallel queries reserving DOP workers, and blocked sessions parking workers on locks. Look at the BLOCKING_CHAIN drill-down `reconstructed_blocking_chains` (apex, depth, victim_count) and the parallel offenders (`get_top_queries_by_cpu` with `parallel_only=true`) together.",
            Remediation:
                "Collapse the blocking first — it pins workers on sessions that are not even running, so it is the bigger win. This is a report on a past window, so go after the recurrence rather than a one-off kill: fix the abandoned-transaction code path (BEGIN TRAN left open on an error/timeout path; SET XACT_ABORT ON) or the slow operation under the held lock. Then guard parallelism: raise `cost threshold for parallelism` to 50 and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). Do NOT raise `max worker threads` to mask either cause. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["CXPACKET"] = new AdviceBlock(
            Headline:
                "Parallelism waits — queries are spending real time waiting for their parallel workers to synchronize",
            Investigation:
                "The collector groups every CX* wait (CXPACKET, CXCONSUMER, CXSYNC_PORT, CXSYNC_CONSUMER) into one CXPACKET fact (`DuckDbFactCollector.GroupParallelismWaits` in Lite, `SqlServerFactCollector.GroupParallelismWaits` in Dashboard). That grouping hides producer/consumer skew — CXCONSUMER on its own specifically means one branch is doing the work while siblings stall waiting for rows. If the per-execution duration of the offending queries is closer to their CPU time than CPU÷DOP would predict, parallelism is mostly contending with itself, not paying for itself. Open the Wait Stats tab to see the breakdown over the analysis window (the server's current CTFP and MAXDOP are stated in the remediation below). The QUERY_HIGH_DOP amplifier flags queries running at DOP > 8 — `get_top_queries_by_cpu` with `parallel_only=true` ranks them.",
            Remediation:
                "Most OLTP queries should not go parallel — raise `cost threshold for parallelism` to 50 and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). When that's already done and CXPACKET persists, the problem is the underlying plan: a parallel scan of a large table, a hash join with bad row estimates, or a skewed plan where one branch does everything. Pull the plan via `analyze_query_plan` for the offending `query_hash` and look for missing indexes or operator-level row-count divergence — fix the plan and the wait disappears.");

        t["CPU_SQL_PERCENT"] = new AdviceBlock(
            Headline:
                "SQL Server process CPU is sustained above the threshold — the workload is eating real CPU, not waiting on something else",
            Investigation:
                "Open the CPU tab to see the SQL vs. other-process split over the analysis window — that confirms SQL is the consumer rather than antivirus, a runaway agent job, or another tenant on the VM. The drill-down `top_cpu_queries` is already attached: five queries ranked by total CPU over the window, with `query_text`, `execution_count`, `max_dop`, and `spills`. SOS_SCHEDULER_YIELD co-elevation on the Wait Stats tab means schedulers are saturated; a CPU_SPIKE corroborator means the load is bursty rather than steady. Call `get_cpu_utilization` for the per-minute trend and `get_top_queries_by_cpu` for the live cached-plan view of the same queries.",
            Remediation:
                "Tune the queries in `top_cpu_queries` — that's almost always cheaper than buying cores. The PARAMETER_SENSITIVITY and PLAN_REGRESSION findings (if they fired alongside this one) point at specific root causes: a plan that was good for one parameter value being reused for a bad one, or a plan that's worse than what the same query used to run. For PLAN_REGRESSION specifically, the generated EXEC `sp_query_store_force_plan` in the remediation block forces the historical-better plan as a fast fix while you address why the worse plan was chosen.");

        t["CPU_SPIKE"] = new AdviceBlock(
            Headline:
                "CPU spike detected — peak usage is well above the period average, not sustained saturation",
            Investigation:
                "Spikes differ from steady saturation: the box has headroom most of the time but something briefly burns it all. The drill-down `spike_peak` carries the exact peak time and CPU %, and `queries_at_spike` lists the five sessions active within ±2 minutes of that peak — `session_id`, `database`, `cpu_time_ms`, `dop`, `wait_type`, and `query_text`. Open the CPU tab and zoom to that timestamp, then cross-check the Wait Stats tab for what waits dominated in the same window. Co-elevated PLAN_REGRESSION, PARAMETER_SENSITIVITY, or CXPACKET tells you which of the three usual suspects you're dealing with.",
            Remediation:
                "If PLAN_REGRESSION co-fired, the historical-better plan is identified by `best_plan_id` in `regressed_queries` and the `sp_query_store_force_plan` EXEC is generated in the remediation block below. If PARAMETER_SENSITIVITY co-fired, do NOT force a plan — that locks in the wrong plan for the other parameter values that the engine already detected diverging. Use `OPTION (RECOMPILE)` on the affected statement, or branch the procedure by parameter value. For ad-hoc reporting spikes that don't match either pattern, Resource Governor or moving the report off-peak is the durable fix.");

        // ─────────────────────────────────────────────────────────────────
        // Memory pressure
        // ─────────────────────────────────────────────────────────────────

        t["PAGEIOLATCH_SH"] = new AdviceBlock(
            Headline:
                "PAGEIOLATCH_SH waits — SQL is reading data pages from disk into the buffer pool faster than it can serve them",
            Investigation:
                "The classic buffer-pool-too-small or scan-too-large signal — SQL is evicting pages it still needs and reading them straight back from disk. Open File I/O → File I/O Latency to separate workload-driven pressure from underlying storage: high PAGEIOLATCH_SH with sub-20ms read latency means the workload is reading too much, not that disk is slow. The PAGEIOLATCH amplifiers (in `FactScorer.PageiolatchAmplifiers`) boost severity when IO_READ_LATENCY_MS ≥ 20 and when memory grant waiters are present. Call `get_memory_stats` for the buffer-pool size, `get_memory_grants` if grants are competing with the pool, and `get_top_queries_by_cpu` to find the readers — high `logical_reads` per execution is the marker.",
            Remediation:
                "Find the scan and add a covering nonclustered index — one missing index can drop gigabytes of reads per execution and the wait collapses with it. The `plan_analysis` drill-down on BAD_ACTOR findings surfaces the missing-index suggestions the optimizer already generated. If indexing is genuinely right and the wait persists, the buffer pool is too small for the working set — raise `max server memory` if it is capped below the working set (leaving headroom for the OS), or add RAM.");

        t["PAGEIOLATCH_EX"] = new AdviceBlock(
            Headline:
                "PAGEIOLATCH_EX waits — SQL is waiting to write data pages, usually under heavy modification workload",
            Investigation:
                "Shows up under heavy bulk inserts, large updates, or tempdb workspace writes. Open File I/O → File I/O Latency for the write column to separate workload-driven pressure from underlying disk pressure; the drill-down `file_latency_breakdown` already attached carries `avg_write_latency_ms` per database and file type, ranked. If TEMPDB_USAGE co-fired, the writes are workspace (hash/sort spills) — the QUERY_SPILLS drill-down `top_spilling_queries` names the offenders by `query_hash` and `total_spills`. Open the TempDB Stats sub-tab under Resource Metrics to see whether the version store or internal objects drove the growth.",
            Remediation:
                "For tempdb-driven PAGEIOLATCH_EX, fix the queries in `top_spilling_queries` — update statistics with FULLSCAN to correct the cardinality estimates that produced too-small grants, or rewrite the operator that spills. For modification-workload pressure, batch large operations into smaller chunks so the buffer pool can flush between batches. If `file_latency_breakdown` shows the storage itself is slow (write latency consistently above 10ms on data, 2ms on log), no amount of query tuning will recover it — the storage is the bottleneck and needs hardware-side investigation.");

        t["RESOURCE_SEMAPHORE"] = new AdviceBlock(
            Headline:
                "RESOURCE_SEMAPHORE waits — queries are queueing for memory grants because the workspace pool is exhausted",
            Investigation:
                "Memory grants are reserved up front for sorts, hashes, and parallel operators. When the workspace pool fills, large queries queue behind it and small ones can starve. Open the Memory Grants sub-tab under Memory to see grant pressure over the analysis window, and the Memory Pressure Events sub-tab for the corresponding ring-buffer notifications. The MEMORY_GRANT_PENDING fact carries the live waiter count; if it co-fired the drill-down `pending_grants` is attached with `waiter_count`, `granted_memory_mb`, and `used_memory_mb` for each snapshot. Call `get_memory_grants` for the per-pool semaphore state and `get_resource_semaphore` (Dashboard) for the breakdown of granted vs. available workspace.",
            Remediation:
                "The usual cause is over-granting: a query gets a grant much larger than it actually uses because cardinality estimation was wrong. Look at the `top_cpu_queries` drill-down for the offenders, then `analyze_query_plan` for each `query_hash` to see the operator-level row-count estimates vs. actuals. Update statistics with FULLSCAN on the affected tables, or add filtered indexes if a subset of the data drives the bad estimate. Per-query stopgaps: `OPTION (MAX_GRANT_PERCENT = X)` caps a single offender's grant without affecting others. Resource Governor workload-group caps are the durable answer if one workload chronically starves the others.");

        t["RESOURCE_SEMAPHORE_QUERY_COMPILE"] = new AdviceBlock(
            Headline:
                "Query compile gateway waits — too many concurrent compilations of expensive queries",
            Investigation:
                "SQL Server gates expensive plan compilations through a semaphore (the big-plan gateway). When too many big plans need compiling at once, sessions wait at the gateway instead of running. The cause is almost always poor parameterisation: ad-hoc queries with literal values, or RPC calls without proper parameter typing, each producing a unique plan that has to be compiled from scratch. Open Memory → Plan Cache to see the single-use plan ratio; `get_plan_cache_bloat` (Dashboard) gives the same view as a numeric breakdown. `get_top_queries_by_cpu` will show the compile-heavy queries — high `execution_count` against high per-compile cost is the signature.",
            Remediation:
                "Fix the app to parameterise its calls — that's the only durable cure. As a server-side stopgap, `ALTER DATABASE ... SET PARAMETERIZATION FORCED` makes the optimizer parameterise more aggressively, but test it on a workload copy first because it changes plan-selection behaviour in ways that can hurt other queries. Enabling `optimize for ad hoc workloads` reduces plan-cache bloat from one-off queries but does nothing for the compile cost of any individual large plan. Plan guides on the worst-offender patterns are a per-query escape hatch.");

        t["MEMORY_GRANT_PENDING"] = new AdviceBlock(
            Headline:
                "Queries are queued waiting for memory grants — the workspace pool is full",
            Investigation:
                "The drill-down `pending_grants` is already attached to this finding: each row shows `waiter_count`, `granted_memory_mb`, `used_memory_mb`, `timeout_errors`, and `forced_grants` for the snapshots where waiters were present. One sustained waiter is concerning, five is critical, forced grants or timeouts are emergency. Open the Memory Grants sub-tab under Memory for the trend over the window. RESOURCE_SEMAPHORE co-elevation confirms pool exhaustion is the cause; QUERY_SPILLS co-elevation means queries are running with grants smaller than they needed and spilling to tempdb. Call `get_memory_grants` for the per-pool semaphore state.",
            Remediation:
                "Same playbook as RESOURCE_SEMAPHORE: the offenders are over-granting because of bad cardinality estimates. Look at `top_cpu_queries` (attached when CPU is co-elevated) for the heavy hitters, then `analyze_query_plan` for the specific `query_hash` to see where the estimate diverged from actuals. Update statistics with FULLSCAN, add the right indexes, and the grants shrink. `OPTION (MAX_GRANT_PERCENT = X)` is a per-query stopgap that caps the worst offender without affecting others.");

        // ─────────────────────────────────────────────────────────────────
        // Blocking / lock contention
        // ─────────────────────────────────────────────────────────────────

        t["BLOCKING_EVENTS"] = new AdviceBlock(
            Headline:
                "Blocked process reports are firing above the per-hour threshold — sessions are waiting on locks held by other sessions",
            Investigation:
                "Open Blocking → Blocked Process Reports and zoom to the analysis window — that's the same source the engine just analyzed, with the XML available per row. The drill-down `top_blocking_chains` is already attached: five entries ranked by `wait_time_ms`, each carrying `database`, `blocked_spid`, `blocking_spid`, `lock_mode`, and truncated SQL for both sides. Call `get_blocked_process_reports` for the full parsed reports (Lite) or `get_blocking` (Dashboard), and `get_blocking_trend` (Lite) or `get_blocking_deadlock_stats` (Dashboard) to see whether the rate is climbing across the window or a single spike.",
            Remediation:
                "Look at the locks involved in `top_blocking_chains`: shared/exclusive mismatch (LCK_M_S blocked by writers) is RCSI territory — enable READ_COMMITTED_SNAPSHOT on those databases (`ALTER DATABASE [db] SET READ_COMMITTED_SNAPSHOT ON`) so readers read row versions instead of taking shared locks. Writer/writer contention (X/U/IX modes) is not helped by RCSI; the answer is shorter transactions, indexes that let the writer find rows faster instead of locking a range while scanning, and consistent object access order between procedures. For chronic blocking driven by one slow operation, fix that one query — the lock-hold duration is usually proportional to query duration.");

        t["DEADLOCKS"] = new AdviceBlock(
            Headline:
                "Deadlocks are firing above the per-hour threshold — at least one transaction is being killed every few minutes",
            Investigation:
                "Open Blocking → Deadlocks and zoom to the window — every row carries the deadlock graph XML, viewable in-app. The drill-down `top_deadlocks` is already attached with the three most recent victims by collection time, including truncated victim SQL. Call `get_deadlocks` for the parsed event stream and `get_deadlock_detail` for the full graph XML on a specific event. SQL Server's deadlock detector runs every 5 seconds, so any sustained rate above the threshold means active recurring contention — not transient. If the graph shows LCK_M_S victims (readers being killed for writers), READ_COMMITTED_SNAPSHOT on that database eliminates the class — readers read row versions instead of taking the shared locks that deadlock with writers.",
            Remediation:
                "Most deadlocks are caused by inconsistent object access order between two procedures (proc A locks table X then Y; proc B locks Y then X). Read the graph, identify the two paths, and pick one access order for both. For reader/writer deadlocks specifically, enabling READ_COMMITTED_SNAPSHOT on the database eliminates the entire class — readers stop taking shared locks and read row versions instead. Raising deadlock priority on the loser does not prevent the deadlock; it only changes which side dies. Shortening transactions (moving non-transactional work outside `BEGIN TRAN`) shrinks the window in which the deadlock can occur.");

        t["BLOCKING_CHAIN"] = new AdviceBlock(
            Headline:
                "Multi-level blocking chain reconstructed — a session is blocking blockers, fanning out into a pile-up",
            Investigation:
                "The drill-down `reconstructed_blocking_chains` is already attached: up to three chains, each carrying `apex_spid`, `apex_sleeping`, `depth`, `victim_count`, `max_wait_ms`, and a per-level breakdown with `blocking_spid`, `blocked_spid`, `lock_mode`, `wait_time_ms`, and the SQL on both sides. `apex_sleeping = true` is the abandoned-transaction signature — an application started a BEGIN TRAN and never reached COMMIT/ROLLBACK on the error path. THREADPOOL co-elevation means every blocked victim is also holding a worker thread, and the server is at risk of thread exhaustion. Open Blocking → Blocked Process Reports to walk the same chain in the UI; `get_blocked_process_reports` (Lite) or `get_blocked_process_xml` (Dashboard) returns the parsed event stream.",
            Remediation:
                "This is a report on a window that has already passed — the pile-up has cleared and a `KILL` now buys nothing, so aim at the recurrence. If `apex_sleeping = true` on the top chain, that is the abandoned-transaction signature: an application opened a BEGIN TRAN and never reached COMMIT/ROLLBACK on its error or client-timeout path. Fix that code path, and SET XACT_ABORT ON so an aborted batch rolls back automatically — killing the session only clears one occurrence. For an active apex, look at the level-0 entry: there is one slow operation everyone else is queued behind. Common shapes: a missing index forcing a scan under a held lock, an UPDATE whose WHERE clause has no supporting index so it locks rows while it scans, or an unindexed foreign key that makes a parent-side UPDATE/DELETE scan the child table to enforce referential integrity. Fix that one operation and the chain dissolves.");

        t["LCK"] = new AdviceBlock(
            Headline:
                "General lock contention is significant in wait stats — writer/writer or update/exclusive lock conflicts dominating",
            Investigation:
                "LCK groups X (exclusive), U (update), IX (intent exclusive), SIX, and BU waits — the writer/writer side of lock contention. Unlike LCK_M_S/LCK_M_IS (readers blocked by writers, fixable with RCSI), these are real serialization points: two writers genuinely cannot proceed at the same time. The drill-down `lock_mode_breakdown` is already attached: every LCK% wait type ranked by `total_wait_ms` and `waiting_tasks` over the analysis window — that tells you exactly which lock modes are dominating. Open the Wait Stats tab to see the LCK family on the chart. `get_waiting_tasks` (Lite) or `get_blocking` (Dashboard) returns the live waiting list with lock details.",
            Remediation:
                "RCSI does NOT help writer/writer contention — it only addresses readers blocked by writers. The fixes are mechanical: shorten transactions so locks are held briefly, fix the queries causing long lock holds (a missing index turning a seek into a scan extends every lock the scan takes), and partition hot tables so independent parts of the workload do not collide on the same rows. An UPDATE without a useful WHERE-clause index is by itself frequently the entire problem — `analyze_query_plan` on the offender will show the scan and the missing-index suggestion.");

        t["LCK_M_S"] = new AdviceBlock(
            Headline:
                "LCK_M_S waits — readers are being blocked by writers, classic shared/exclusive lock contention",
            Investigation:
                "SELECT queries are queueing behind UPDATE/INSERT/DELETE transactions on the same rows or pages. The drill-down `lock_mode_breakdown` shows LCK_M_S ranked against the other lock modes for the window; `config_issues` (attached when DB_CONFIG co-fired) lists the databases where RCSI is off — those are the candidates for the fix. Open Configuration → Database Configuration in-app to see the full per-database RCSI/auto-shrink/auto-close/page-verify state. Reader/writer deadlocks are the same pattern escalated — if DEADLOCKS co-fired, the graph will show LCK_M_S victims.",
            Remediation:
                "Enable READ_COMMITTED_SNAPSHOT on the affected database: `ALTER DATABASE <db> SET READ_COMMITTED_SNAPSHOT ON;`. Readers stop taking shared locks and instead read the previous-committed row version, which eliminates the entire wait class for everything running at READ COMMITTED or below. The ALTER needs a brief exclusive lock on the database, so do it at a quiet moment. If the application uses NOLOCK / READUNCOMMITTED hints to work around this same problem today, RCSI is strictly better — it returns committed data instead of dirty reads — but test on a copy first if any code relies on dirty-read behaviour.");

        t["LCK_M_IS"] = new AdviceBlock(
            Headline:
                "LCK_M_IS waits — intent-shared locks are being blocked, usually shared locks waiting for exclusive operations to finish",
            Investigation:
                "Intent-shared locks are the page/table-level locks SELECT takes to declare 'I will be reading rows on this page'. Waiting on IS means the page or table is held in an incompatible mode by a writer — same reader-blocked-by-writer pattern as LCK_M_S, one level up the lock hierarchy. The drill-down `lock_mode_breakdown` shows LCK_M_IS ranked against the other lock modes for the window. `config_issues` (when DB_CONFIG co-fired) lists databases with RCSI off — those are the candidates. Open Configuration → Database Configuration to see the full per-database state.",
            Remediation:
                "Enable READ_COMMITTED_SNAPSHOT on the affected database: `ALTER DATABASE <db> SET READ_COMMITTED_SNAPSHOT ON;`. This eliminates reader/writer blocking for everything at or below READ COMMITTED — the most common isolation level by far. If the application currently uses READ UNCOMMITTED with NOLOCK hints to avoid these waits, RCSI is the strictly better mechanism: it returns committed data rather than dirty reads, and you can remove the hints once it's enabled.");

        t["SCH_M"] = new AdviceBlock(
            Headline:
                "Schema modification lock waits — DDL or index operations are blocking everything else on the affected object",
            Investigation:
                "SCH-M is the most exclusive lock SQL Server takes — it's incompatible with everything, including the IS lock SELECT requires. A query holding SCH-M on a hot table blocks the entire workload against that table. Sources: `ALTER TABLE`, `CREATE/DROP INDEX`, partition operations, and certain statistics updates. Open the Running Jobs tab to see whether a scheduled maintenance job is running during the wrong window — if RUNNING_JOBS also fired, the drill-down will name the job. Call `get_waiting_tasks` for the live SCH-M waiters (Lite) and `get_blocking` (Dashboard) to confirm the DDL session is the apex.",
            Remediation:
                "Move the DDL operation to an actual maintenance window — that's the cure for the scheduled-job case. For ongoing index maintenance on hot tables, `ONLINE = ON` rebuilds take SCH-M only briefly at the start and end of the operation instead of holding it for the whole rebuild; on Enterprise Edition, `WAIT_AT_LOW_PRIORITY` lets the rebuild yield to user queries when there's contention. Statistics updates with `WITH FULLSCAN` on huge tables can also drive SCH-M — schedule them, or use sampled updates instead.");

        t["LCK_RANGE"] = new AdviceBlock(
            Headline:
                "Range-lock waits — SERIALIZABLE isolation is escalating to row-range locks, blocking other readers and writers",
            Investigation:
                "Range locks (LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_*) appear only under SERIALIZABLE — they prevent phantom reads by locking ranges of an index rather than just the rows being read. (REPEATABLE READ holds its row/page locks to end-of-transaction but does NOT take key-range locks, which is exactly why phantoms remain possible under it.) Either the application is explicitly requesting SERIALIZABLE, or a `BEGIN TRANSACTION` somewhere is defaulting to it. The most common silent source is .NET's `System.Transactions.TransactionScope`, which defaults to SERIALIZABLE if you do not pass `TransactionOptions`. The drill-down `lock_mode_breakdown` shows which range lock types are dominating; `get_waiting_tasks` (Lite) or `get_blocking` (Dashboard) returns the live waiters with their session program names — the apex is often a specific application name.",
            Remediation:
                "Fix the source of the SERIALIZABLE request. For .NET callers, pass `new TransactionOptions { IsolationLevel = IsolationLevel.ReadCommitted }` to `TransactionScope` — this is the single most common fix. For explicit `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE` in T-SQL, ask whether phantom prevention is genuinely required; in most cases it isn't. RCSI does NOT help here — RCSI is a READ COMMITTED feature; SERIALIZABLE callers would need SNAPSHOT isolation enabled separately and opt into it explicitly. If SERIALIZABLE is truly required, shorter transactions and tighter range predicates are the only knobs that reduce the lock footprint.");

        // The range-lock family all share one advice block — every LCK_M_R* key
        // in FactScorer.GetWaitThresholds maps to the same LCK_RANGE entry.
        var rangeLock = t["LCK_RANGE"];
        t["LCK_M_RS_S"]  = rangeLock;
        t["LCK_M_RS_U"]  = rangeLock;
        t["LCK_M_RIn_NL"] = rangeLock;
        t["LCK_M_RIn_S"] = rangeLock;
        t["LCK_M_RIn_U"] = rangeLock;
        t["LCK_M_RIn_X"] = rangeLock;
        t["LCK_M_RX_S"]  = rangeLock;
        t["LCK_M_RX_U"]  = rangeLock;
        t["LCK_M_RX_X"]  = rangeLock;

        // ─────────────────────────────────────────────────────────────────
        // I/O
        // ─────────────────────────────────────────────────────────────────

        t["IO_READ_LATENCY_MS"] = new AdviceBlock(
            Headline:
                "Average read latency is above the storage-health threshold — disk reads are slow",
            Investigation:
                "The drill-down `file_latency_breakdown` is already attached: per-database, per-file-type breakdown of `avg_read_latency_ms` and `avg_write_latency_ms` over the window, ranked. That tells you which files are slow — data, log, or tempdb. Above 20ms is concerning, 50ms is critical for OLTP storage. Open File I/O → File I/O Latency in-app to see the same data graphed over time. PAGEIOLATCH_SH co-elevation means the workload is read-heavy enough to expose the latency; PAGEIOLATCH_SH near zero with high latency means storage is slow but the workload isn't hitting it hard. Call `get_file_io_stats` for the latest snapshot and `get_file_io_trend` for the trajectory.",
            Remediation:
                "Check the workload first — a missing index forcing scans drives enormous read I/O that exposes any storage as slow. Look at `top_cpu_queries` ordered by `logical_reads` per execution, run `analyze_query_plan` on the worst, and fix the indexes the plan analysis recommends. The latency often disappears without any storage change. If indexing is right and latency persists, the storage layer is the bottleneck and needs hardware-side action: IOPS, queue depth, whether data files are on appropriate-tier storage, whether another tenant on the SAN is contending. Latency that varies by time of day is almost always shared-infrastructure contention.");

        t["IO_WRITE_LATENCY_MS"] = new AdviceBlock(
            Headline:
                "Average write latency is above the storage-health threshold — disk writes are slow",
            Investigation:
                "The drill-down `file_latency_breakdown` is already attached: `avg_write_latency_ms` per database and file type, ranked. The transaction log is the usual suspect — every commit blocks on WRITELOG, so even modest log latency directly hurts throughput. Above 10ms on data files is concerning, 2ms on log is the OLTP target. Open File I/O → File I/O Latency for the write series over time. WRITELOG co-elevation confirms log is the bottleneck; PAGEIOLATCH_EX co-elevation means data file writes are slow under modification load. `get_file_io_stats` and `get_file_io_trend` return the same data programmatically.",
            Remediation:
                "Move the transaction log to its own low-latency device (NVMe, log-tier SAN), and never share that device with non-database workloads. For data-file write pressure, look at the modification workload — bulk inserts and large updates are the usual drivers, and batching them so the buffer pool can flush between batches helps. A too-wide index set inflates write I/O: every INSERT and DELETE maintains all nonclustered indexes, and an UPDATE maintains every index that includes one of the changed columns — so dropping unused or redundant indexes directly cuts modification cost. The plan-analysis output and `analyze_query_plan` on the modification queries will surface specific opportunities.");

        t["WRITELOG"] = new AdviceBlock(
            Headline:
                "WRITELOG waits — transactions are waiting for the log to flush before they can commit",
            Investigation:
                "WRITELOG is the wait at COMMIT: every transaction blocks until its log records are durably written to disk. Two independent causes — slow log storage, or too many small commits. The drill-down `file_latency_breakdown` (attached when an I/O finding co-fired) carries `avg_write_latency_ms` for the log file. Open File I/O → File I/O Latency to see the log series over the window. If log latency is low but WRITELOG is still high, the workload is committing too frequently — chatty client code with one tiny transaction per call. Open the Perfmon tab and add `Transactions/sec` to see commit rate; `get_perfmon_trend` returns it programmatically.",
            Remediation:
                "Storage fix: log file on its own low-latency device (NVMe, dedicated log-tier SAN), with no contention from data files or tempdb. Workload fix: batch many small writes into fewer larger transactions where the consistency model allows — turning 1000 single-row inserts per second into ten 100-row batches collapses WRITELOG dramatically. `ALTER DATABASE ... SET DELAYED_DURABILITY = FORCED` trades durability for latency and is appropriate when losing the last few committed transactions on a crash is acceptable; not appropriate for financial or audit workloads.");

        // ─────────────────────────────────────────────────────────────────
        // Latch contention
        // ─────────────────────────────────────────────────────────────────

        t["LATCH_EX"] = new AdviceBlock(
            Headline:
                "Exclusive page-latch contention — in-memory contention on hot pages, often tempdb allocation or last-page insert hotspots",
            Investigation:
                "Page latches are short-term synchronization on individual buffer-pool pages. EX latch waits usually mean parallel inserts into a heap or narrow index (every session contending for the same allocation page), tempdb allocation contention on GAM/SGAM/PFS pages (the `2:1:1` family of resource waits), or insert hotspots on tables with monotonically increasing keys. The LATCH amplifiers (in `FactScorer.LatchAmplifiers`) co-elevate with TEMPDB_USAGE, CXPACKET, and SOS_SCHEDULER_YIELD — those tell you which shape this is. Open the Latch Stats sub-tab under Resource Metrics for the per-latch-class breakdown over the window. Call `get_latch_stats` (Dashboard) for the live `sys.dm_os_latch_stats` view; `get_tempdb_trend` confirms or rules out the tempdb-allocation shape.",
            Remediation:
                "For tempdb allocation contention, confirm at least 4 tempdb data files of equal size (8 if cores >= 8) and that `MIXED_PAGE_ALLOCATION` is `OFF` (the default since 2016). For a last-page insert hotspot — an ever-increasing (IDENTITY / sequential) leading key on a B-tree, where every insert latches the same trailing page — the direct fix is `OPTIMIZE_FOR_SEQUENTIAL_KEY = ON` on that index (2019+); otherwise a non-sequential leading key, hash partitioning, or in-memory OLTP. Adding a clustered index on a sequential key CREATES this contention rather than relieving it, so that is not the fix. For parallel inserts contending on a heap's allocation pages (PFS/IAM), spread or partition the insert workload — a sequential clustered key will only move the contention to the index's last page.");

        t["LATCH_SH"] = new AdviceBlock(
            Headline:
                "Shared page-latch contention — concurrent readers contending for the same hot pages",
            Investigation:
                "Less common than EX, but shows up when many parallel readers hit the same small set of pages — root pages of busy indexes, single-page tables that everyone reads. Open the Latch Stats sub-tab under Resource Metrics for the breakdown by latch class. PAGE class points at the buffer pool; ACCESS_METHODS_HOBT_VIRTUAL_ROOT is the famously hot root-page latch on heavily-read indexes. Call `get_latch_stats` (Dashboard) for the live `sys.dm_os_latch_stats` view. CXPACKET co-elevation (the LATCH amplifier flags this at ≥ 10%) means parallel operations are amplifying the contention.",
            Remediation:
                "Architectural problem, not a configuration one. If a single hot page is being thrashed by small lookups, partition the index so the hot data spans multiple pages, denormalize the lookup into a wider structure, or cache at the application layer. There's no `sp_configure` setting that fixes this — the schema or workload has to change. If the contention is on a queue-table or status-flag pattern that everyone polls, switching that hot pattern to a service broker queue or an event-driven design is usually the durable answer.");

        // ─────────────────────────────────────────────────────────────────
        // TempDB
        // ─────────────────────────────────────────────────────────────────

        t["TEMPDB_USAGE"] = new AdviceBlock(
            Headline:
                "TempDB usage is above the configured threshold — workspace allocation or version store growth is heavy",
            Investigation:
                "The drill-down `tempdb_breakdown` is already attached: per-snapshot `user_objects_mb`, `internal_objects_mb`, `version_store_mb`, and `unallocated_mb` ranked by total usage. That immediately tells you which of the three drivers is the problem. User objects = explicit `#temp` tables and table variables from application code. Internal objects = spills from hash joins, sorts, and hash aggregates running with too-small grants. Version store = RCSI/SI row versions held by a long-running reader transaction. Open the TempDB tab in-app to see the same series graphed. `get_tempdb_trend` returns it programmatically. QUERY_SPILLS co-elevation confirms the spill shape; the drill-down `top_spilling_queries` names the offenders.",
            Remediation:
                "Match the fix to the driver. Spill-driven: update statistics on the offending tables (the cardinality estimate fed the too-small grant), add the missing index the plan analysis surfaces, or use `OPTION (MIN_GRANT_PERCENT = X)` as a per-query stopgap. User-object-driven: look at the worst-offender procedures and ask whether every `#temp` is needed; chains of `SELECT INTO #x FROM #y` are often a single CTE in disguise. Version-store growth: this reports a window that has passed, so the version-holding transaction is long gone — the durable fix is the code path that holds a transaction open across the version-store read (no transaction over 5 minutes is normal for OLTP); shorten it or commit sooner. Tempdb: one data file per core up to 8, sized identically, with autogrowth left ON at an equal fixed-MB increment — pre-size the files so growth is rare rather than disabling it (a hard stop on growth turns a busy window into query failures).");

        // ─────────────────────────────────────────────────────────────────
        // Query-level
        // ─────────────────────────────────────────────────────────────────

        t["QUERY_SPILLS"] = new AdviceBlock(
            Headline:
                "Query spills — operators are running out of granted memory and spilling intermediate results to tempdb",
            Investigation:
                "A hash join, sort, or hash aggregate gets a grant smaller than it needs and falls back to writing to tempdb — usually 10-100x slower than the in-memory operator. Root cause is almost always a bad cardinality estimate: the optimizer guessed 1,000 rows and the operator actually saw 1,000,000. The drill-down `top_spilling_queries` is already attached: five queries ranked by `total_spills` with `database`, `query_hash`, `execution_count`, and truncated `query_text`. Open Queries → Top Queries by Duration in-app and sort by spill columns, or call `get_top_queries_by_cpu` to pull the same view by `query_hash`. `analyze_query_plan` on the hash will surface the operator-level estimate-vs-actual divergence.",
            Remediation:
                "Update statistics with FULLSCAN on the tables that drive the bad estimate — stale statistics are the single most common cause. Add filtered indexes if a subset of the data is the hot spot, or rewrite the operator (a hash join over a sorted input that should have been a merge join is a classic). Per-query stopgaps: `OPTION (RECOMPILE)` re-estimates at each execution against current statistics; `OPTION (MIN_GRANT_PERCENT = X)` forces a larger grant. If many queries spill consistently and grant pressure is high, the workspace pool may simply need more memory available, so check whether `max server memory` is capped too low for the workload.");

        t["QUERY_HIGH_DOP"] = new AdviceBlock(
            Headline:
                "Queries are running with high degree of parallelism — DOP above 8 is unusual outside of warehousing workloads",
            Investigation:
                "Queries at DOP > 8 consume disproportionate scheduler and thread-pool resources for what's usually a small per-query benefit, and they amplify CXPACKET and THREADPOOL pressure. The drill-down `top_cpu_queries` (attached when CPU also flagged) carries `max_dop` per query — that's the direct signal. Open Queries → Top Queries by Duration to see DOP alongside other per-query metrics; sort by `max_dop` descending. Look at per-execution CPU vs. duration: if duration is roughly CPU÷DOP, parallelism is paying for itself; if duration is close to CPU, the workers are mostly contending with each other. Call `get_top_queries_by_cpu` with `parallel_only=true` for the same view programmatically.",
            Remediation:
                "Cap `MAXDOP` at the instance level — the logical processors in a single NUMA node (≤ 8) — and raise `cost threshold for parallelism` to 50 so trivial queries stop going parallel at all. For specific queries that genuinely benefit from higher DOP (analytics, reporting), use `OPTION (MAXDOP X)` as a per-query override rather than raising the instance default. After the change, watch CXPACKET on the Wait Stats tab — if it drops without total CPU regressing, the change was correct.");

        t["PARAMETER_SENSITIVITY"] = new AdviceBlock(
            Headline:
                "Parameter-sensitive plan — one cached plan is wildly different in cost across different parameter values",
            Investigation:
                "The drill-down `parameter_sensitive_queries` is already attached: up to five queries with `worker_ratio` (max/min per-execution CPU for the same plan), `grant_ratio` (grant divergence), and `spills_on_some_inputs` (the plan spills on some parameter values but not others). A `worker_ratio` above 10x — the detector threshold — means the plan is catastrophic for some inputs: usually a plan compiled for a selective value being reused for an unselective one, or vice versa. The query text and hashes are in the drill-down; open Queries → Top Queries by Duration in-app and look up the `query_hash` for the full plan history. `analyze_query_plan` on the hash shows the plan shape, and running the query with the extreme parameter values shows the two divergent shapes.",
            Remediation:
                "**Do not force a plan.** Forcing locks in a plan that is bad for some parameter values — the whole point of detecting parameter sensitivity is that no single plan works for every input. The correct fixes: `OPTION (RECOMPILE)` for a fresh plan per execution (good when values vary widely and the query is not called thousands of times per minute, since compile cost matters), `OPTION (OPTIMIZE FOR ...)` to deliberately compile for the worst-case value, plan guides for specific branches, or splitting the query into two procedures by parameter shape. On SQL Server 2022, Parameter Sensitive Plan optimization (PSP) handles some plan shapes automatically — check the database compatibility level and consider raising it if the workload is on the supported shape list.");

        t["PLAN_REGRESSION"] = new AdviceBlock(
            Headline:
                "Plan regression — a query is running a worse plan than one it has performed well with in the past",
            Investigation:
                "The drill-down `regressed_queries` is already attached: up to five queries with `regression_factor` (latest cost ÷ historical-best cost per execution), `latest_plan_hash`, `best_plan_hash`, `best_plan_id` (the integer ID `sp_query_store_force_plan` requires), and the per-execution CPU and duration numbers for both plans. The 14-day window means the historical best plan may not be in plan cache anymore — Query Store has it. Open Queries → Query Store by Duration in-app to walk the plan history for the `query_id` and visualize the regression. `analyze_query_store_plan` returns the plan analysis for the regressed `query_id` and `plan_id`. If the engine flagged `latest_is_forced > 0` and `force_failure_count > 0`, SQL is already failing to apply a force — examine `sys.query_store_plan` for the `force_failure_reason` because another `sp_query_store_force_plan` call against the same plan will not help.",
            Remediation:
                "Forcing the historical-better plan is the fast fix while you investigate why the worse plan was chosen — the generated EXEC `sp_query_store_force_plan` is in the remediation block below with the IDs filled in. **Confirm the better plan is still better against current data before forcing** — schema changes, data growth, or statistics updates may have made the old plan stale. Run both plans against representative parameter values first. Forcing is reversible: the snippet includes a commented `sp_query_store_unforce_plan` line for back-out. For the durable fix, address the root cause — stale statistics, parameter sniffing on a new value, or a recently-dropped index are the usual culprits.");

        // ─────────────────────────────────────────────────────────────────
        // DB config
        // ─────────────────────────────────────────────────────────────────

        t["DB_CONFIG"] = new AdviceBlock(
            Headline:
                "Database-level configuration issues detected — one or more databases have settings that cause measurable harm",
            Investigation:
                "The drill-down `config_issues` is already attached: per-database breakdown with `recovery_model`, `rcsi`, `query_store`, and an `issues` list naming the specific problems (`auto_shrink ON`, `auto_close ON`, `RCSI OFF`, `page_verify=...`). Each issue has a different cost: AUTO_SHRINK causes catastrophic fragmentation by repeatedly shrinking and re-growing; AUTO_CLOSE adds connection-time overhead on every first query against an idle database; page_verify below CHECKSUM weakens torn-page detection on modern storage; RCSI off makes reader/writer blocking unavoidable. Open Configuration → Database Configuration in-app for the full per-database state. `get_database_config` (Lite) returns it programmatically.",
            Remediation:
                "Mechanical fixes for the unambiguous ones: `ALTER DATABASE <db> SET AUTO_SHRINK OFF` — always. `ALTER DATABASE <db> SET AUTO_CLOSE OFF` — always for any database with real workload. `ALTER DATABASE <db> SET PAGE_VERIFY CHECKSUM` — always on modern storage. RCSI is the only nuanced one: enabling it eliminates reader/writer blocking but adds version-store overhead to writes — test on a copy first if the application uses NOLOCK hints or relies on default isolation semantics in surprising ways. The amplifier on this finding boosts severity when LCK_M_S/LCK_M_IS are also high; that combination is the strong signal RCSI would actually help.");

        t["FILE_AUTOGROWTH_PERCENT"] = new AdviceBlock(
            Headline:
                "Large file(s) growing in percentage steps",
            Investigation:
                "The drill-down `autogrowth_percent_files` lists each offending file (database, logical name, type, size, configured percent). Percentage growth on a large file means an ever-larger single allocation — 10% of a 200 GB file is a 20 GB extend, and every transaction that triggers it waits for the whole allocation. Log growths are always zeroed, so a large percentage log growth stalls every writer until it finishes.",
            Remediation:
                "Switch the flagged files to a fixed-MB autogrowth so each growth is bounded and predictable: 1024 MB for data files, 64 MB for log files. The attached `alter_statement` per file applies exactly that. This is a metadata-only change and is safe to run online.");

        // ─────────────────────────────────────────────────────────────────
        // Server config (WS3) — one advisory per per-setting CONFIG_* root key
        // ─────────────────────────────────────────────────────────────────

        t["CONFIG_MAXDOP"] = new AdviceBlock(
            Headline:
                "MAXDOP is 0 — a single query can fan out across every scheduler (up to 64)",
            Investigation:
                "`max degree of parallelism` at 0 lets one query use all available schedulers, up to a hard limit of 64 processors. On a server with more cores than a single NUMA node, that lets one large query monopolize schedulers and drive up CXPACKET / SOS_SCHEDULER_YIELD; on a small box — a single NUMA node of 8 or fewer logical processors — MAXDOP 0 is within Microsoft's guidance and is far less likely to be the problem. Open Configuration → Server Configuration to see the value alongside the socket/core layout. Note this is the SERVER default: MAXDOP can be overridden per database with `ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = N`, which takes precedence for queries running in that database — so if the parallelism is concentrated in one database, set it there rather than changing the instance default. `get_database_scoped_config` shows the current per-database values (the analysis engine scores only the server default today).",
            Remediation:
                "Set MAXDOP from CPU topology, not edition — the SKU is irrelevant to the right value. Microsoft's guidance keys on logical processors per NUMA node: on a single NUMA node keep MAXDOP at or under the processor count, capped at 8; only on multi-NUMA hardware with more than 16 logical processors per node does it go higher (half the per-node count, max 16). The Apply button sets it to this server's cores-per-socket capped at 8 — cores-per-socket is the NUMA-node proxy, since NUMA node count isn't collected — followed by RECONFIGURE, an online metadata change. On a box with more than 16 logical processors per NUMA node you can raise it by hand. Raise Cost Threshold for Parallelism (the companion finding, if it fired) in the same pass.");

        t["CONFIG_CTFP"] = new AdviceBlock(
            Headline:
                "Cost Threshold for Parallelism is at (or below) the default 5",
            Investigation:
                "`cost threshold for parallelism` is the optimizer-cost cutoff above which a query gets a parallel plan. The shipped default of 5 is from the 1990s: on modern hardware it sends trivial queries parallel, paying thread-coordination overhead (CXPACKET) for no gain. Call `audit_config` for the current value.",
            Remediation:
                "Raise CTFP to 50 as a starting point (then tune up if CXPACKET persists on genuinely large queries). The Apply button runs `sp_configure 'cost threshold for parallelism', 50` + RECONFIGURE — an online metadata change. Pair it with a sane MAXDOP (the companion finding).");

        t["CONFIG_MAX_MEMORY_MB"] = new AdviceBlock(
            Headline:
                "max server memory is unconfigured — SQL Server can consume all the RAM",
            Investigation:
                "`max server memory (MB)` is at its ~2 PB default, so SQL Server will grow its buffer pool until the OS is under memory pressure — which can page out SQL's own working set and destabilize the whole host (and any other instances). Call `audit_config` / open Server Configuration for the current value and the server's total physical RAM.",
            Remediation:
                "Cap max server memory below total RAM, leaving headroom for the OS, the SQL Server thread stacks, and anything else on the box (a common starting point is total RAM minus 4 GB for the OS plus ~1 GB per 4 GB beyond 16 GB, but the right number depends on what else runs here). This is intentionally NOT auto-applied — the correct value is workload- and host-specific. Copy the statement, set the value you've chosen, and run `sp_configure 'max server memory (MB)', <your MB>` + RECONFIGURE.");

        t["CONFIG_MIN_MAX_MEMORY_NARROW"] = new AdviceBlock(
            Headline:
                "min server memory is pinned near max server memory",
            Investigation:
                "`min server memory (MB)` is set within ~20% of `max server memory (MB)`. min server memory is a floor SQL will not release BELOW once reached — pinning it near max means SQL effectively never gives memory back to the OS, which starves other processes and defeats the OS's ability to reclaim under pressure. The finding's metadata carries the configured min and max. Call `audit_config` for context.",
            Remediation:
                "Lower min server memory well below max so SQL can grow into the cap under load but release memory back to the OS when idle (min is best left at the default 0, or a modest floor only if you have a specific reason). This is NOT auto-applied — how low to set it is a workload judgement. Copy the statement, choose your floor, and run `sp_configure 'min server memory (MB)', <your MB>` + RECONFIGURE.");

        // ─────────────────────────────────────────────────────────────────
        // Server health (WS5) — advise-only: LPIM, IFI, memory dumps.
        // No Apply (these need OS / service-account / investigation work);
        // advice prose + copy-paste guidance only.
        // ─────────────────────────────────────────────────────────────────

        t["CONFIG_IFI_DISABLED"] = new AdviceBlock(
            Headline:
                "Instant File Initialization is OFF — data file growth and restores zero-fill the whole allocation",
            Investigation:
                "Without Instant File Initialization (IFI) SQL Server writes zeros across every newly-allocated data-file region before it can be used — so every data-file autogrowth, every CREATE DATABASE, every RESTORE, and every TempDB re-creation at startup stalls for as long as it takes to zero the new space (minutes, on large files). The finding's metadata carries the service account that needs the right. Confirm the current state with `SELECT servicename, instant_file_initialization_enabled FROM sys.dm_server_services;` (SQL 2016 SP1+). IFI applies to DATA files only — log files are always zeroed regardless, which is why fixed-MB log autogrowth still matters.",
            Remediation:
                "Grant the SQL Server service account the Windows 'Perform volume maintenance tasks' user right (SeManageVolumePrivilege): secpol.msc → Local Policies → User Rights Assignment → Perform volume maintenance tasks → add the service account, then restart the SQL Server service for it to take effect. The SQL Server 2016+ setup wizard offers this as a checkbox; on Linux IFI is effectively always on (no action). This is an OS-level change, not a T-SQL one, so there is nothing to Apply from here — but it is one of the highest-value, lowest-risk changes you can make to file-growth and restore times.");

        t["CONFIG_LPIM_DISABLED"] = new AdviceBlock(
            Headline:
                "Lock Pages in Memory is OFF — the OS can page out SQL Server's buffer pool under memory pressure",
            Investigation:
                "Lock Pages in Memory (LPIM) prevents Windows from trimming SQL Server's working set — paging the buffer pool out to disk — when the OS comes under memory pressure. Without it, a memory-hungry neighbour process (or a runaway on the box) can force SQL's data cache out to the page file, and you see SQL's working set suddenly trimmed — total server memory dropping below its target with a spike in hard page faults that no SQL workload explains. This finding only fires on non-Express editions with meaningful RAM, where the buffer pool is large enough for paging to hurt. Read the current memory model with `SELECT sql_memory_model_desc FROM sys.dm_os_sys_info;` — CONVENTIONAL means LPIM is off; LOCK_PAGES or LARGE_PAGES means it is in effect.",
            Remediation:
                "LPIM is a judgement call, not an automatic win, which is why there is nothing to Apply from here. Grant it only alongside a correctly-configured 'max server memory (MB)' cap — LPIM with an uncapped max can starve the OS itself. To enable: grant the SQL Server service account the Windows 'Lock pages in memory' user right (secpol.msc → Local Policies → User Rights Assignment → Lock pages in memory), then restart the SQL Server service. It is most valuable on dedicated database hosts with large buffer pools; on a shared or memory-constrained box, fix 'max server memory' first and weigh LPIM second.");

        t["SERVER_MEMORY_DUMPS"] = new AdviceBlock(
            Headline:
                "SQL Server has written one or more memory dumps — the engine hit a condition it considered worth dumping",
            Investigation:
                "Every row in `sys.dm_server_memory_dumps` is a point where SQL Server detected something abnormal — an access violation, a non-yielding scheduler, a latch time-out, a failed assertion, or a stack corruption — and wrote a dump for diagnosis. The finding's metadata carries the dump count. List them with `SELECT filename, creation_time, size_in_bytes FROM sys.dm_server_memory_dumps ORDER BY creation_time DESC;` and correlate the creation_time against the SQL Server ERRORLOG (`EXEC sys.xp_readerrorlog 0, 1, N'dump';`) — the log entry around each dump names the failure type. A single old dump from a since-patched build is usually historical; recent or repeating dumps are a live reliability signal.",
            Remediation:
                "Dumps mean investigate, not a setting to flip — so there is nothing to Apply. First, get current on Cumulative Updates: a large share of dump-producing bugs are already fixed in later builds, and 'apply the latest CU and re-evaluate' resolves many cases outright. If dumps continue on a current build, match the ERRORLOG failure type to a known issue or open a case with Microsoft and attach the dump files — they are the artifact support needs. Watch the volume holding the dump directory: repeated large dumps can themselves fill the disk. Do not delete the dump files until you (or support) have read them.");

        // ─────────────────────────────────────────────────────────────────
        // Query-plan advisories (WS4) — advise-only: missing indexes, plan warnings.
        // Parsed from the already-collected plans of the top queries by cost. The
        // specific suggested indexes / warnings are in the finding's drill-down detail;
        // no Apply (index + query changes are judgement calls, tested per workload).
        // ─────────────────────────────────────────────────────────────────

        t["MISSING_INDEX"] = new AdviceBlock(
            Headline:
                "The optimizer asked for indexes that don't exist — top queries are scanning where they could seek",
            Investigation:
                "SQL Server records a missing-index request in the query plan whenever the optimizer believes an index it couldn't find would have materially lowered a query's cost. This finding parsed the actual plans of your most expensive queries and collected those requests; the drill-down lists each one with its table, the optimizer's estimated impact %, and the suggested CREATE INDEX. Treat them as a STARTING POINT, not a prescription: the engine's suggestions are naive — it proposes one index per query in isolation, often with the key column order wrong, with wide INCLUDE lists, and with no awareness of indexes you already have or of the write cost. Weigh each by impact AND by how often the query actually runs.",
            Remediation:
                "Evaluate and test — there is nothing to auto-Apply, because a wrong index is worse than a missing one (every INSERT and DELETE pays to maintain it, as does any UPDATE that touches one of its columns). Consolidate overlapping suggestions into the fewest indexes that cover them, get the key-column order right (equality before inequality, then by selectivity), keep INCLUDE lists lean, and check the suggestion against your existing indexes so you don't create a near-duplicate. Validate the chosen index against the real plan on a copy of the data before it goes to production. The drill-down's CREATE statements are the raw optimizer text — refine them, don't paste them blind.");

        t["PLAN_WARNING"] = new AdviceBlock(
            Headline:
                "Plans for your top queries carry warnings — implicit conversions, spills, oversized grants and the like",
            Investigation:
                "The plan analyzer inspected the actual execution plans of your most expensive queries and flagged actionable problems; the drill-down lists each by type, severity, and message. Common ones: an implicit conversion (a WHERE/JOIN comparing mismatched data types, which makes the predicate non-sargable and forces a scan), a sort/hash spill to tempdb (the memory grant was too small because the row estimate was low), an excessive or stuck memory grant (one query reserving memory that throttles everyone else's concurrency), a forced-serial or ineffective parallel plan, or a scalar-UDF call evaluated per row. Each warning is tied to a specific query in the detail, so you know exactly where to look.",
            Remediation:
                "These are query- and schema-specific fixes, so there's nothing to Apply globally. Implicit conversions: align the column and parameter/literal data types (or fix the column type) so the predicate can seek. Spills and bad grants: the root cause is almost always estimate inaccuracy — update statistics, and look for stale stats, table variables, or non-sargable predicates feeding the estimate. Oversized grants: fix the estimate first; `MIN_GRANT_PERCENT`/`MAX_GRANT_PERCENT` hints are a last resort. Work the highest-severity, most-frequently-run warnings first — the detail names the query and the exact warning type for each.");

        // ─────────────────────────────────────────────────────────────────
        // Jobs / disk / bad actors
        // ─────────────────────────────────────────────────────────────────

        t["RUNNING_JOBS"] = new AdviceBlock(
            Headline:
                "Long-running SQL Agent jobs detected — one or more jobs have been running past their normal duration",
            Investigation:
                "Open the Running Jobs tab in-app: the engine shows each currently-running job with its current duration alongside historical average and p95, with jobs above their normal duration flagged. Call `get_running_jobs` for the same view programmatically. Common patterns: a maintenance job (CHECKDB, index rebuild) that overlapped into business hours, a job stuck on a blocked spid (BLOCKING_EVENTS will co-fire), or a job whose data volume grew beyond what its design assumed. SCH_M co-elevation usually means a long index-maintenance job is now blocking the regular workload. For long-window history, `msdb.dbo.sysjobs` and `sysjobhistory` carry job-history detail beyond the engine's analysis window.",
            Remediation:
                "If the job is still running, you can decide kill-vs-wait: killing mid-CHECKDB or mid-rebuild is safe — both unwind cleanly, you just redo the work in a real maintenance window. But this finding is about the recurrence, not a one-off kill: for chronically-long jobs the workload has usually outgrown the implementation: CHECKDB on a 10 TB database isn't a job, it's a project. Partial substitutes: `WITH PHYSICAL_ONLY` for the daily check plus a less-frequent full check, partitioning so maintenance runs on one partition at a time, incremental statistics so updates touch one partition. For jobs blocked behind blocking, the blocking is the real problem — work the BLOCKING_EVENTS playbook.");

        t["DISK_SPACE"] = new AdviceBlock(
            Headline:
                "Disk free space is below the healthy-headroom threshold — risk of database growth being denied",
            Investigation:
                "Below 10% free is concerning, below 5% is critical. Open File I/O → File I/O Latency or the TempDB tab to see per-file growth over the analysis window. Call `get_database_sizes` (Lite) for per-file totals, auto-growth settings, and the volume free-space numbers. Common shapes: tempdb growth from a runaway spilling query (QUERY_SPILLS will co-fire and `top_spilling_queries` will name it), a log file that has not been backed up (FULL recovery without log backups means the log cannot truncate and grows indefinitely), or a data file with overly-aggressive auto-growth that filled the volume.",
            Remediation:
                "Free space first, root cause second. For FULL-recovery databases with bloated logs, take a log backup — that's the only way to truncate. For tempdb that grew from a one-time spill event, `DBCC SHRINKFILE` is one of the few legitimate uses of shrink (do it after the spilling query is fixed, or it'll just regrow). For data files, expand the volume if business-as-usual growth filled it. Going forward: set fixed-MB auto-growth (not percentage), pre-size based on observed growth, and confirm Instant File Initialization is granted to the service account so growths don't block writes while the file is zeroed.");

        t["BAD_ACTOR"] = new AdviceBlock(
            Headline:
                "One query is dominating workload cost — its frequency × per-execution impact rank it as the top offender",
            Investigation:
                "The drill-down `bad_actor_query` is already attached: `database`, `query_hash`, `query_text`, `execution_count`, `avg_cpu_ms`, `avg_elapsed_ms`, `avg_reads`, `total_cpu_ms`, `total_reads`, `total_spills`, `max_dop`. The drill-down also fetches the live plan for this hash and attaches `plan_analysis` with the warnings and missing-index suggestions the optimizer generated. Shape matters: a query running 100,000 times at 5ms is a parameterisation or caching opportunity; one running once at 30 seconds is a single-query tuning project. Open Queries → Top Queries by Duration in-app and search for the `query_hash` for the full plan history; `get_query_trend` returns the time-series for this hash; `analyze_query_plan` returns the full plan analysis on demand.",
            Remediation:
                "Match the fix to the shape. High-frequency lightweight: does the WHERE clause have a covering index? Can the application cache results so it doesn't ask 100,000 times per minute? Is parameterisation working, or is every execution compiling? Low-frequency heavyweight: this is a tuning project — apply the missing-index suggestions from `plan_analysis`, rewrite subqueries to joins (or back), update statistics on the driving tables. The `plan_analysis` field already lists the optimizer's own missing-index suggestions with `impact` and `create_statement`; treat those as the starting point, not the answer.");

        // ─────────────────────────────────────────────────────────────────
        // Anomaly facts (first-class)
        // ─────────────────────────────────────────────────────────────────

        t["ANOMALY_CPU_SPIKE"] = new AdviceBlock(
            Headline:
                "CPU is anomalously elevated compared to its baseline for this time bucket",
            Investigation:
                "The anomaly detector compares this window's CPU to the same hour-of-week historical baseline over 30 days; 2σ trips the warning, 4σ trips critical. The baseline context (deviation, ratio, baseline mean, sample count) is in the finding's metadata. The drill-down `spike_peak` and `queries_at_spike` are attached: the exact peak timestamp and the five sessions active within ±2 minutes of it. Open the CPU tab and zoom to `spike_peak.time` to see the SQL vs. other-process split. Call `get_cpu_utilization` for the per-minute trend and `get_active_queries` for the wider list of sessions active across the window. Common drivers: a recently-cached bad plan (check whether PLAN_REGRESSION also fired), a one-off workload (marketing campaign, backfill job), or another process on the host.",
            Remediation:
                "Confirm one-time vs. sustained. Transient (a deploy, an ad-hoc report) needs no action beyond noting it — the anomaly framing means 'unusual right now', not 'automatically bad'. If the elevation persists, the threshold-based CPU_SQL_PERCENT finding will fire on the next analysis window and the standard CPU-pressure playbook applies (tune the queries in `top_cpu_queries`, address parallelism via `audit_config`, force regressed plans via the PLAN_REGRESSION remediation).");

        t["ANOMALY_BLOCKING_SPIKE"] = new AdviceBlock(
            Headline:
                "Blocking events are anomalously elevated compared to baseline — a ratio above 3x normal rate",
            Investigation:
                "Ratio-based scoring: 3x baseline = 0.5 score, 10x = 1.0, so this finding means blocking rate is well above the workload's normal pattern for this hour-of-week bucket. The drill-down `top_blocking_chains` is attached when BLOCKING_EVENTS scoring also surfaced data; `reconstructed_blocking_chains` carries the apex with `apex_sleeping` for the abandoned-transaction signature. Open Blocking → Blocked Process Reports and zoom to the window. Call `get_blocking_trend` (Lite) or `get_blocking_deadlock_stats` (Dashboard) to see whether the rate is climbing or a one-shot spike, and `get_blocked_process_reports` (Lite) or `get_blocked_process_xml` (Dashboard) for the full parsed events. A sudden ratio spike often points to a recent deploy, schema change, or new query pattern that introduced fresh contention.",
            Remediation:
                "First question: what changed? Recent deploy, schema migration, new query pattern, job running outside its normal window. If a sleeping apex headed the chain (`apex_sleeping = true`), that is the abandoned-transaction signature — but this is a report on a window that has passed, so the chain has cleared and a KILL buys nothing; fix the code path that left a BEGIN TRAN open on its error/timeout path (SET XACT_ABORT ON) so it stops recurring. For systemic increases without an obvious change, the BLOCKING_EVENTS standard playbook applies: RCSI for reader/writer waits, shorter transactions for writer/writer, index tuning to reduce lock-hold duration on the slow operations everyone is queueing behind.");

        t["ANOMALY_DEADLOCK_SPIKE"] = new AdviceBlock(
            Headline:
                "Deadlock rate is anomalously elevated — a sudden jump in deadlocks compared to normal",
            Investigation:
                "Deadlocks normally run at a low steady-state rate determined by the workload's transaction patterns; a ratio spike against the hour-of-week baseline means a new deadlock-prone interaction was just introduced. The drill-down `top_deadlocks` is attached with the three most recent victims by collection time. Open Blocking → Deadlocks and zoom to the window — the new pattern will dominate the list, and each row has the full graph XML viewable in-app. Call `get_deadlock_detail` to extract the graph XML for the dominant pattern. `get_deadlock_trend` (Lite) or `get_blocking_deadlock_stats` (Dashboard) shows whether the rate is still climbing.",
            Remediation:
                "Find the new pattern in the graphs and fix the interaction — almost always by enforcing consistent object access order between the two procedures involved, or by shortening the transaction so the window for the deadlock is smaller. If a recent deploy correlates with the spike, the new code is the prime suspect — review the latest changes to the procedures named in the deadlock graphs. For reader/writer deadlocks specifically (LCK_M_S victims in the graph), RCSI on the affected database eliminates the entire class.");

        t["ANOMALY_READ_LATENCY"] = new AdviceBlock(
            Headline:
                "Read latency is anomalously elevated compared to its baseline — storage was slower than normal during this window",
            Investigation:
                "Hour-of-week baseline comparison: reads are slower than they usually are at this specific time bucket. The drill-down `file_latency_breakdown` (attached when scoring also surfaced data) names which files are affected — data, log, or tempdb. Open File I/O → File I/O Latency and zoom to the window for the trend. `get_file_io_stats` and `get_file_io_trend` return the same data programmatically. Two shapes: storage-tier anomalies (another tenant on the SAN, a backup job that overlapped) usually resolve on their own; workload-tier anomalies (a new scan-heavy query, a missing-index regression) persist until the workload is fixed.",
            Remediation:
                "Check `top_cpu_queries` (attached when CPU is co-elevated) for queries with unusually high `logical_reads` per execution — if one stands out, that's the workload-tier cause and fixing the index or the query catches the storage back up. If the queries all look normal but latency is up, it's a storage-layer issue: check with the storage team about other tenants, scheduled jobs (backups, replication, snapshots) on the same infrastructure. Transient storage-side anomalies need only monitoring; sustained ones become an IO_READ_LATENCY_MS threshold finding on the next window and the standard playbook applies.");

        t["ANOMALY_WRITE_LATENCY"] = new AdviceBlock(
            Headline:
                "Write latency is anomalously elevated compared to its baseline — disk writes were slower than normal during this window",
            Investigation:
                "Same shape as the read-latency anomaly, for writes. Log file write latency is usually the most consequential — every commit blocks on WRITELOG, so even a brief elevation hurts throughput. The drill-down `file_latency_breakdown` (when scoring surfaced it) names the slow files; open File I/O → File I/O Latency for the trend. `get_file_io_stats` returns the latest snapshot. WRITELOG co-elevation means the workload was committing through the slow log during this window. Storage-side events — a SAN snapshot, a failover, a backup that overlapped — often correlate with anomalous write latency.",
            Remediation:
                "If a specific external event correlates (overlapping backup, SAN maintenance window), let it pass — that's expected behaviour with no SQL-side fix. If the elevation is sustained without an obvious external cause, the storage layer needs investigation: the log file should live on its own low-latency device. Workload-side, a sudden burst of large writes (bulk insert, big update) can drive transient write latency anomalies — if the burst recurs, the WRITELOG playbook applies (batch writes, separate log storage, delayed durability for non-financial workloads).");

        t["ANOMALY_BATCH_REQUESTS"] = new AdviceBlock(
            Headline:
                "Batch requests per second are anomalously elevated — the server is fielding far more queries than usual",
            Investigation:
                "Batch rate is the rawest measure of workload, so a sigma spike means traffic surged for this hour-of-week bucket. Open the Perfmon tab and add `Batch Requests/sec` for the trend; call `get_perfmon_trend` with `counter_name=Batch Requests/sec` programmatically. The drill-down `queries_at_spike` (when CPU also spiked) shows what was active at the peak. `get_active_queries` returns the active-session snapshots across the window; `get_top_queries_by_cpu` ordered by `execution_count` ranks the high-frequency queries. Common drivers: an application loop that broke (chatty retries, polling that should be event-driven), a new caller hitting the database directly, or a legitimate burst (marketing event, batch import).",
            Remediation:
                "Confirm intentional vs. broken. Misbehaving application — uncached lookups, retry storms, polling instead of subscribing — is fixed at the source; no amount of SQL tuning will help a server hit with 100,000 unnecessary requests per second. Legitimate burst that's the new normal: scale the workload through app-layer caching, connection pooling, or splitting reads to a replica. If the new load level persists going forward, it'll appear as a new BAD_ACTOR or CPU finding on subsequent analysis windows and the standard playbooks apply.");

        t["ANOMALY_SESSION_SPIKE"] = new AdviceBlock(
            Headline:
                "Active session count is anomalously elevated — far more connections than normal for this time bucket",
            Investigation:
                "Session-count spikes usually mean clients are holding connections open longer than expected. Open the Session Stats sub-tab under Resource Metrics (Dashboard) to see the trend with the top application and host names already aggregated; `get_session_stats` returns the same view grouped by application. Typical causes: a long-blocked workload (BLOCKING_EVENTS or BLOCKING_CHAIN co-elevation confirms it), connection-pool exhaustion in the app, or a runaway client opening connections without closing them. Call `get_active_queries` to see what those sessions were actually doing.",
            Remediation:
                "If the count climbed because workers are blocked, blocking is the real problem — sessions free themselves as blocking resolves and the BLOCKING_EVENTS playbook applies. If a specific application is accumulating connections without releasing (the Session Stats top-app text will name it), the connection-pool configuration there is wrong: pool size, idle timeout, or an outright leak. Watch for sustained session counts approaching `max user connections` or the worker-thread limit — at that point the spike is an availability problem, not just a workload anomaly.");

        t["ANOMALY_QUERY_DURATION"] = new AdviceBlock(
            Headline:
                "Query duration is anomalously elevated — the median or P95 query is slower than baseline for this time bucket",
            Investigation:
                "Duration anomalies are workload-shape signals: either the same queries are running slower, or a different mix is running than usual. Open Queries → Top Queries by Duration and zoom to the window; `get_query_duration_trend` (Lite) returns the average-duration time-series. Compare against `get_top_queries_by_cpu` ordered by `avg_elapsed_ms` for the elapsed-time leaders. If one specific query's duration jumped, PLAN_REGRESSION and PARAMETER_SENSITIVITY are the right places to look — both will surface the root cause cleanly with the drill-down already populated.",
            Remediation:
                "Track down the slow query through the standard channels: plan analysis (`analyze_query_plan` on the `query_hash`), statistics, indexes. If multiple queries got slower at once, the host itself is slower — co-elevated CPU, I/O, or memory findings should pinpoint which resource. Duration anomalies without any other resource finding almost always mean blocking; check the Blocking → Blocked Process Reports view and the BLOCKING_EVENTS fact even if it didn't score above its own threshold this window.");

        t["ANOMALY_MEMORY_PRESSURE"] = new AdviceBlock(
            Headline:
                "Server memory usage is anomalously elevated vs. baseline — SQL's total memory has moved unusually against its target for this time bucket",
            Investigation:
                "This anomaly compares the ratio of `Total Server Memory` to `Target Server Memory` (the two memory counters the collectors store) against its hour-of-week baseline. A spike usually means the OS forced SQL's target down under external memory pressure, or the buffer pool grew unusually fast. Open Memory → Overview to see total vs. target and the buffer pool across the window, and Memory → Memory Clerks to see where the bytes went. If MEMORY_GRANT_PENDING co-fired its `pending_grants` drill-down is attached and grant pressure is part of the story. Call `get_memory_stats` for the latest snapshot, `get_memory_trend` for the time-series, `get_memory_clerks` for the allocation breakdown, and `get_memory_pressure_events` (Lite) for the ring-buffer notifications. QUERY_SPILLS co-elevation means queries are running with grants too small and spilling to tempdb.",
            Remediation:
                "Match the fix to the shape. If total server memory dropped below target, the OS is reclaiming memory from SQL — check whether `max server memory` is capped too low and whether another process on the host is the aggressor; on a dedicated box, Lock Pages in Memory (the CONFIG_LPIM_DISABLED finding) prevents the paging. If the buffer pool grew fast and grants are pending, an offender is consuming a too-large grant from a bad cardinality estimate — `analyze_query_plan` on the worst `query_hash` shows the estimate-vs-actual divergence, and FULLSCAN statistics or a filtered index fixes it. Anomalies that resolve on their own are typically one-time reporting queries; sustained ones become standard RESOURCE_SEMAPHORE or memory-grant findings on the next window.");

        t["ANOMALY_OBJECT_GROWTH"] = new AdviceBlock(
            Headline:
                "A table grew sharply day-over-day — its reserved size jumped well above its recent footprint",
            Investigation:
                "Computed from the two most recent daily index/object-size snapshots: the named table's total reserved MB (all indexes summed) rose by more than the trip threshold in both percentage and absolute terms. The finding's metadata carries `database_name`, `schema_table`, `prior_mb`, `current_mb`, `growth_mb`, and `growth_pct`. Open FinOps → Object Sizes & Growth and sort by Growth 30d / Daily Rate to see the trend and the next-largest growers; call `get_table_index_sizes` for the full ranked list. Distinguish a one-time load (archive import, backfill, index rebuild that temporarily doubles space) from sustained organic growth — the daily-rate column over several days tells you which.",
            Remediation:
                "If it's expected load, no action beyond capacity awareness — confirm the volume has headroom (FinOps → Database Sizes shows volume free space). If it's unexpected: check for a runaway process inserting without cleanup, a disabled or broken purge job, an index rebuild leaving the old allocation, or a heap that needs a clustered index. For genuinely large, fast-growing tables, evaluate PAGE compression and partitioning. Persistent steep growth against limited volume free space is the early warning for an out-of-space outage — act before the file fills.");

        t["ANOMALY_OBJECT_CONTENTION"] = new AdviceBlock(
            Headline:
                "An index accrued significant new lock-wait time — contention on this object jumped since the last snapshot",
            Investigation:
                "Computed from consecutive daily index/object snapshots (same instance start time, so not a counter reset): the named index's cumulative row-lock wait time grew by more than the trip threshold day-over-day. Metadata carries `database_name`, `schema_table`, `index_name`, `lock_wait_ms_delta`, and `escalation_delta`. Open FinOps → Locking & Contention to see this object and the other top-contended indexes; call `get_object_locking` for the ranked list. Cross-reference Blocking → Blocked Process Reports for the same window to see the actual blocking chains, and check whether a lock-escalation spike (`escalation_delta`) accompanied it — escalation to a table lock serializes the whole object.",
            Remediation:
                "Find the queries hitting this object (Query Performance filtered to the database/table) and reduce how long they hold locks: shorten transactions, add the missing index so writers touch fewer rows, or batch large modifications. Reader/writer contention (S vs X) is eliminated by RCSI on the database. If lock escalation is the driver, either fix the query touching too many rows or, as a last resort, disable escalation on the specific table (ALTER TABLE ... SET (LOCK_ESCALATION = DISABLE)) after confirming memory headroom for the extra row locks.");

        return t;
    }
}
