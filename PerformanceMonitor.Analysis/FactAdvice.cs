using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
    /// The ONE caveat sentence that rides with every missing-index suggestion this engine delivers (#3805) —
    /// the MISSING_INDEX card's investigation opens with it, both the static block and the composed one, so the
    /// drill-down's CREATE statements never reach a reader without it. Fixed text, verbatim from the maintainer,
    /// byte-identical to <c>McpPlanAnalysisFormatter.MissingIndexCaveat</c> in <c>PerformanceMonitor.PlanAnalysis</c>,
    /// which every <c>analyze_*_plan</c> tool puts on its <c>missing_indexes[]</c> rows as <c>caveat</c>. Duplicated
    /// rather than referenced because this assembly references no project (it is the shared engine every SKU
    /// builds on), and <c>McpPlanAnalysisEnvelopeTests</c> pins the two strings equal so the duplication cannot
    /// drift. The maintainer's position the sentence carries: a request is corroboration for a statement already
    /// measured slow, never a diagnosis and never a finding's driver; its counters are plan-cache-bounded and its
    /// impact is one operator's statement-scoped estimate; an index is a per-table commitment with write cost and
    /// regression risk for other plans. <c>FactScorer</c> demotes the fact to the Information rung for the same
    /// reason.
    /// </summary>
    public const string MissingIndexCaveat = "Missing-index requests are weak evidence: uses are plan-cache-bounded and \"impact\" is one operator's estimated cost. A request corroborates a measured-slow plan; it never drives a finding. Any new index can regress other statements and adds write cost — test it.";

    /// <summary>
    /// The ONE clause a card gains when its fact ranked more objects than its own subject (#3691 lane 43):
    /// <c>"; and two more: public.orders (3.1× for 5 hours), public.events (1.4× for 2 hours)"</c>. Appends to
    /// the sentence that states the SUBJECT's figures, because <see cref="Fact.Ranked"/>[0] IS the subject — so
    /// "two more" is literally "besides the one just described" and can never be read as two more than the
    /// population count a card states separately.
    ///
    /// <para><b>Empty string below two ranked objects</b>, which is the byte-identity arm and the reason this
    /// returns a clause rather than composing a sentence: a caller interpolates it into the prose it already
    /// had, so every fact that ranks nothing (nearly all of them, and every SQL Server fact — no SQL Server
    /// collector sets <see cref="Fact.Ranked"/>) renders the characters it rendered before. Pinned by
    /// <c>FactRankedTests</c>.</para>
    ///
    /// <para><b>Why the figures come from a callback.</b> Each family states its rank in its own units and its
    /// own words — a ratio and a duration here, bytes and a percentage there — and the numbers live in
    /// <see cref="RankedObject.Figures"/> under that family's own metadata names. A shared formatter would
    /// either have to know every family or print raw doubles, so the family passes its own one-line formatter
    /// and this owns only the grammar. Return an empty string from it for an object whose name is the whole
    /// statement and the parentheses are omitted.</para>
    ///
    /// <para>Lives in this shared file rather than in <c>PgTargetAdvice</c> because the seam it reads is shared:
    /// <see cref="Fact.Ranked"/> is on <see cref="Fact"/>, and the first SQL Server family that ranks objects
    /// must not have to grow a second copy of this grammar to say the same thing. Today's three callers are all
    /// PostgreSQL-target.</para>
    /// </summary>
    public static string NameTheRest(Fact fact, Func<RankedObject, string> figures)
    {
        var named = NameTheRestList(fact, figures, separator: ", ");
        if (named.Length == 0)
            return string.Empty;

        /* Spelled out to three, which is FactRanked.MaxObjects — the numeral arm exists so a cap raised without
           touching this file reads as English rather than as "and 4 more" beside spelled-out siblings. */
        var rest = fact.Ranked.Count - 1;
        var count = rest switch
        {
            1 => "one",
            2 => "two",
            3 => "three",
            _ => rest.ToString(CultureInfo.InvariantCulture),
        };

        return $"; and {count} more: {named}";
    }

    /// <summary>
    /// The NAMED LIST alone — <c>"public.orders (300 MB, 40%), sales.ledger (10 MB)"</c> — without the clause
    /// grammar <see cref="NameTheRest"/> wraps it in, for a family whose card states the rest in a SENTENCE of
    /// its own rather than as a clause on the subject's. Empty string under two ranked objects, same rule and
    /// same reason (#3691 lane 43).
    ///
    /// <para>Two entry points and one formatter, because the alternative is two grammars: the bloat cards state
    /// the subject's figures across several sentences and could not append "and two more" to any one of them
    /// without reading as a comment on that sentence, while the autovacuum-disabled card states its subject in
    /// one sentence and wants the clause there. What must not differ between them is how an object and its
    /// figures are rendered, and that lives here.</para>
    ///
    /// <para><paramref name="separator"/> is the caller's because it is a property of the FIGURES, not a
    /// preference: the bloat families state three figures per object ("300 MB, 40%, 20,000 dead tuples now"),
    /// so a comma between objects would be one comma among four and the list would stop parsing to a human;
    /// the clause form's figures carry none, so a comma is right there. Both spellings are pinned.</para>
    /// </summary>
    public static string NameTheRestList(Fact fact, Func<RankedObject, string> figures, string separator)
    {
        ArgumentNullException.ThrowIfNull(fact);
        ArgumentNullException.ThrowIfNull(figures);
        ArgumentException.ThrowIfNullOrEmpty(separator);

        if (fact.Ranked.Count < 2)
            return string.Empty;

        var named = fact.Ranked.Skip(1).Select(o =>
        {
            var text = figures(o) ?? string.Empty;
            return text.Length == 0 ? o.ObjectName : $"{o.ObjectName} ({text})";
        });

        return string.Join(separator, named);
    }

    /// <summary>
    /// Looks up advice for a fact-key. Returns null if the key is unknown.
    /// </summary>
    public static AdviceBlock? GetForFactKey(string? factKey)
    {
        if (string.IsNullOrEmpty(factKey))
            return null;

        if (_byKey.TryGetValue(factKey, out var direct))
            return direct;

        /* #3542: the PostgreSQL-target vocabulary has its own static table, in PgTargetAdvice — one arm
           here, so the content lanes never touch this file. Before any SQL Server prefix arm, because
           ANOMALY_PG_* must not reach the ANOMALY_WAIT_ composer below. */
        if (PgTargetAdvice.IsPgKey(factKey))
            return PgTargetAdvice.Static(factKey);

        if (factKey.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase))
            return _byKey.GetValueOrDefault("BAD_ACTOR");

        // ANOMALY_WAIT_PROFILE is the one all-types wait-profile fact (change 1). It prefix-matches
        // ANOMALY_WAIT_ below, so special-case it FIRST — the generic per-type composer would render
        // "Anomalous spike in PROFILE" otherwise.
        if (factKey.Equals("ANOMALY_WAIT_PROFILE", StringComparison.OrdinalIgnoreCase))
            return _byKey.GetValueOrDefault("ANOMALY_WAIT_PROFILE");

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
        /* #3542: the PostgreSQL-target vocabulary composes in PgTargetAdvice — one delegating arm, so the
           content lanes never edit this switch. */
        if (PgTargetAdvice.IsPgKey(rootFactKey))
            return PgTargetAdvice.Compose(rootFactKey, factsByKey);
        // BAD_ACTOR facts are keyed BAD_ACTOR_{query_hash}; compose from that fact directly.
        if (rootFactKey.StartsWith("BAD_ACTOR_", StringComparison.Ordinal))
            return ComposeBadActor(rootFactKey, factsByKey);
        // Range-lock waits surface under the real LCK_M_RS_*/RIn_*/RX_* keys (kept individual by the
        // collector), each aliased to the LCK_RANGE block; state their wait totals.
        if (rootFactKey.StartsWith("LCK_M_R", StringComparison.Ordinal))
            return ComposeRangeLock(rootFactKey, factsByKey);
        return rootFactKey switch
        {
            // CPU: state the actual CPU %, the SQL-vs-other-process split, and the signal-wait share
            // (the collected time-based proxy for runnable-queue depth) that separates genuine CPU
            // pressure from CPUs that are simply busy doing real work.
            "SOS_SCHEDULER_YIELD" => ComposeSos(factsByKey),
            "CPU_SQL_PERCENT" => ComposeCpuSqlPercent(factsByKey),
            "CPU_SPIKE" => ComposeCpuSpike(factsByKey),
            "THREADPOOL_PARALLEL" => ComposeThreadpoolParallel(factsByKey),
            "THREADPOOL_MIXED" => ComposeThreadpoolMixed(factsByKey),
            "CONFIG_MAXDOP" => ComposeConfigMaxdop(factsByKey),
            "CONFIG_CTFP" => ComposeConfigCtfp(factsByKey),
            // Parallelism value-gap blocks: state the server's actual MAXDOP/CTFP instead of the
            // generic "raise CTFP to 50, cap MAXDOP" guidance (or, for CXPACKET, an audit_config
            // deferral). All three reuse the same current-values prefix.
            "THREADPOOL" => ComposeWithMaxdopCtfpPrefix("THREADPOOL", factsByKey),
            "CXPACKET" => ComposeWithMaxdopCtfpPrefix("CXPACKET", factsByKey),
            "QUERY_HIGH_DOP" => ComposeQueryHighDop(factsByKey),
            // Memory value blocks: state the server's actual max server memory + physical RAM
            // instead of deferring to audit_config (B2). The two config-rooted blocks state the
            // configured values; the wait/anomaly blocks append the current cap.
            "CONFIG_MAX_MEMORY_MB" => ComposeConfigMaxMemory(factsByKey),
            "CONFIG_MIN_MAX_MEMORY_NARROW" => ComposeConfigMinMaxNarrow(factsByKey),
            "PAGEIOLATCH_SH" => ComposePageIoLatch(factsByKey, "PAGEIOLATCH_SH"),
            "PAGEIOLATCH_EX" => ComposePageIoLatch(factsByKey, "PAGEIOLATCH_EX"),
            "MEMORY_GRANT_PENDING" => ComposeMemoryGrantPending(factsByKey),
            "QUERY_SPILLS" => ComposeQuerySpills(factsByKey),
            // Simple wait-type blocks: each states its own wait totals, with a clean concept + fix.
            "RESOURCE_SEMAPHORE" or "RESOURCE_SEMAPHORE_QUERY_COMPILE" or "WRITELOG" or "HADR_SYNC_COMMIT"
                or "LATCH_EX" or "LATCH_SH" or "PAGELATCH_EX" or "PAGELATCH_UP" or "LCK_M_S" or "LCK_M_IS"
                => ComposeWaitByKey(rootFactKey, factsByKey),
            // #3538 A9: schema-modification waits name the long-running job when RUNNING_JOBS fired — the
            // same window the SCH_M → RUNNING_JOBS graph edge folds the two cards into one incident.
            "SCH_M" => ComposeSchM(factsByKey),
            // Query-level: state the cost-swing ratio / regression factor / tempdb driver the engine
            // measured, and permute on the discriminating flag (forced-plan-failing, dominant consumer).
            "PARAMETER_SENSITIVITY" => ComposeParameterSensitivity(factsByKey),
            "PLAN_REGRESSION" => ComposePlanRegression(factsByKey),
            "TEMPDB_USAGE" => ComposeTempdbUsage(factsByKey),
            // I/O latency + general lock contention.
            "IO_READ_LATENCY_MS" => ComposeIoLatency(factsByKey, "IO_READ_LATENCY_MS"),
            "IO_WRITE_LATENCY_MS" => ComposeIoLatency(factsByKey, "IO_WRITE_LATENCY_MS"),
            "LCK" => ComposeLck(factsByKey),
            // Configuration, capacity, and server-health blocks: state the counts/values collected.
            "DB_CONFIG" => ComposeDbConfig(factsByKey),
            "FILE_AUTOGROWTH_PERCENT" => ComposeFileAutogrowth(factsByKey),
            "MISSING_INDEX" => ComposeMissingIndex(factsByKey),
            "PLAN_WARNING" => ComposePlanWarning(factsByKey),
            "RUNNING_JOBS" => ComposeRunningJobs(factsByKey),
            "DISK_SPACE" => ComposeDiskSpace(factsByKey),
            "CONFIG_IFI_DISABLED" or "CONFIG_LPIM_DISABLED" or "SERVER_MEMORY_DUMPS"
                => ComposeServerHealth(rootFactKey, factsByKey),
            // #3653 A10 (Q2): a server configuration change observed inside the window, with the ±4 h
            // before/after compare frozen on the fact — states the setting, old → new, when it was first
            // observed, and which metrics moved beyond their band (or that none did).
            ConfigChangeAttribution.FactKey => ComposeConfigChanged(factsByKey),
            // Anomaly facts: state the observed value, how many σ above the hour-of-week baseline, and
            // the baseline itself — "X is Nσ above its M baseline for this time of week".
            "ANOMALY_CPU_SPIKE" => ComposeAnomaly(factsByKey, "ANOMALY_CPU_SPIKE", "peak_cpu", "SQL CPU", Pct, "A brief CPU burst well above what this hour-of-week normally sees.", "avg_cpu_in_window"),
            "ANOMALY_READ_LATENCY" => ComposeAnomaly(factsByKey, "ANOMALY_READ_LATENCY", "current_latency_ms", "Read latency", Ms, "Storage reads ran slower than this hour-of-week normally does.", "avg_latency_ms"),
            "ANOMALY_WRITE_LATENCY" => ComposeAnomaly(factsByKey, "ANOMALY_WRITE_LATENCY", "current_latency_ms", "Write latency", Ms, "Storage writes ran slower than this hour-of-week normally does.", "avg_latency_ms"),
            "ANOMALY_BATCH_REQUESTS" => ComposeAnomaly(factsByKey, "ANOMALY_BATCH_REQUESTS", "peak_batch_requests", "Batch requests/sec", Rate, "Throughput jumped well above the usual level for this hour-of-week.", "avg_batch_requests"),
            "ANOMALY_SESSION_SPIKE" => ComposeAnomaly(factsByKey, "ANOMALY_SESSION_SPIKE", "peak_connections", "Connection count", Num, "Far more sessions connected than this hour-of-week normally sees — often a connection-pool leak or a retry storm.", "avg_connections"),
            "ANOMALY_QUERY_DURATION" => ComposeAnomaly(factsByKey, "ANOMALY_QUERY_DURATION", "peak_total_elapsed_us", "Total query duration", Micros, "Queries ran far longer in aggregate than this hour-of-week normally does.", "avg_total_elapsed_us"),
            "ANOMALY_MEMORY_PRESSURE" => ComposeAnomalyMemoryPressure(factsByKey),
            "ANOMALY_WAIT_PROFILE" => ComposeAnomalyWaitProfile(factsByKey),
            "ANOMALY_BLOCKING_SPIKE" => ComposeAnomalyRatio(factsByKey, "ANOMALY_BLOCKING_SPIKE", "blocking event"),
            "ANOMALY_DEADLOCK_SPIKE" => ComposeAnomalyRatio(factsByKey, "ANOMALY_DEADLOCK_SPIKE", "deadlock"),
            "ANOMALY_OBJECT_GROWTH" => ComposeAnomalyObjectGrowth(factsByKey),
            "ANOMALY_OBJECT_CONTENTION" => ComposeAnomalyObjectContention(factsByKey),
            // Blocking family: state the chain depth / event counts / deadlock counts the engine
            // actually collected, permute on the lead-blocker context (sleeping vs active head), and
            // append the RCSI count from the co-fired DB_CONFIG fact.
            "BLOCKING_CHAIN" => ComposeBlockingChain(factsByKey),
            "BLOCKING_EVENTS" => ComposeBlockingEvents(factsByKey),
            "DEADLOCKS" => ComposeDeadlocks(factsByKey),
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
            if (advice is null)
                continue;
            advice = WithNamedHops(advice, story, byKey);
            advice = WithSideLeaves(advice, story);
            story.StoryText = SerializeForStoryText(advice);
        }
    }

    /// <summary>
    /// #3691: appends one sentence per identity-bearing hop the engine recorded on the story
    /// (<see cref="AnalysisStory.NamedHops"/>) to the root's INVESTIGATION — "Behind it: `PG_IDLE_IN_TRANSACTION`
    /// (severity 1.20) — billing-worker as billing idle in transaction for 15 min …" — so the operator reading the
    /// root card learns WHO was behind the chain without opening a card the traversal never made. Investigation,
    /// not Remediation: the hop's name is where to look; the hop's own remediation stays the hop's (the reverse
    /// links the symptom composers already carry — LinkedJobClause, the PostgreSQL co-fire clauses — keep saying
    /// what to do about the sibling in their own family's words). Each hop's composed headline is read from the
    /// FULL fact set here, the one place both are in scope, the same way the root's values are. A story with no
    /// named hops returns the block untouched — the byte-identity pin for every chain this does not concern.
    /// </summary>
    private static AdviceBlock WithNamedHops(AdviceBlock advice, AnalysisStory story, IReadOnlyDictionary<string, Fact> byKey)
    {
        if (story.NamedHops is null || story.NamedHops.Count == 0)
            return advice;
        var investigation = new StringBuilder(advice.Investigation ?? string.Empty);
        foreach (var hop in story.NamedHops)
        {
            var sentence = FactIdentity.Sentence(hop, Compose(hop.Key, byKey)?.Headline);
            if (investigation.Length == 0)
                sentence = sentence.TrimStart();
            investigation.Append(sentence);
        }
        return advice with { Investigation = investigation.ToString() };
    }

    /// <summary>
    /// #3691 (lane 42): appends the ONE sentence naming the config lever(s) hanging off this story
    /// (<see cref="AnalysisStory.SideLeafKeys"/>) to the root's INVESTIGATION, after the named-hop sentences and by
    /// the same rule — the levers are where to look next, and the lever's own card (the payload's
    /// <c>side_leaves</c>) carries its value and its remediation. Before this, the lever rooted a card of its own
    /// beside the incident; now the walk consumes it, so the root's card is the only place that can point at it and
    /// this sentence is that pointer. Remediation is untouched: the lever's fix is the lever's, in its own family's
    /// words. A story with no side leaves returns the block untouched — the byte-identity arm for every chain this
    /// does not concern, which is nearly all of them.
    /// </summary>
    private static AdviceBlock WithSideLeaves(AdviceBlock advice, AnalysisStory story)
    {
        var sentence = StorySideLeaves.Sentence(story.SideLeafKeys);
        if (sentence is null)
            return advice;
        var investigation = advice.Investigation ?? string.Empty;
        return advice with
        {
            Investigation = investigation.Length == 0 ? sentence.TrimStart() : investigation + sentence
        };
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
            "back); otherwise fix the slow operation under the held lock. Then guard parallelism. " +
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

        sb.Append(" — the attached top parallel queries rank them; check whether each genuinely benefits from the parallelism it is getting.");

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

    /// <summary>
    /// CONFIG_CHANGED composed (#3653 A10, Q2): the setting(s), old → new, when the change was FIRST
    /// OBSERVED, how much of the ±4 h compare exists yet, and the metrics that moved beyond their band —
    /// or the sentence that none did, which is the finding's other value. Every number is read off the
    /// fact <see cref="ConfigChangeAttribution"/> built; the setting names ride in ObjectName and the
    /// values in the doubles-only metadata under the keys that class spells.
    ///
    /// <para><b>The words are chosen against three lies.</b> (1) "changed at" on a clock that only saw the
    /// value — the config snapshot runs on connect, so on the observation anchor the time on the fact is when
    /// the new value was first SEEN; the prose says "first observed" and states the span since the previous
    /// snapshot, and when that span exceeds the before-window it says the before half may already hold the
    /// new value. When the fact is anchored on the default trace instead
    /// (<see cref="ConfigChangeAttribution.MetaAnchorClock"/> = <see cref="ConfigChangeAttribution.AnchorSourceDefaultTrace"/>,
    /// #3740), "changed at" is TRUE — the sp_configure line was stamped at the instant of the change — so the
    /// prose says it, names the source, states how much later the snapshot first saw it, and drops the span
    /// sentences, whose premise (the change landed somewhere in a span) no longer holds; a setting the trace
    /// did not date on a multi-setting event is named as riding the event's anchor. (2) A full four hours after
    /// — when the change is younger than that, the after half is a partial and the prose says how much of
    /// it exists and that later passes complete it. (3) Cause — a before/after is not a causal test; the
    /// remediation says so in the compare tool's own terms and offers the reads that would firm it up.
    /// A non-dynamic setting whose configured value moved while in-use did not gets its own sentence:
    /// the engine is still running the old value, so nothing should have moved yet.</para>
    ///
    /// <para><b>Slice two: three families, one grammar.</b> Each change on the fact is decoded from its
    /// ObjectName segment (<see cref="ConfigChangeAttribution.Changes"/>) and described in its family's own
    /// words — a server setting exactly as above (that prose is pinned and unchanged), a database option as
    /// "<c>`AdventureWorks` recovery_model FULL → SIMPLE</c>" (set to / cleared when one side was NULL), a
    /// trace flag as "<c>trace flag 4199 enabled (GLOBAL)</c>" / "disabled (was GLOBAL)" / "scope changed (now
    /// …)". The headline names the family ("Database configuration changed", "Trace flag changed") or, for an
    /// event folded across families at one connect, "Configuration changed: N configuration changes observed
    /// together" — the folded card exists precisely because the data cannot say which of them moved a metric,
    /// and the prose does not pretend otherwise. The observation, span, after-half and compare sentences are
    /// the same words for every family (the snapshot cadence and the compare are the same). The remediation
    /// points at the history tool(s) of the families present and adds the grading tool that exists for each:
    /// <c>audit_config</c> for a server setting, the standing <c>DB_CONFIG</c> advisory for a database option,
    /// <c>get_trace_flags</c> for what is on now.</para>
    ///
    /// <para>Falls back to the static block when the fact is absent (a finding persisted before the
    /// composer existed, or a story whose root the lookup cannot find).</para>
    /// </summary>
    private static AdviceBlock ComposeConfigChanged(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey[ConfigChangeAttribution.FactKey];
        if (!facts.TryGetValue(ConfigChangeAttribution.FactKey, out var fact))
            return fallback;

        var changes = ConfigChangeAttribution.Changes(fact);
        if (changes.Count == 0)
            return fallback;

        var families = changes.Aggregate((ConfigChangeAttribution.ChangeFamily)0, (acc, c) => acc | c.Family);
        var hasServer = (families & ConfigChangeAttribution.ChangeFamily.ServerConfig) != 0;
        var hasDatabase = (families & ConfigChangeAttribution.ChangeFamily.DatabaseConfig) != 0;
        var hasTraceFlags = (families & ConfigChangeAttribution.ChangeFamily.TraceFlags) != 0;
        var mixed = (hasServer ? 1 : 0) + (hasDatabase ? 1 : 0) + (hasTraceFlags ? 1 : 0) > 1;

        // ── the change itself: `name` old → new, per setting, in its family's words ──
        var described = new List<string>(changes.Count);
        var pendingRestart = new List<string>();
        foreach (var change in changes)
        {
            var name = change.Name;
            switch (change.Family)
            {
                case ConfigChangeAttribution.ChangeFamily.DatabaseConfig:
                {
                    var db = $"`{change.DatabaseName}`";
                    described.Add(change.OldText is null && change.NewText is not null
                        ? $"{db} {change.Setting} set to {change.NewText}"
                        : change.OldText is not null && change.NewText is null
                            ? $"{db} {change.Setting} cleared (was {change.OldText})"
                            : $"{db} {change.Setting} {change.OldText ?? "(none)"} → {change.NewText ?? "(none)"}");
                    break;
                }
                case ConfigChangeAttribution.ChangeFamily.TraceFlags:
                {
                    described.Add(change.ChangeType switch
                    {
                        "enabled" => $"trace flag {change.Setting} enabled ({change.Scope})",
                        "disabled" => $"trace flag {change.Setting} disabled (was {change.Scope})",
                        "modified" => $"trace flag {change.Setting} scope changed (now {change.Scope})",
                        _ => $"trace flag {change.Setting} changed",
                    });
                    break;
                }
                default:
                {
                    var oldInUse = fact.Metadata.TryGetValue(ConfigChangeAttribution.OldInUseKey(name), out var oi) ? oi : (double?)null;
                    var newInUse = fact.Metadata.TryGetValue(ConfigChangeAttribution.NewInUseKey(name), out var ni) ? ni : (double?)null;
                    var oldCfg = fact.Metadata.TryGetValue(ConfigChangeAttribution.OldConfiguredKey(name), out var oc) ? oc : (double?)null;
                    var newCfg = fact.Metadata.TryGetValue(ConfigChangeAttribution.NewConfiguredKey(name), out var nc) ? nc : (double?)null;
                    var restart = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.RequiresRestartKey(name)) > 0;

                    if (oldInUse is not null && newInUse is not null && oldInUse != newInUse)
                        described.Add($"`{name}` {Num(oldInUse.Value)} → {Num(newInUse.Value)}");
                    else if (oldCfg is not null && newCfg is not null && oldCfg != newCfg)
                    {
                        described.Add($"`{name}` configured {Num(oldCfg.Value)} → {Num(newCfg.Value)}" + (restart ? " (in use unchanged until restart)" : " (in use unchanged)"));
                        if (restart) pendingRestart.Add(name);
                    }
                    else
                        described.Add($"`{name}` changed");
                    break;
                }
            }
        }
        var changeList = string.Join(", ", described);

        // ── when, and how honest "when" is ──
        var changeUnix = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaChangeTimeUnix);
        var changeAt = changeUnix > 0 ? DateTimeOffset.FromUnixTimeSeconds((long)changeUnix).UtcDateTime : (DateTime?)null;
        var traceAnchored = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaAnchorClock) == ConfigChangeAttribution.AnchorSourceDefaultTrace;
        var gapHours = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaObservationGapHours);
        var beforeHours = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaBeforeHours);
        if (beforeHours <= 0) beforeHours = ConfigChangeAttribution.CompareWindowHours;

        var inv = new StringBuilder();
        inv.Append(changeList);
        if (traceAnchored && changeAt is not null)
        {
            /* #3740: the default trace saw the change happen, so "changed at" is the truth and the snapshot's
               lateness is a fact about this card's arrival, not about the compare's windows. */
            inv.Append($" — changed at {changeAt.Value:yyyy-MM-dd HH:mm} UTC (default trace: the sp_configure line, msg {ConfigChangeAttribution.ReconfigureMessageNumber}).");
            var lagHours = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaObservedLagHours);
            var observedUnix = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaObservedAtUnix);
            var observedAt = observedUnix > 0 ? DateTimeOffset.FromUnixTimeSeconds((long)observedUnix).UtcDateTime : (DateTime?)null;
            if (observedAt is not null)
            {
                inv.Append(lagHours >= 0.1
                    ? $" The configuration snapshot (taken on connect) first observed it {lagHours:0.#} h later, at {observedAt.Value:yyyy-MM-dd HH:mm} UTC; the compare below is anchored on the trace's time, not on that observation."
                    : $" The configuration snapshot first observed it at {observedAt.Value:yyyy-MM-dd HH:mm} UTC, within minutes of the change.");
            }
            var undated = changes.Where(c => !fact.Metadata.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(c.Name))).ToList();
            if (undated.Count > 0 && undated.Count < changes.Count)
                inv.Append($" The trace dated {changes.Count - undated.Count} of the {changes.Count} settings; {string.Join(", ", undated.Select(c => $"`{c.Name}`"))} had no matching line in the span between snapshots and shares this anchor.");
        }
        else
        {
            inv.Append(changeAt is null
                ? " — first observed by the configuration snapshot this window."
                : $" — first observed by the configuration snapshot at {changeAt.Value:yyyy-MM-dd HH:mm} UTC.");
            if (gapHours > 0)
            {
                inv.Append($" The snapshot runs on connect, so the change itself landed somewhere in the {gapHours:0.#} h since the previous snapshot; this finding's time is when it was seen, not when it was made.");
                if (gapHours > beforeHours)
                    inv.Append($" That span is longer than the {beforeHours:0} h before-window, so the \"before\" half may already reflect the new value and a null result here does not mean the change had no effect.");
            }
        }

        // ── the compare ──
        var unavailable = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaCompareUnavailable) > 0;
        var afterHours = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaAfterHoursObserved);
        var afterClamped = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaAfterWindowClamped) > 0;
        var earlier = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaEarlierEventsInWindow);
        var moved = ConfigChangeAttribution.MovedKeys(fact);
        var stable = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaStable);
        var omitted = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.MetaMovedKeysOmitted);

        string verdict;
        if (unavailable)
        {
            verdict = "The before/after compare could not run this pass (the store read failed on both sides), so this card records the change and nothing about its effect; the next pass retries while the change stays inside the window.";
        }
        else
        {
            var afterClause = afterClamped
                ? $"the {afterHours:0.#} h after it that exist so far — the after half is still filling in, and later passes complete it"
                : $"the {afterHours:0.#} h after it";
            inv.Append($" The engine compared the {beforeHours:0} h before the {(traceAnchored ? "change" : "observation")} with {afterClause}.");

            if (pendingRestart.Count == changes.Count)
            {
                verdict = "Nothing should have moved yet: the engine is still running the old value until the next restart, and the compare is a control for that pass, not a verdict on this one.";
            }
            else if (moved.Count == 0)
            {
                verdict = "No metric in the compare moved beyond its dispersion band — that is the finding: at this window's grain the change had no measurable effect.";
            }
            else
            {
                var parts = new List<string>(moved.Count);
                foreach (var key in moved)
                {
                    var worse = fact.Metadata.GetValueOrDefault(ConfigChangeAttribution.StatusKey(key)) > 0;
                    var sigma = fact.Metadata.TryGetValue(ConfigChangeAttribution.DeltaSigmaKey(key), out var ds) ? ds : (double?)null;
                    var rel = fact.Metadata.TryGetValue(ConfigChangeAttribution.RelativeMoveKey(key), out var rm) ? rm : (double?)null;
                    var magnitude = sigma is not null
                        ? $"{(sigma.Value >= 0 ? "+" : string.Empty)}{sigma.Value:0.#}σ"
                        : rel is not null && rel.Value < 1.0
                            ? $"{(worse ? "+" : "−")}{rel.Value * 100:0}%"
                            : worse ? "appeared" : "resolved";
                    parts.Add($"{key} {magnitude} ({(worse ? "worse" : "better")})");
                }
                verdict = $"Moved beyond its band after the change: {string.Join("; ", parts)}"
                          + (omitted > 0 ? $"; and {Plural(omitted, "more key")} (see the fact's metadata)" : string.Empty)
                          + (stable > 0 ? $". {Plural(stable, "other compared key")} stayed inside band." : ".");
            }
        }
        inv.Append(' ').Append(verdict);
        if (earlier > 0)
            inv.Append($" {Plural(earlier, "earlier configuration change")} also sat inside this pass's window and is not compared here — the passes that ran while it was the most recent change carried its own compare.");

        // ── headline: the family names itself; a folded event says it was observed together ──
        var family = mixed ? "Configuration changed"
            : hasDatabase ? "Database configuration changed"
            : hasTraceFlags ? (changes.Count == 1 ? "Trace flag changed" : "Trace flags changed")
            : "Server configuration changed";
        var subject = changes.Count == 1 ? described[0]
            : mixed ? $"{Plural(changes.Count, "configuration change")} observed together"
            : hasDatabase ? $"{Plural(changes.Count, "database setting")} changed together"
            : hasTraceFlags ? $"{Plural(changes.Count, "trace flag")} changed together"
            : $"{Plural(changes.Count, "server setting")} changed together";
        var headline = unavailable
            ? $"{family}: {subject} — effect not yet compared"
            : pendingRestart.Count == changes.Count
                ? $"{family}: {subject} — takes effect at the next restart"
                : moved.Count == 0
                    ? $"{family}: {subject} — nothing moved beyond band in the ±{beforeHours:0} h compare"
                    : $"{family}: {subject} — {Plural(moved.Count, "metric")} moved beyond band after it";

        // ── remediation: the history read(s) of the families present, the compare, and each family's grader ──
        var historyTools = new List<string>(3);
        if (hasServer) historyTools.Add("`get_server_config_changes`");
        if (hasDatabase) historyTools.Add("`get_database_config_changes`");
        if (hasTraceFlags) historyTools.Add("`get_trace_flag_changes`");
        var graders = new List<string>(3);
        if (hasServer) graders.Add("`audit_config` grades the new value against guidance");
        if (hasDatabase) graders.Add("the standing `DB_CONFIG` advisory grades the database options this engine has an opinion on (auto-shrink, auto-close, RCSI, auto-stats, page verify)");
        if (hasTraceFlags) graders.Add("`get_trace_flags` lists what is on now");
        var rem =
            "This is an attribution, not an accusation: one window against one window cannot show that a change CAUSED anything " +
            "(DB time on an unchanged server routinely varies severalfold day to day), and a metric that moved may have moved for " +
            "reasons of its own. Read it as a pointer. If something moved the wrong way and stays moved on later passes, the " +
            $"{(mixed ? "changes are" : "setting is")} the first suspect: {string.Join(" / ", historyTools)} {(historyTools.Count == 1 ? "lists" : "list")} the change with its old and new values, " +
            "`compare_analysis` reruns the compare over any pair of windows (a wider one, or the same hour yesterday), and " +
            $"{string.Join("; ", graders)}. If nothing moved, there is nothing to do; the card recurs " +
            "on each pass while the change sits inside the analysis window and stops on its own.";

        return fallback with { Headline = headline, Investigation = inv.ToString(), Remediation = rem };
    }

    /// <summary>A fact's metadata value, or null when the fact or the metadata key is absent.</summary>
    private static double? FactMeta(IReadOnlyDictionary<string, Fact> facts, string key, string metaKey) =>
        facts.TryGetValue(key, out var f) && f.Metadata.TryGetValue(metaKey, out var v) ? v : (double?)null;

    /// <summary>True when a related fact actually fired (present and scored above 0) this window.</summary>
    private static bool Fired(IReadOnlyDictionary<string, Fact> facts, string key) =>
        facts.TryGetValue(key, out var f) && f.Severity > 0;

    /// <summary>
    /// Names the query-level findings that ACTUALLY co-fired this window (plan regression, parameter
    /// sensitivity, missing indexes, plan warnings), so a CPU/wait remediation points at the real
    /// causes the engine found instead of a speculative list. Empty when none co-fired.
    /// </summary>
    private static string QueryCauseClause(IReadOnlyDictionary<string, Fact> facts)
    {
        var bits = new List<string>();
        if (Fired(facts, "PLAN_REGRESSION"))
            bits.Add("a plan regression — force the historically faster plan (see that finding)");
        if (Fired(facts, "PARAMETER_SENSITIVITY"))
            bits.Add("parameter sensitivity — a plan is far more expensive for some parameter values (that finding has the figures)");
        if (Fired(facts, "MISSING_INDEX"))
            bits.Add("missing-index requests — the missing-index card lists the optimizer's suggestions from this window's top plans as corroboration, with its caveat");
        if (Fired(facts, "PLAN_WARNING"))
            bits.Add("plan warnings — check those plans for implicit conversions and spills");
        return bits.Count == 0 ? string.Empty : " Co-fired this window: " + string.Join("; ", bits) + ".";
    }

    /// <summary>Human-readable duration from milliseconds: "850 ms", "1.4 s", "3.2 min".</summary>
    private static string Duration(double ms) =>
        ms < 1000 ? $"{ms:N0} ms"
        : ms < 60000 ? $"{ms / 1000.0:0.#} s"
        : $"{ms / 60000.0:0.#} min";

    /// <summary>"{n} {noun}" with a plural "s" unless n == 1, e.g. "1 deadlock" / "47 deadlocks".</summary>
    private static string Plural(double n, string noun) =>
        $"{n:N0} {noun}{(Math.Abs(n - 1) < 0.5 ? string.Empty : "s")}";

    // Anomaly value formatters (passed to ComposeAnomaly as the observed/baseline renderer).
    private static string Pct(double v) => $"{v:0.#}%";
    private static string Ms(double v) => $"{v:0.#} ms";
    private static string Rate(double v) => $"{v:N0}/sec";
    private static string Num(double v) => $"{v:N0}";
    private static string Micros(double v) => Duration(v / 1000.0);

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
    /// The "N databases have RCSI off" sentence, read from the co-fired DB_CONFIG fact's
    /// `rcsi_off_count`. Empty when DB_CONFIG did not co-fire or no database has RCSI off (nothing
    /// concrete to state). Shared by the blocking / deadlock composers.
    /// </summary>
    private static string RcsiSentence(IReadOnlyDictionary<string, Fact> facts)
    {
        var rcsiOff = FactMeta(facts, "DB_CONFIG", "rcsi_off_count");
        if (rcsiOff is not > 0)
            return string.Empty;
        var n = (long)rcsiOff.Value;
        return n == 1
            ? "One database on this server currently has RCSI off — enable it there if its blocking is readers blocked by writers."
            : $"{n:N0} databases on this server currently have RCSI off — enable it on the ones whose blocking is readers blocked by writers.";
    }

    /// <summary>
    /// BLOCKING_CHAIN composed from the reconstructed-chain metadata: states the worst chain's depth,
    /// victim count, longest wait, and total chains, and PERMUTES on whether the head was sleeping
    /// (abandoned-transaction signature) vs actively running (one slow operation under a held lock) —
    /// the lead-blocker context that changes the fix. Falls back to the static block when the chain
    /// metadata was not collected this window.
    /// </summary>
    private static AdviceBlock ComposeBlockingChain(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["BLOCKING_CHAIN"];
        var depth = FactMeta(facts, "BLOCKING_CHAIN", "worst_chain_depth");
        if (depth is null)
            return fallback;

        var victims = FactMeta(facts, "BLOCKING_CHAIN", "worst_chain_victim_count");
        var sleeping = FactMeta(facts, "BLOCKING_CHAIN", "worst_apex_sleeping") is > 0;
        var maxWait = FactMeta(facts, "BLOCKING_CHAIN", "worst_chain_max_wait_ms");
        var totalChains = FactMeta(facts, "BLOCKING_CHAIN", "total_reconstructed_chains");

        var inv = new StringBuilder($"The worst blocking chain this window was {Plural(depth.Value, "session")} deep");
        if (victims is > 0)
            inv.Append($", with {Plural(victims.Value, "session")} stacked behind the head");
        inv.Append(sleeping
            ? ". The session at the head was sleeping with an open transaction — the abandoned-transaction signature: a BEGIN TRAN that never reached COMMIT or ROLLBACK, usually an error or client-timeout path that skips cleanup, so the lock stays held while the connection does nothing."
            : ". The session at the head was actively running, so one slow operation was holding a lock the rest queued behind.");
        if (maxWait is > 0)
            inv.Append($" The longest waiter sat for {Duration(maxWait.Value)}.");
        if (totalChains is > 1)
            inv.Append($" {Plural(totalChains.Value, "separate chain")} formed across the window.");

        var rem = new StringBuilder(
            "This reports a window that has already passed, so the chain cleared long ago — a KILL now buys nothing; aim at the recurrence. ");
        rem.Append(sleeping
            ? "Because the head was sleeping, the fix is the code path that opens a BEGIN TRAN and never reaches COMMIT/ROLLBACK on an error or client-timeout path — and SET XACT_ABORT ON so an aborted batch rolls back automatically."
            : "Because the head was active, the fix is that one slow operation — find it and cut how long it holds locks: shorten the transaction and index it so it touches fewer rows.");
        var rcsi = RcsiSentence(facts);
        rem.Append(rcsi.Length > 0
            ? " " + rcsi
            : " Where the contention is readers blocked by writers, enabling RCSI on the database removes that class entirely.");

        return fallback with
        {
            Headline = $"A blocking chain {depth.Value:N0} deep formed — the session at the head held a resource the rest queued behind",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// BLOCKING_EVENTS composed: states the event count and rate, distinct head blockers, average and
    /// longest blocked wait, and how many were headed by a sleeping (abandoned) transaction, then the
    /// RCSI sentence. Falls back to the static block (still appending RCSI) when the event metadata
    /// was not collected.
    /// </summary>
    private static AdviceBlock ComposeBlockingEvents(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["BLOCKING_EVENTS"];
        var count = FactMeta(facts, "BLOCKING_EVENTS", "event_count");
        var rcsi = RcsiSentence(facts);
        if (count is null)
            return rcsi.Length == 0 ? fallback : fallback with { Remediation = fallback.Remediation + " " + rcsi };

        var perHour = FactMeta(facts, "BLOCKING_EVENTS", "events_per_hour");
        var heads = FactMeta(facts, "BLOCKING_EVENTS", "distinct_head_blockers");
        var avgWait = FactMeta(facts, "BLOCKING_EVENTS", "avg_wait_time_ms");
        var maxWait = FactMeta(facts, "BLOCKING_EVENTS", "max_wait_time_ms");
        var sleepers = FactMeta(facts, "BLOCKING_EVENTS", "sleeping_blocker_count");

        var inv = new StringBuilder($"{Plural(count.Value, "blocking event")} this window");
        if (perHour is > 0)
            inv.Append($", about {perHour.Value:0.#} per hour");
        inv.Append('.');
        if (heads is > 0)
            inv.Append($" {Plural(heads.Value, "distinct head blocker")}.");
        if (avgWait is > 0)
            inv.Append(maxWait is > 0
                ? $" Blocked sessions waited {Duration(avgWait.Value)} on average, up to {Duration(maxWait.Value)}."
                : $" Blocked sessions waited {Duration(avgWait.Value)} on average.");
        else if (maxWait is > 0)
            inv.Append($" The longest blocked wait was {Duration(maxWait.Value)}.");
        if (sleepers is > 0)
            inv.Append($" {sleepers.Value:N0} were headed by a sleeping (abandoned) transaction — a BEGIN TRAN left open on an error or client-timeout path, holding its locks while the connection does nothing.");

        var rem = new StringBuilder("This reports a past window, so aim at the recurrence rather than the individual events. ");
        rem.Append(sleepers is > 0
            ? "The sleeping head blockers point straight at abandoned transactions: fix the code path that leaves a BEGIN TRAN open, and SET XACT_ABORT ON so an aborted batch rolls back. "
            : "The usual driver is one slow operation holding a lock — find it and cut its duration: shorten the transaction and index it so it touches fewer rows. ");
        rem.Append(rcsi.Length > 0
            ? rcsi
            : "Where the contention is readers blocked by writers, enabling RCSI on the database removes that class entirely.");

        return fallback with
        {
            Headline = $"{Plural(count.Value, "blocking event")} this window — sessions stalled waiting on locks others held",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// DEADLOCKS composed: states the deadlock count and rate, then clean retrospective remediation,
    /// then the RCSI sentence. Falls back to the static block (still appending RCSI) when the count
    /// metadata was not collected.
    /// </summary>
    private static AdviceBlock ComposeDeadlocks(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["DEADLOCKS"];
        var count = FactMeta(facts, "DEADLOCKS", "deadlock_count");
        var rcsi = RcsiSentence(facts);
        if (count is null)
            return rcsi.Length == 0 ? fallback : fallback with { Remediation = fallback.Remediation + " " + rcsi };

        var perHour = FactMeta(facts, "DEADLOCKS", "deadlocks_per_hour");
        var inv = new StringBuilder($"{Plural(count.Value, "deadlock")} this window");
        if (perHour is > 0)
            inv.Append($", about {perHour.Value:0.#} per hour");
        inv.Append(". A deadlock is two sessions each holding a lock the other needs; SQL Server kills the cheaper one as the victim and rolls it back. This reports a window that has already passed — the deadlock graphs were captured at the time, so the work now is stopping the recurrence.");

        var rem = new StringBuilder(
            "Deadlocks are almost always a lock-ordering or duration problem: have the participating transactions touch objects in a consistent order, keep them short, and add the indexes that let their queries lock fewer rows. ");
        rem.Append(rcsi.Length > 0
            ? rcsi
            : "Where the deadlock is a reader against a writer, RCSI on the database removes that class entirely.");

        return fallback with
        {
            Headline = $"{Plural(count.Value, "deadlock")} this window — sessions killed and rolled back over circular lock waits",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// SOS_SCHEDULER_YIELD composed: states the total yield wait, the average per yield, and the
    /// SIGNAL-WAIT share — time workers spent ready to run but waiting for a scheduler, the collected
    /// proxy for runnable-queue depth that separates genuine CPU oversubscription from CPUs merely
    /// kept busy. Falls back to the static block when the wait metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeSos(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["SOS_SCHEDULER_YIELD"];
        var waitMs = FactMeta(facts, "SOS_SCHEDULER_YIELD", "wait_time_ms");
        if (waitMs is null || waitMs.Value <= 0)
            return fallback;

        var avg = FactMeta(facts, "SOS_SCHEDULER_YIELD", "avg_ms_per_wait");
        var signal = FactMeta(facts, "SOS_SCHEDULER_YIELD", "signal_wait_time_ms");
        var signalPct = signal is not null ? signal.Value / waitMs.Value * 100.0 : (double?)null;

        var inv = new StringBuilder($"Workers spent {Duration(waitMs.Value)} yielding the scheduler this window");
        if (avg is > 0)
            inv.Append($", {Duration(avg.Value)} per yield on average");
        inv.Append(". A worker logs SOS_SCHEDULER_YIELD when it uses its full 4 ms quantum and voluntarily yields, so this is sustained CPU-bound work.");
        if (signalPct is not null)
            inv.Append($" Of that, {Duration(signal!.Value)} ({signalPct:0.#}%) was signal wait — time workers were ready to run but waiting for a scheduler to free up. The larger that share, the more this is genuine CPU pressure (more demand than the schedulers can serve) rather than CPUs simply kept busy.");
        inv.Append(" The top CPU-consuming queries for the window are attached — those are what burned the CPU.");

        var rem = new StringBuilder("Tune the attached top queries — the wait points straight at the queries burning CPU, and cutting that work is cheaper than adding cores.");
        rem.Append(QueryCauseClause(facts));
        if (signalPct is not null)
            rem.Append(" Use the signal-wait share above to size the cores question: the higher it was, the more adding cores is justified once tuning is exhausted; a low share means the CPUs were busy but not oversubscribed, so stay on the queries.");
        if (Fired(facts, "CXPACKET"))
            rem.Append(" CXPACKET co-fired this window, so part of the CPU is parallelism overhead — raise cost threshold for parallelism to 50 and cap MAXDOP at the per-NUMA-node processor count (no more than 8).");

        return fallback with { Investigation = inv.ToString(), Remediation = rem.ToString() };
    }

    /// <summary>
    /// CPU_SQL_PERCENT composed: states SQL Server's own average/peak CPU and the other-process share,
    /// and permutes on which is the larger consumer (rule out external load before tuning SQL). Falls
    /// back to the static block when the CPU metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeCpuSqlPercent(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CPU_SQL_PERCENT"];
        var avgSql = FactMeta(facts, "CPU_SQL_PERCENT", "avg_sql_cpu");
        if (avgSql is null)
            return fallback;

        var maxSql = FactMeta(facts, "CPU_SQL_PERCENT", "max_sql_cpu");
        var avgOther = FactMeta(facts, "CPU_SQL_PERCENT", "avg_other_cpu");
        var maxOther = FactMeta(facts, "CPU_SQL_PERCENT", "max_other_cpu");

        var inv = new StringBuilder($"SQL Server's own CPU averaged {avgSql.Value:0.#}%");
        if (maxSql is > 0)
            inv.Append($", peaking at {maxSql.Value:0.#}%");
        inv.Append(" across the window");
        if (avgOther is not null)
            inv.Append($", while other processes on the host averaged {avgOther.Value:0.#}%{(maxOther is > 0 ? $" (peak {maxOther.Value:0.#}%)" : string.Empty)}");
        inv.Append(". ");
        inv.Append(avgOther is not null && avgOther.Value > avgSql.Value
            ? "Other processes were the larger consumer — antivirus, another instance, or another tenant on the VM is worth ruling out before tuning SQL."
            : "SQL Server is the consumer here, so the work is in the queries, not elsewhere on the box.");
        inv.Append(" The top CPU-consuming queries for the window are attached.");

        var rem = "Tune the attached top queries — almost always cheaper than adding cores; their plans show what to fix." + QueryCauseClause(facts);

        return fallback with
        {
            Headline = $"SQL Server CPU averaged {avgSql.Value:0.#}% this window — the workload is consuming real CPU, not waiting on something else",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// CPU_SPIKE composed: states the peak CPU and the window average it spiked above (a burst, not
    /// sustained saturation). Falls back to the static block when the CPU metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeCpuSpike(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CPU_SPIKE"];
        var maxSql = FactMeta(facts, "CPU_SPIKE", "max_sql_cpu");
        if (maxSql is null)
            return fallback;

        var avgSql = FactMeta(facts, "CPU_SPIKE", "avg_sql_cpu");
        var inv = new StringBuilder($"SQL CPU spiked to {maxSql.Value:0.#}%");
        if (avgSql is not null)
            inv.Append($" against a window average of {avgSql.Value:0.#}% — the box has headroom most of the time but something briefly burned it");
        inv.Append(". The sessions active at the peak are attached.");

        var rem = new StringBuilder();
        if (Fired(facts, "PLAN_REGRESSION"))
            rem.Append("A plan regression co-fired — the engine attaches the historically faster plan and a ready-to-run force statement; forcing it is the fast fix while you address why the worse plan got chosen.");
        else if (Fired(facts, "PARAMETER_SENSITIVITY"))
            rem.Append("Parameter sensitivity co-fired — do NOT force a plan (it locks in the wrong shape for the other parameter values); use OPTION (RECOMPILE) on the affected statement, or branch the procedure by parameter.");
        else
            rem.Append("Neither a plan regression nor parameter sensitivity fired this window, so the burst is most likely ad-hoc or scheduled work — the sessions active at the peak are attached; Resource Governor or moving that work off-peak is the durable fix.");
        if (Fired(facts, "MISSING_INDEX"))
            rem.Append(" Missing-index requests co-fired — the missing-index card lists the optimizer's suggestions from this window's top plans; they corroborate this measured spike rather than diagnose it, and each carries its caveat.");

        return fallback with
        {
            Headline = $"CPU spiked to {maxSql.Value:0.#}% — a burst well above the window average, not sustained saturation",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// The "these waits totaled X across N waits, Y each on average" sentence shared by the simple
    /// wait-type composers. Empty when the wait-time metadata is absent.
    /// </summary>
    private static string WaitNumbers(IReadOnlyDictionary<string, Fact> facts, string key, string label)
    {
        var waitMs = FactMeta(facts, key, "wait_time_ms");
        if (waitMs is null || waitMs.Value <= 0)
            return string.Empty;
        var count = FactMeta(facts, key, "waiting_tasks_count");
        var avg = FactMeta(facts, key, "avg_ms_per_wait");
        var sb = new StringBuilder($"{label} totaled {Duration(waitMs.Value)} this window");
        if (count is > 0)
            sb.Append($" across {Plural(count.Value, "wait")}");
        if (avg is > 0)
            sb.Append($", {Duration(avg.Value)} each on average");
        sb.Append(". ");
        return sb.ToString();
    }

    /// <summary>
    /// A simple wait-type block composed: the WaitNumbers sentence followed by a clean (tool-name-free)
    /// concept body and remediation supplied by the caller. Falls back to the static block when the
    /// wait metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeSimpleWait(IReadOnlyDictionary<string, Fact> facts, string key, string label, string headline, string conceptBody, string remediation)
    {
        var numbers = WaitNumbers(facts, key, label);
        if (numbers.Length == 0)
            return _byKey[key];
        return _byKey[key] with { Headline = headline, Investigation = numbers + conceptBody, Remediation = remediation };
    }

    /// <summary>
    /// Per-key clean text for the simple wait-type blocks, routed through ComposeSimpleWait so each
    /// states its own wait totals. Bodies are tool-name-free and preserve the audited corrections
    /// (range locks → SERIALIZABLE only; #4149 moved last-page-insert / OPTIMIZE_FOR_SEQUENTIAL_KEY OFF
    /// LATCH_EX — that guidance belongs to PAGELATCH_EX, a page latch, not a non-buffer LATCH_EX).
    /// </summary>
    private static AdviceBlock ComposeWaitByKey(string key, IReadOnlyDictionary<string, Fact> facts) => key switch
    {
        "RESOURCE_SEMAPHORE" => ComposeSimpleWait(facts, key, "Memory-grant waits",
            "Queries are queuing for memory grants — RESOURCE_SEMAPHORE",
            "RESOURCE_SEMAPHORE means queries are waiting for a workspace-memory grant before they can run: the grants already in flight are large enough that new requests queue behind them. The usual driver is over-sized grants from bad cardinality estimates, not a genuine shortage of memory.",
            "Cut the grant sizes rather than just adding memory: fix the cardinality estimates driving them — update statistics and add the indexes that let the optimizer estimate rows accurately — and find the queries asking for the largest grants. If max server memory is capped well below physical RAM, raising it gives the pool more workspace. Resource Governor can cap per-request grants when one workload is the aggressor."),
        "RESOURCE_SEMAPHORE_QUERY_COMPILE" => ComposeSimpleWait(facts, key, "Compile-memory waits",
            "Queries are queuing for compile memory — RESOURCE_SEMAPHORE_QUERY_COMPILE",
            "This is memory pressure during query COMPILATION, not execution — too many concurrent compilations competing for compile memory, typically a flood of unparameterized ad-hoc queries each compiling its own plan.",
            "Parameterize the ad-hoc queries so they reuse cached plans instead of compiling fresh, enable 'optimize for ad hoc workloads' so one-off plans stop bloating the cache, and address whatever generates the compile storm (an ORM emitting literal values, or unnecessary recompiles)."),
        // #3538 A9: the WRITELOG → RUNNING_JOBS edge's prose — a fully logged rebuild or reload while a job
        // runs long is the log-flush driver, so the job is named beside the storage advice.
        "WRITELOG" => ComposeSimpleWait(facts, key, "Log-flush waits",
            "Transaction log flushes are slow — WRITELOG",
            "WRITELOG is time spent waiting for the transaction log to harden to disk on commit. Sustained WRITELOG points at slow log storage or a commit-heavy workload doing many tiny transactions, each forcing its own flush.",
            "Put the transaction log on the fastest storage available — write latency matters far more than throughput here — and batch tiny transactions where the application allows, so fewer, larger commits flush less often. Confirm the log is not autogrowing in small increments under load." + LinkedJobClause(facts)),
        "HADR_SYNC_COMMIT" => ComposeSimpleWait(facts, key, "Synchronous-commit waits",
            "Commits are waiting on a synchronous availability-group secondary — HADR_SYNC_COMMIT",
            "HADR_SYNC_COMMIT is the time a committing transaction on the primary spends waiting for a synchronous-commit secondary to acknowledge that it has hardened the log block. It is the availability-group half of commit latency — the local log flush is WRITELOG — so on a synchronous AG every commit pays both. Sustained HADR_SYNC_COMMIT means the round trip to the secondary is slow: the secondary's own log write, the network between the replicas, or a secondary that is busy with redo or with readable-secondary queries and is slow to harden.",
            "Measure the replica side before touching the primary: the secondary's transaction-log write latency and its redo queue, and the network latency between the replicas. A secondary whose log sits on slower storage than the primary's, or in another region, sets the primary's commit latency — fix that, not the primary. Then review which databases actually need synchronous commit: it is a durability and failover-readiness POLICY (zero data loss on failover), and only the databases whose recovery-point objective requires it should carry it; the rest can be asynchronous by design, not as a performance shortcut. Do NOT switch a database from synchronous to asynchronous commit to make this wait disappear — that trades away the guarantee the setting exists for, and the decision belongs to whoever owns the recovery-point objective, not to a wait-stats finding."),
        // #4149: LATCH_EX/LATCH_SH are NOT page latches — sys.dm_os_wait_stats documents LATCH_EX as
        // excluding buffer and transaction-mark latches. The class decides the cause, so the first step
        // is get_latch_stats, not tempdb or last-page-insert guesswork (that advice moved to
        // PAGELATCH_EX/PAGELATCH_UP below, where the wait type actually implicates it).
        "LATCH_EX" => ComposeSimpleWait(facts, key, "Exclusive latch waits",
            "Exclusive latch contention on a non-buffer resource — LATCH_EX",
            "LATCH_EX is contention on in-memory structures OTHER than buffer-pool pages or transaction-mark latches — sys.dm_os_wait_stats excludes both from this wait type. It is not last-page insert contention or tempdb allocation contention; those surface as PAGELATCH_EX and PAGELATCH_UP. Find the latch class first: ACCESS_METHODS_DATASET_PARENT and ACCESS_METHODS_SCAN_RANGE_GENERATOR point at parallel scan coordination, FGCB_ADD_REMOVE points at file growth or shrink, and LOG_MANAGER points at log growth.",
            "Get the latch class before acting — the fix depends entirely on which one is hot. For ACCESS_METHODS_DATASET_PARENT or ACCESS_METHODS_SCAN_RANGE_GENERATOR, reduce unnecessary parallelism (cost threshold for parallelism, MAXDOP) or address why the optimizer is choosing wide parallel scans. For FGCB_ADD_REMOVE, fix file autogrowth (larger fixed increments, pre-size the file) so the file stops growing or shrinking under load. For LOG_MANAGER, address log growth directly — pre-size the log and fix whatever is generating excess log records."),
        "LATCH_SH" => ComposeSimpleWait(facts, key, "Shared latch waits",
            "Shared latch contention on a non-buffer resource — LATCH_SH",
            "LATCH_SH is shared-latch contention on in-memory structures OTHER than buffer-pool pages — sys.dm_os_wait_stats excludes buffer latches from this wait type. It is not concurrent readers hitting the same hot page (that is a buffer-pool page latch, tracked separately); it frequently accompanies heavy parallelism or specific internal hotspots. Find the latch class first: ACCESS_METHODS_DATASET_PARENT and ACCESS_METHODS_SCAN_RANGE_GENERATOR point at parallel scan coordination, FGCB_ADD_REMOVE points at file growth or shrink, and LOG_MANAGER points at log growth.",
            "Get the latch class before acting — the fix depends entirely on which one is hot. For ACCESS_METHODS_DATASET_PARENT or ACCESS_METHODS_SCAN_RANGE_GENERATOR, reduce unnecessary parallelism (cost threshold for parallelism, MAXDOP) or address why the optimizer is choosing wide parallel scans. For FGCB_ADD_REMOVE, fix file autogrowth. For LOG_MANAGER, address log growth directly."),
        // #4149: last-page-insert / OPTIMIZE_FOR_SEQUENTIAL_KEY belongs here, not on LATCH_EX — the
        // wait CREATE INDEX's own docs name for it is PAGELATCH_EX.
        "PAGELATCH_EX" => ComposeSimpleWait(facts, key, "Exclusive page-latch waits",
            "Exclusive page-latch contention — often last-page insert hotspots (PAGELATCH_EX)",
            "PAGELATCH_EX is contention on in-memory buffer-pool pages, not row locks. The most common cause is last-page insert contention: many sessions inserting into an index with an ever-increasing key (an identity or a datetime) all fight for the latch on the final page.",
            "For last-page insert contention on SQL Server 2019 and later, enable OPTIMIZE_FOR_SEQUENTIAL_KEY on the hot index — it is purpose-built for exactly this. Otherwise spread the inserts across more pages (a hash or reversed key, or partitioning). Do NOT add a clustered index expecting it to fix this — a sequential clustered key is what CREATES the hotspot."),
        "PAGELATCH_UP" => ComposeSimpleWait(facts, key, "Tempdb allocation page-latch waits",
            "Tempdb allocation contention — PFS/GAM/SGAM page latches (PAGELATCH_UP)",
            "PAGELATCH_UP is contention on tempdb's allocation bitmap pages (PFS/GAM/SGAM): sessions that rapidly create and drop temp tables, or spill sorts and hashes to tempdb, all latch the same small set of allocation pages in each data file. It is an in-memory latch — not disk I/O (that is PAGEIOLATCH) and not row locks — and it is the classic 'too few tempdb data files for the core count' signal.",
            "Add tempdb data files so allocations spread across more bitmap pages — one per logical core up to 8, all the same size, with equal autogrowth (the source recommendation is add tempdb files / TF 1118). Confirm MIXED_PAGE_ALLOCATION is OFF (the default since 2016; TF 1118 is the pre-2016 equivalent forcing uniform extents). Then cut the churn that drives it: reduce how many temp objects the hot procedures create, and right-size the queries spilling to tempdb (accurate statistics give adequate grants) so fewer sessions hit the allocation pages at once."),
        "LCK_M_S" => ComposeSimpleWait(facts, key, "Shared-lock waits",
            "Readers are blocked waiting for shared locks — LCK_M_S",
            "LCK_M_S is time spent waiting to acquire a shared (read) lock, which under the default READ COMMITTED means a reader is blocked behind a writer's exclusive lock.",
            "This is the classic readers-blocked-by-writers pattern that Read Committed Snapshot Isolation (RCSI) eliminates — readers get the last committed row version instead of blocking. Failing that, shorten the writing transactions and index them so they lock fewer rows for less time."),
        "LCK_M_IS" => ComposeSimpleWait(facts, key, "Intent-shared lock waits",
            "Waits acquiring intent-shared locks — LCK_M_IS",
            "LCK_M_IS is waiting for an intent-shared lock — the lock a reader takes at the table or page level before taking shared locks on rows. Blocking here means an incompatible higher-level lock is held, often an exclusive table or page lock from a large modification or a lock escalation.",
            "Find the modification holding the table- or page-level exclusive lock — usually a large UPDATE/DELETE that escalated to a table lock. Batch it so it touches fewer rows at once (staying under the escalation threshold) and index it so it locks only what it changes. RCSI removes the reader side of the contention."),
        _ => _byKey[key]
    };

    /// <summary>
    /// Range-lock composed: prepends the wait totals to the shared (cleaned) LCK_RANGE block that every
    /// real LCK_M_R* key aliases to. Falls back to that block when the wait metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeRangeLock(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey[key];
        var numbers = WaitNumbers(facts, key, "Key-range lock waits");
        return numbers.Length == 0 ? fallback : fallback with { Investigation = numbers + fallback.Investigation };
    }

    /// <summary>
    /// The dominant lock-mode contributor from a grouped LCK fact's metadata: the largest per-mode
    /// "{LCK_M_*}_ms" entry, excluding the standard wait-metadata keys. Null when none present.
    /// </summary>
    private static (string Mode, double Ms)? DominantLockMode(Fact f)
    {
        var standard = new HashSet<string> { "wait_time_ms", "signal_wait_time_ms", "resource_wait_time_ms", "avg_ms_per_wait", "period_duration_ms" };
        string? best = null;
        double bestMs = 0;
        foreach (var kv in f.Metadata)
            if (kv.Key.EndsWith("_ms", StringComparison.Ordinal) && !standard.Contains(kv.Key) && kv.Value > bestMs)
            {
                best = kv.Key;
                bestMs = kv.Value;
            }
        return best is null ? null : (best[..^3], bestMs);
    }

    /// <summary>
    /// LCK composed: states the total lock-wait time and the dominant lock mode (from the per-mode
    /// metadata split). Falls back to the static block when the wait metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeLck(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["LCK"];
        var numbers = WaitNumbers(facts, "LCK", "Lock waits");
        if (numbers.Length == 0)
            return fallback;

        var inv = new StringBuilder(numbers);
        if (facts.TryGetValue("LCK", out var f))
        {
            var dom = DominantLockMode(f);
            if (dom is not null)
                inv.Append($"The largest single contributor was {dom.Value.Mode} at {Duration(dom.Value.Ms)}. ");
        }
        inv.Append("Lock waits are sessions blocked waiting to acquire locks others hold — the general signal that concurrent access is serializing on the same rows or objects.");

        var rem =
            "The fix follows the dominant mode above: shared and intent-shared waits are readers blocked by " +
            "writers (Read Committed Snapshot Isolation removes them); exclusive and update waits are writers " +
            "contending — shorten and index the modifications so they lock fewer rows for less time. The " +
            "blocking-events and blocking-chain findings, when they co-fired, name the specific chains.";

        return fallback with
        {
            Headline = "Lock contention — sessions are serializing on locks others hold (LCK)",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// IO_READ_LATENCY_MS / IO_WRITE_LATENCY_MS composed: states the average latency, the operation
    /// count, and total stall. Falls back to the static block when the metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeIoLatency(IReadOnlyDictionary<string, Fact> facts, string key)
    {
        var fallback = _byKey[key];
        var read = key == "IO_READ_LATENCY_MS";
        var avg = FactMeta(facts, key, read ? "avg_read_latency_ms" : "avg_write_latency_ms");
        if (avg is null)
            return fallback;

        var totalOps = FactMeta(facts, key, read ? "total_reads" : "total_writes");
        var stall = FactMeta(facts, key, read ? "total_stall_read_ms" : "total_stall_write_ms");
        var verb = read ? "reads" : "writes";

        var inv = new StringBuilder($"Average {(read ? "read" : "write")} latency was {avg.Value:0.#} ms this window");
        if (totalOps is > 0)
            inv.Append($" across {totalOps.Value:N0} {verb}");
        if (stall is > 0)
            inv.Append($", {Duration(stall.Value)} of total stall");
        inv.Append($". Latency in the tens of milliseconds is slow enough to surface as query latency, and this is a server-wide average — a single hot file can be worse than the number suggests.");

        var rem = new StringBuilder();
        if (read)
        {
            rem.Append("Two angles: cut the read volume by fixing the scans that pull more data than needed and give the buffer pool enough memory to keep hot pages cached; and check the storage itself — sustained high read latency on a quiet workload is a storage problem, not a SQL one.");
            if (Fired(facts, "MISSING_INDEX"))
                rem.Append(" Missing-index requests co-fired this window — the missing-index card lists them as corroboration for the read volume, with its caveat; an index that covers a scan cuts its reads, and every write then pays for it.");
            if (Fired(facts, "PAGEIOLATCH_SH") || Fired(facts, "PAGEIOLATCH_EX"))
                rem.Append(" PAGEIOLATCH co-fired, confirming reads are stalling on disk.");
        }
        else
        {
            rem.Append("Write latency is usually the storage or the log path: confirm the data and log files are on adequately fast storage, watch for autogrowth events stalling writes, and check whether a backup, CHECKDB, or index maintenance overlapped the window.");
            if (Fired(facts, "WRITELOG"))
                rem.Append(" WRITELOG co-fired, pointing specifically at the log.");
            // #3538 A9: when the IO_WRITE_LATENCY_MS → RUNNING_JOBS edge fired, the "check whether index
            // maintenance overlapped the window" above is no longer a guess — say which job.
            rem.Append(LinkedJobClause(facts));
        }

        return fallback with
        {
            Headline = $"Storage {verb} averaged {avg.Value:0.#} ms this window — slow enough to surface as query latency",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// MEMORY_GRANT_PENDING composed: states the peak queued waiters and, when present, the emergency
    /// signals (grant timeouts, forced minimal grants). Falls back to the static block when absent.
    /// </summary>
    private static AdviceBlock ComposeMemoryGrantPending(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["MEMORY_GRANT_PENDING"];
        var maxWaiters = FactMeta(facts, "MEMORY_GRANT_PENDING", "max_waiters");
        if (maxWaiters is null)
            return fallback;

        var timeouts = FactMeta(facts, "MEMORY_GRANT_PENDING", "total_timeout_errors") ?? 0;
        var forced = FactMeta(facts, "MEMORY_GRANT_PENDING", "total_forced_grants") ?? 0;

        var inv = new StringBuilder($"Up to {Plural(maxWaiters.Value, "query")} were queued for a memory grant at once this window");
        if (timeouts > 0 || forced > 0)
        {
            var bits = new List<string>();
            if (timeouts > 0)
                bits.Add($"{Plural(timeouts, "grant request")} timed out waiting");
            if (forced > 0)
                bits.Add($"the engine issued {Plural(forced, "forced minimal grant")}");
            inv.Append($", and grant pressure reached the emergency stage — {string.Join(" and ", bits)}");
        }
        inv.Append(". Queries cannot begin executing until they receive their requested workspace memory, so a backlog here stalls throughput directly.");

        var rem =
            "Cut the grants rather than just adding memory: the usual cause is over-sized grants from bad " +
            "cardinality estimates, so update statistics and add the indexes that let the optimizer estimate " +
            "rows accurately, and find the queries asking for the largest grants. If max server memory is " +
            "capped well below physical RAM, raising it gives the pool more workspace to hand out. Resource " +
            "Governor can cap per-request grants when one workload is starving the rest.";

        return fallback with
        {
            Headline = $"Queries are stalling for memory grants — up to {maxWaiters.Value:N0} queued at once",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// PAGEIOLATCH_SH / _EX composed: the WaitNumbers sentence plus the buffer-pool/storage concept and
    /// the current max-server-memory sentence. Falls back to the static block (still appending the
    /// memory sentence) when the wait metadata is absent.
    /// </summary>
    private static AdviceBlock ComposePageIoLatch(IReadOnlyDictionary<string, Fact> facts, string key)
    {
        var fallback = _byKey[key];
        var numbers = WaitNumbers(facts, key, "Page-I/O latch waits");
        var mem = MaxMemorySentence(facts);
        if (numbers.Length == 0)
            return mem.Length == 0 ? fallback : fallback with { Remediation = fallback.Remediation + " " + mem };

        var purpose = key == "PAGEIOLATCH_EX" ? "modify" : "read";
        var body =
            $"PAGEIOLATCH waits are time spent waiting for a data page to be read from disk into the buffer pool (here, to {purpose} it) — the page was not cached, so SQL Server had to fetch it. Sustained PAGEIOLATCH usually means the working set does not fit in memory, so pages are evicted and re-read, or that the storage is slow.";
        var rem = new StringBuilder("Two levers: give the buffer pool more memory so hot pages stay cached, and reduce the pages read by fixing the scans that pull more data than needed.");
        if (Fired(facts, "MISSING_INDEX"))
            rem.Append(" Missing-index requests co-fired — the missing-index card lists them as corroboration for the pages read, with its caveat.");
        if (Fired(facts, "IO_READ_LATENCY_MS"))
            rem.Append(" Read latency also fired this window, so the storage is part of the problem, not just memory.");
        if (mem.Length > 0)
            rem.Append(" " + mem);

        return fallback with
        {
            Headline = key == "PAGEIOLATCH_EX"
                ? "Waiting on disk reads of pages to modify — PAGEIOLATCH_EX"
                : "Waiting on disk reads of pages into the buffer pool — PAGEIOLATCH_SH",
            Investigation = numbers + body,
            Remediation = rem.ToString()
        };
    }

    /// <summary>
    /// QUERY_SPILLS composed: states total spills, how many distinct queries spilled, then the
    /// statistics/grant remediation and the current max-server-memory sentence.
    /// </summary>
    private static AdviceBlock ComposeQuerySpills(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["QUERY_SPILLS"];
        var total = FactMeta(facts, "QUERY_SPILLS", "total_spills");
        var mem = MaxMemorySentence(facts);
        if (total is null)
            return mem.Length == 0 ? fallback : fallback with { Remediation = fallback.Remediation + " " + mem };

        var distinct = FactMeta(facts, "QUERY_SPILLS", "spilling_query_count");
        var inv = new StringBuilder($"{Plural(total.Value, "spill")} to tempdb this window");
        if (distinct is > 0)
            inv.Append($", across {Plural(distinct.Value, "distinct query")}");
        inv.Append(". A spill means a query's memory grant was too small for its sort or hash operation, so the overflow went to tempdb — disk-speed work in place of memory-speed.");

        var rem =
            "Spills are a symptom of under-estimated memory grants, almost always from stale or skewed " +
            "statistics: update statistics on the participating tables and add the indexes that let the " +
            "optimizer estimate rows accurately, so it requests an adequate grant. Persistent spilling on " +
            "good statistics points at genuine memory pressure.";
        if (mem.Length > 0)
            rem += " " + mem;

        return fallback with
        {
            Headline = $"{Plural(total.Value, "query spill")} to tempdb — memory grants too small for their sorts/hashes",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// QUERY_HIGH_DOP composed: states how many queries ran at DOP &gt; 8, then the current-MAXDOP/CTFP
    /// guidance clause. Falls back to the static block (with the clause) when the count is absent.
    /// </summary>
    private static AdviceBlock ComposeQueryHighDop(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["QUERY_HIGH_DOP"];
        var count = FactMeta(facts, "QUERY_HIGH_DOP", "high_dop_query_count");
        var clause = MaxdopCtfpClause(facts);
        if (count is null)
            return clause.Length == 0 ? fallback : fallback with { Remediation = clause + fallback.Remediation };

        var inv =
            $"{Plural(count.Value, "query")} ran at a degree of parallelism above 8 this window. Very high DOP rarely " +
            "helps OLTP queries — each parallel branch reserves a worker thread and adds coordination overhead " +
            "(CXPACKET), and beyond the processors in a single NUMA node the returns turn negative.";
        var rem = clause +
            "The cap above brings these queries in line with the box's topology; genuinely large analytical " +
            "queries that benefit from parallelism can be granted a higher degree explicitly rather than through a loose global default.";

        return fallback with { Investigation = inv, Remediation = rem };
    }

    /// <summary>
    /// PARAMETER_SENSITIVITY composed: states the worst cost-swing ratio across parameter values, how
    /// many plans showed the pattern, and whether grants/spills diverge (the sniffing fingerprint).
    /// Falls back to the static block when the metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeParameterSensitivity(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["PARAMETER_SENSITIVITY"];
        var ratio = FactMeta(facts, "PARAMETER_SENSITIVITY", "worst_ratio");
        if (ratio is null)
            return fallback;

        var offenders = FactMeta(facts, "PARAMETER_SENSITIVITY", "offender_count");
        var grantDiv = FactMeta(facts, "PARAMETER_SENSITIVITY", "grant_divergence") is > 0;
        var spillDiv = FactMeta(facts, "PARAMETER_SENSITIVITY", "spill_divergence") is > 0;

        var inv = new StringBuilder($"The worst-affected plan ran {ratio.Value:0.#}× more expensive for some parameter values than for others");
        if (offenders is > 0)
            inv.Append($", and {Plural(offenders.Value, "plan")} showed the pattern");
        inv.Append(". That cost swing is the fingerprint of a parameter-sensitive plan: one compiled and cached for a single parameter value, then reused for values that need a very different shape.");
        if (grantDiv || spillDiv)
            inv.Append(grantDiv && spillDiv
                ? " Memory grants AND spills diverged across executions — the textbook symptom of a single plan's grant being wrong for the other parameter values."
                : grantDiv
                    ? " Memory grants diverged sharply across executions — a single plan's grant being wrong for the other parameter values."
                    : " Some executions spilled and others did not — a single plan's grant being wrong for the other parameter values.");

        var rem =
            "Do NOT force one of these plans — that locks in the wrong shape for the other parameter values. " +
            "Use OPTION (RECOMPILE) on the affected statement so each execution gets a plan for its own " +
            "parameters, or branch the procedure by parameter value so each branch compiles its own plan. " +
            "On SQL Server 2022 and later, Parameter Sensitive Plan optimization addresses some of these automatically.";

        return fallback with
        {
            Headline = $"A plan runs {ratio.Value:0.#}× more expensive for some parameter values than others — parameter sensitivity",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// PLAN_REGRESSION composed: states the regression factor, latest-vs-best per-exec CPU, how many
    /// queries regressed, and PERMUTES on whether a forced plan is failing to apply (a silent fallback
    /// to the regressed plan). Falls back to the static block when the metadata is absent.
    /// </summary>
    private static AdviceBlock ComposePlanRegression(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["PLAN_REGRESSION"];
        var factor = FactMeta(facts, "PLAN_REGRESSION", "worst_regression_factor");
        if (factor is null)
            return fallback;

        var offenders = FactMeta(facts, "PLAN_REGRESSION", "offender_count");
        var latest = FactMeta(facts, "PLAN_REGRESSION", "latest_cpu_per_exec_us");
        var best = FactMeta(facts, "PLAN_REGRESSION", "best_cpu_per_exec_us");
        var forceFailing = FactMeta(facts, "PLAN_REGRESSION", "latest_is_forced") is > 0
                           && FactMeta(facts, "PLAN_REGRESSION", "force_failure_count") is > 0;
        var forceFails = FactMeta(facts, "PLAN_REGRESSION", "force_failure_count");

        var inv = new StringBuilder($"The worst-regressed query is running {factor.Value:0.#}× slower than its own best historical plan");
        if (latest is not null && best is not null)
            inv.Append($" ({Micros(latest.Value)} vs {Micros(best.Value)} of CPU per execution)");
        if (offenders is > 0)
            inv.Append($"; {Plural(offenders.Value, "query")} regressed this window");
        inv.Append(". Query Store has the faster plan on record, so this is a plan choice that got worse — not a query that inherently costs more.");
        if (forceFailing && forceFails is not null)
            inv.Append($" A forced plan is in place but failing to apply ({Plural(forceFails.Value, "failure")}), so SQL Server is silently falling back to the regressed plan — the force is not actually protecting you.");

        var rem = forceFailing
            ? "Fix the failing force first — the recorded plan force is not taking effect (the plan was likely evicted or invalidated); re-force the good plan or clear the broken force, then confirm it sticks. Then address WHY the plan regressed — usually stale statistics or a parameter-sensitivity swing."
            : "The engine attaches a ready-to-run force statement for the historically faster plan — forcing it stops the bleeding immediately. Then address WHY the worse plan got chosen — usually stale statistics or a parameter-sensitivity swing — so you are not relying on a forced plan indefinitely.";

        return fallback with
        {
            Headline = $"A query regressed to {factor.Value:0.#}× its best plan's cost — a plan choice got worse",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// TEMPDB_USAGE composed: states peak reserved MB and PERMUTES on the dominant consumer — version
    /// store (long transactions), internal objects (spills), or user objects (#temp) — since each has
    /// a different fix. Falls back to the static block when the metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeTempdbUsage(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["TEMPDB_USAGE"];
        var reserved = FactMeta(facts, "TEMPDB_USAGE", "max_reserved_mb");
        if (reserved is null)
            return fallback;

        var user = FactMeta(facts, "TEMPDB_USAGE", "max_user_object_mb") ?? 0;
        var internalObj = FactMeta(facts, "TEMPDB_USAGE", "max_internal_object_mb") ?? 0;
        var version = FactMeta(facts, "TEMPDB_USAGE", "max_version_store_mb") ?? 0;

        // report.tempdb_pressure keys pressure_level PURELY on the absolute version-store size and checks
        // it FIRST in the recommendation CASE (install/47:1429-1443): a version store over the > 1000 MB
        // MEDIUM bar is a specific, actionable problem (a long-running RCSI/snapshot transaction pinning
        // row versions) regardless of what else is bigger — and it is exactly what the
        // ScoreTempDbVersionStore arm fires on. So when the version store clears that bar make it the
        // driver even if another consumer is nominally larger; otherwise chase the largest consumer.
        var versionStorePressure = version > 1000;
        string driverName, fix;
        double driverMb;
        bool driverIsLargest;
        if (versionStorePressure || (version >= user && version >= internalObj && version > 0))
        {
            driverName = "the version store";
            driverMb = version;
            driverIsLargest = version >= user && version >= internalObj;
            fix = "The version store grows with long-running transactions under RCSI or snapshot isolation (and with heavy triggers) — find and shorten the oldest open transaction; tempdb cannot reclaim its versions until that transaction ends.";
        }
        else if (internalObj >= user && internalObj > 0)
        {
            driverName = "internal objects (sort and hash spills)";
            driverMb = internalObj;
            driverIsLargest = true;
            fix = "Internal objects are sorts and hash spills — fix the queries spilling to tempdb by getting them adequate memory grants and accurate statistics so the work stays in memory, which usually means correcting bad cardinality estimates.";
        }
        else
        {
            driverName = "user objects (#temp tables and table variables)";
            driverMb = user;
            driverIsLargest = true;
            fix = "User objects are #temp tables and table variables — find the sessions materializing large temp objects and reduce what they stage, or index them so they hold fewer rows.";
        }

        return fallback with
        {
            Headline = driverIsLargest
                ? $"tempdb reserved up to {reserved.Value:N0} MB — {driverName} was the largest consumer"
                : $"tempdb reserved up to {reserved.Value:N0} MB — the version store reached {driverMb:N0} MB",
            Investigation = driverIsLargest
                ? $"tempdb reserved up to {reserved.Value:N0} MB this window, with {driverName} the largest consumer at {driverMb:N0} MB. That identifies which of the three tempdb consumers to chase."
                : $"tempdb reserved up to {reserved.Value:N0} MB this window; the version store reached {driverMb:N0} MB — over the 1 GB pressure bar — so it is the signal to chase even though it is not the largest of the three consumers.",
            Remediation = fix
        };
    }

    /// <summary>
    /// Z-score anomaly composed: states the observed value, the σ-deviation above the hour-of-week
    /// baseline, and the baseline itself — "X reached V, Nσ above its M baseline for this hour-of-week".
    /// <paramref name="observedKey"/> is the metadata key holding the observed peak/current value;
    /// <paramref name="fmt"/> renders both it and the baseline in the metric's unit. Falls back to the
    /// static block when the deviation/baseline metadata is absent.
    /// <para>
    /// #3653 (A8): when the fact carries the window MEAN (<paramref name="windowMeanKey"/>) and its
    /// deviation (<c>mean_deviation_sigma</c>) — every fact the pair-gated detectors emit — the sentence
    /// carries both deviations ("peak 6.1σ … the window mean 2.4σ"), because the gate now fired on both
    /// and an operator reading one number would take a peak-sized σ for the whole window's. A fact from
    /// before the pair (no mean sigma) keeps the single-deviation sentence verbatim.
    /// </para>
    /// <para>
    /// #3691 lane 41: a <c>baseline_zero_history</c> fact gets a THIRD shape, before the σ sentence is built —
    /// the SQL Server half of the same fix (one engine, one truth; the PostgreSQL twin is
    /// <c>PgTargetAdvice.ComposeDeviation</c>). The bucket cleared its tier's sample and distinct-day floors and
    /// held nothing but zeros, so the finding is an EXTREMITY against the strongest baseline statement there is,
    /// not a deviation measured in sigmas — and the stored <c>deviation_sigma</c> is the display cap rather than a
    /// measurement, so no σ figure is printed on this arm at all. The trusted sentence below is untouched.
    /// </para>
    /// </summary>
    private static AdviceBlock ComposeAnomaly(IReadOnlyDictionary<string, Fact> facts, string key, string observedKey, string noun, Func<double, string> fmt, string meaning, string? windowMeanKey = null)
    {
        var fallback = _byKey[key];
        var observed = FactMeta(facts, key, observedKey);
        var sigma = FactMeta(facts, key, "deviation_sigma");
        var mean = FactMeta(facts, key, "baseline_mean") ?? FactMeta(facts, key, "baseline_mean_ms");
        if (observed is null || sigma is null || mean is null)
            return fallback;

        var samples = FactMeta(facts, key, "baseline_samples");

        if ((FactMeta(facts, key, "baseline_zero_history") ?? 0) >= 1)
        {
            var days = FactMeta(facts, key, "baseline_distinct_days");
            var restsOn = samples is > 0
                ? $"{samples.Value:N0} baseline sample{(samples.Value == 1 ? "" : "s")}" +
                  (days is > 0 ? $" across {days.Value:N0} distinct day{(days.Value == 1 ? "" : "s")}" : string.Empty)
                : "this server's hour-of-week baseline";
            return fallback with
            {
                Headline = $"{noun} reached {fmt(observed.Value)} — against a month in which this hour saw none",
                Investigation =
                    $"{noun} reached {fmt(observed.Value)} this window. This server's baseline for this hour-of-week is not " +
                    $"thin — it is a measured ZERO: {restsOn}, not one of them above zero. {meaning} That makes this an " +
                    "extremity rather than a deviation: the quantity went from never-happens to this, and there is no " +
                    "dispersion to divide by precisely because there was nothing to divide. Beyond any σ — no deviation " +
                    "figure is printed, because none would be honest against a history of zeros. Check whether it lines up " +
                    "with a workload change, a deploy, or a one-off job before treating it as chronic.",
                Remediation =
                    "This is the first time this hour has seen it at all, which makes the change itself the lead: find what " +
                    "changed. If it was a one-time event — a report run, a backfill, a deploy — awareness is enough, but a " +
                    "quantity that was reliably zero and now is not will usually recur; the standard threshold finding will " +
                    "pick it up on a later window with its own detail, and treat it then with the matching wait or resource advice."
            };
        }
        var inv = new StringBuilder($"{noun} reached {fmt(observed.Value)} this window, {sigma.Value:0.#}σ above its {fmt(mean.Value)} baseline for this hour-of-week");
        if (samples is > 0)
            inv.Append($" (over {samples.Value:N0} baseline samples)");
        var windowMean = windowMeanKey is null ? null : FactMeta(facts, key, windowMeanKey);
        var meanSigma = FactMeta(facts, key, "mean_deviation_sigma");
        if (windowMean is not null && meanSigma is not null)
            inv.Append($"; the window's mean of {fmt(windowMean.Value)} sat {meanSigma.Value:0.#}σ above it (the detector fires only when the mean clears its bar as well, so this is the window running high, not one hot sample)");
        inv.Append($". {meaning} This is a deviation from normal for this time of day and week, not necessarily a sustained problem — check whether it lines up with a workload change, a deploy, or a one-off job before treating it as chronic.");

        var rem =
            "If it was a one-time event — a report run, a backfill, a deploy — no action beyond awareness. " +
            "If it recurs or sustains, it will cross the standard thresholds and surface as a first-class " +
            "finding with its own detail on a later window; treat it then with the matching wait or resource advice.";

        return fallback with
        {
            Headline = $"{noun} spiked to {fmt(observed.Value)} — {sigma.Value:0.#}σ above its baseline for this time of week",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// Ratio-anomaly composed (BLOCKING / DEADLOCK spike): states the event count this window and how
    /// many times the hour-of-week baseline rate it represents. Falls back to the static block when
    /// the count/ratio metadata is absent.
    /// </summary>
    private static AdviceBlock ComposeAnomalyRatio(IReadOnlyDictionary<string, Fact> facts, string key, string noun)
    {
        var fallback = _byKey[key];
        var current = FactMeta(facts, key, "current_count");
        var ratio = FactMeta(facts, key, "ratio");
        if (current is null || ratio is null)
            return fallback;

        // is_new = the baseline was too thin to trust a ratio (the detector fell back to the absolute
        // count). Render it as a first occurrence — never the dishonest "spiked to 100×" the sentinel
        // ratio would otherwise print.
        var isNew = (FactMeta(facts, key, "is_new") ?? 0) >= 1;
        if (isNew)
        {
            var invNew =
                $"{Plural(current.Value, noun)} this window — a first occurrence, with no established baseline for this " +
                "hour-of-week yet to compare against. Treat it as a new event, not a proven regression: check whether it " +
                "coincides with a workload change, a deploy, or a one-off before treating it as chronic.";
            var remNew =
                $"If it recurs or sustains, it will cross the standard {noun} threshold and surface as a first-class finding " +
                "with its chain or graph detail on a later window; treat it then. A one-time event that matches a known cause needs only awareness.";
            return fallback with
            {
                Headline = $"{Plural(current.Value, noun)} this window — first occurrence, no baseline yet",
                Investigation = invNew,
                Remediation = remNew
            };
        }

        var baseRate = FactMeta(facts, key, "baseline_rate");
        var inv = new StringBuilder($"{Plural(current.Value, noun)} this window — about {ratio.Value:0.#}× the normal rate for this hour-of-week");
        if (baseRate is not null)
            inv.Append($" (baseline {baseRate.Value:0.#})");
        inv.Append(". This is a spike against the time-of-week baseline, not necessarily a sustained problem — check whether it coincides with a workload change or a one-off event.");

        var rem =
            $"If it recurs or sustains, it will cross the standard {noun} threshold and surface as a first-class finding " +
            "with its chain or graph detail on a later window; treat it then. A one-time spike that matches a known event needs only awareness.";

        return fallback with
        {
            Headline = $"{noun} rate spiked to {ratio.Value:0.#}× its baseline for this time of week",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// ANOMALY_WAIT_PROFILE composed (change 1): the whole-server wait rate shifted above its baseline.
    /// Names the top contributing wait types (from the contrib_&lt;TYPE&gt; metadata keys — the type
    /// name lives in the KEY since the metadata dictionary values are doubles), and states the current
    /// vs baseline per-second rate. When is_new (thin baseline), renders it as a first occurrence.
    /// <para>
    /// #3741: when the fact carries the window MEAN rate (<c>avg_ms_per_sec</c>) the sentence names it
    /// beside the peak, as #3724's <see cref="ComposeAnomaly"/> does for every other family — the
    /// detector now fires the trusted arms only when the mean clears its bar too, and an operator reading
    /// the peak alone would take one hot collection's number for the whole window's. On the trusted arms
    /// the clause says so and carries the mean's robust deviation (<c>mean_modified_z</c>) when the bucket
    /// had one, or the mean's multiple of the baseline when it did not (the ratio arm). On is_new the bar
    /// is on the peak ALONE, so the mean is stated as plain context — the sentence must not claim a gate
    /// the detector did not apply. A pre-#3741 fact (no mean) keeps its sentence verbatim.
    /// </para>
    /// </summary>
    private static AdviceBlock ComposeAnomalyWaitProfile(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["ANOMALY_WAIT_PROFILE"];
        if (!facts.TryGetValue("ANOMALY_WAIT_PROFILE", out var f))
            return fallback;

        // Top contributors: metadata keys "contrib_<TYPE>" → value = the type's total wait ms; name
        // them in descending order.
        var contributors = f.Metadata
            .Where(kvp => kvp.Key.StartsWith("contrib_", StringComparison.Ordinal))
            .OrderByDescending(kvp => kvp.Value)
            .Select(kvp => kvp.Key.Substring("contrib_".Length))
            .ToList();
        var topList = contributors.Count == 0
            ? "the collected wait types"
            : string.Join(", ", contributors.Take(3));

        var isNew = (f.Metadata.GetValueOrDefault("is_new")) >= 1;
        var current = f.Metadata.GetValueOrDefault("current_ms_per_sec");
        var mean = f.Metadata.GetValueOrDefault("baseline_mean");
        var ratio = f.Metadata.GetValueOrDefault("ratio");
        /* #3741: the window mean beside the peak. Nullable: absent on a fact from before the pair. */
        double? windowMean = f.Metadata.TryGetValue("avg_ms_per_sec", out var avg) ? avg : null;
        var meanModifiedZ = f.Metadata.GetValueOrDefault("mean_modified_z");

        string inv;
        string headline;
        if (isNew)
        {
            headline = "The server's wait profile is heavy, with no baseline yet to compare against";
            var meanClause = windowMean is null
                ? ""
                : $" (the window's mean was about {windowMean.Value:0.#} ms/sec)";
            inv =
                $"The all-types wait rate peaked at about {current:0.#} ms/sec this window{meanClause}, led by {topList}. There is no " +
                "established wait-rate baseline for this hour-of-week yet, so this is flagged on its absolute level, not a " +
                "proven deviation — treat it as a first look at where the server spends its wait time, and check whether it " +
                "lines up with a workload change or a one-off job.";
        }
        else
        {
            headline = $"The server's wait profile shifted to about {ratio:0.#}× its baseline for this time of week";
            /* The trusted arms fire only when the mean clears its bar as well — say so, with the mean's own
               deviation: its robust modified z when the bucket had a median/MAD (the gate arm), otherwise its
               multiple of the baseline mean (the ratio arm, where the modified z is 0 by construction). */
            var meanClause = windowMean is null
                ? ""
                : meanModifiedZ > 0
                    ? $"; the window's mean of {windowMean.Value:0.#} ms/sec sat {meanModifiedZ:0.#} robust σ above the baseline median " +
                      "(the detector fires only when the mean clears its bar as well, so this is the profile running heavy across the window, not one hot collection)"
                    : mean > 0
                        ? $"; the window's mean of {windowMean.Value:0.#} ms/sec was roughly {windowMean.Value / mean:0.#}× that baseline " +
                          "(the detector fires only when the mean clears its bar as well, so this is the profile running heavy across the window, not one hot collection)"
                        : "";
            inv =
                $"The all-types wait rate peaked at about {current:0.#} ms/sec this window — roughly {ratio:0.#}× the " +
                $"{mean:0.#} ms/sec normal for this hour-of-week — led by {topList}{meanClause}. This is a shift in the overall wait " +
                "profile, not necessarily a sustained problem: check whether it coincides with a workload change, a deploy, " +
                "or a one-off event before treating it as chronic.";
        }

        var rem =
            "Open the Wait Stats tab and zoom to this window to see which of the named types drove the shift, then chase " +
            "the dominant one with its normal playbook. If the elevated profile persists across windows, the threshold-based " +
            "finding for the leading wait type will fire and the standard advice applies.";

        return fallback with
        {
            Headline = headline,
            Investigation = inv,
            Remediation = rem
        };
    }

    /// <summary>
    /// ANOMALY_MEMORY_PRESSURE composed: the z-score anomaly framing plus the current max-server-memory
    /// sentence (this anomaly's fix often depends on the configured cap).
    /// </summary>
    private static AdviceBlock ComposeAnomalyMemoryPressure(IReadOnlyDictionary<string, Fact> facts)
    {
        var a = ComposeAnomaly(facts, "ANOMALY_MEMORY_PRESSURE", "peak_memory_pressure_pct", "Memory pressure", Pct,
            "Total server memory fell relative to its target — the OS may be reclaiming memory from SQL Server, or the buffer pool grew unusually fast.", "avg_memory_pressure_pct");
        var mem = MaxMemorySentence(facts);
        return mem.Length == 0 ? a : a with { Remediation = a.Remediation + " " + mem };
    }

    /// <summary>DB_CONFIG composed: lists which per-database settings drifted, with counts.</summary>
    private static AdviceBlock ComposeDbConfig(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["DB_CONFIG"];
        if (!facts.TryGetValue("DB_CONFIG", out var f))
            return fallback;

        var items = new List<string>();
        void Add(string metaKey, string desc)
        {
            if (f.Metadata.TryGetValue(metaKey, out var v) && v > 0)
                items.Add($"{(long)v:N0} with {desc}");
        }
        Add("auto_shrink_on_count", "AUTO_SHRINK on");
        Add("auto_close_on_count", "AUTO_CLOSE on");
        Add("rcsi_off_count", "RCSI off");
        Add("page_verify_not_checksum_count", "page verify not set to CHECKSUM");
        Add("auto_create_stats_off_count", "auto-create statistics off");
        Add("auto_update_stats_off_count", "auto-update statistics off");
        // Query Store off = user DBs (database_count) minus those with it on (query_store_on_count);
        // the QS-off scorer arm (INFO, install/50:83) roots off the same two counts.
        var qsOff = f.Metadata.TryGetValue("database_count", out var dbc) && dbc > 0
                 && f.Metadata.TryGetValue("query_store_on_count", out var qOn)
                    ? (long)(dbc - qOn) : 0L;
        if (qsOff > 0)
            items.Add($"{qsOff:N0} with Query Store off");
        if (items.Count == 0)
            return fallback;

        var inv = $"Across this server's databases: {string.Join("; ", items)}. These are per-database settings that have drifted from the safe defaults and quietly cost performance or stability.";
        var rem =
            "Bring each back to the safe default on the databases that matter: AUTO_SHRINK off (it fragments " +
            "indexes and churns I/O re-growing what it shrank), AUTO_CLOSE off, page verify set to CHECKSUM so " +
            "corruption is detected, auto-create and auto-update statistics on, and RCSI on where the blocking " +
            "is readers-versus-writers. The Database Configuration view lists exactly which databases each applies to.";
        if (qsOff > 0)
            rem +=
                " Where Query Store is off, enable it (ALTER DATABASE <db> SET QUERY_STORE = ON; then " +
                "OPERATION_MODE = READ_WRITE) so query performance history is captured for troubleshooting " +
                "and plan-regression detection.";

        return fallback with
        {
            Headline = "Database settings have drifted from the safe defaults on this server",
            Investigation = inv,
            Remediation = rem
        };
    }

    /// <summary>FILE_AUTOGROWTH_PERCENT composed: states how many files/databases grow by percent.</summary>
    private static AdviceBlock ComposeFileAutogrowth(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["FILE_AUTOGROWTH_PERCENT"];
        var files = FactMeta(facts, "FILE_AUTOGROWTH_PERCENT", "file_count");
        if (files is null)
            return fallback;

        var dbs = FactMeta(facts, "FILE_AUTOGROWTH_PERCENT", "database_count");
        var inv = new StringBuilder($"{Plural(files.Value, "database file")} on this server");
        if (dbs is > 0)
            inv.Append($" (across {Plural(dbs.Value, "database")})");
        inv.Append(" are set to grow by a PERCENTAGE rather than a fixed size. Percentage growth gets larger as the file grows, so each autogrowth event takes longer and stalls writes for longer — and on the log it fragments into ever-larger virtual log files.");

        var rem =
            "Switch these files to a fixed-size growth increment appropriate to their size (a few hundred MB " +
            "to a few GB), so growth is predictable and bounded. Keep autogrowth ENABLED as a safety net — the " +
            "goal is a fixed increment, not turning it off — and size files ahead of demand so it rarely fires. " +
            "Instant File Initialization makes data-file growth near-instant (it does not help the log).";

        return fallback with
        {
            Headline = $"{Plural(files.Value, "file")} grow by percentage, not fixed size — autogrowth stalls worsen as they grow",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>MISSING_INDEX composed: states the suggestion count and the strongest estimated impact.</summary>
    private static AdviceBlock ComposeMissingIndex(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["MISSING_INDEX"];
        var count = FactMeta(facts, "MISSING_INDEX", "index_count");
        if (count is null)
            return fallback;

        var impact = FactMeta(facts, "MISSING_INDEX", "max_impact");
        /* #3805: the caveat opens the composed investigation, as it opens the static one — the composed block
           REPLACES the static Investigation, so a caveat on the static block alone would vanish the moment the
           fact carried metadata, which it always does. */
        var inv = new StringBuilder(MissingIndexCaveat);
        inv.Append($" {Plural(count.Value, "missing-index suggestion")} from the plans of this window's top queries");
        if (impact is > 0)
            inv.Append($", the strongest with an estimated {impact.Value:0.#}% improvement");
        inv.Append(". These come from the optimizer's own missing-index data — it noticed, while costing one statement, that an index it could not find would have lowered that statement's estimated cost; the top plans are the ones this window measured most expensive, which is the only sense in which a request here is traced from a measured-slow query.");

        var rem =
            "Treat these as candidates, not commands: the optimizer suggests a wide covering index per query and " +
            "ignores write cost and overlap with existing indexes. Review them together — several suggestions often " +
            "collapse into one sensible index — and weigh the estimated improvement against the added write and " +
            "maintenance cost before creating any. The attached detail has the suggested key and included columns.";

        return fallback with
        {
            Headline = $"{Plural(count.Value, "missing-index suggestion")} from the top queries{(impact is > 0 ? $" — strongest ~{impact.Value:0.#}% estimated improvement" : string.Empty)}",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>PLAN_WARNING composed: states the warning count and how many are critical.</summary>
    private static AdviceBlock ComposePlanWarning(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["PLAN_WARNING"];
        var warns = FactMeta(facts, "PLAN_WARNING", "warning_count");
        if (warns is null)
            return fallback;

        var crit = FactMeta(facts, "PLAN_WARNING", "critical_count");
        var inv = new StringBuilder($"{Plural(warns.Value, "plan warning")} in the top queries' execution plans this window");
        if (crit is > 0)
            inv.Append($", {crit.Value:N0} of them critical");
        inv.Append(". Plan warnings are things the optimizer flags in the plan itself — implicit conversions defeating an index seek, missing join predicates (accidental cross joops), oversized memory grants, and spills.");

        var rem =
            "Pull the flagged plans and fix by warning type: an implicit conversion usually means a parameter or " +
            "column datatype mismatch (correct the type so the seek becomes usable); a missing join predicate means " +
            "an accidental cross join; grant and spill warnings tie back to statistics. Do the critical ones first.";

        return fallback with
        {
            Headline = crit is > 0
                ? $"{Plural(warns.Value, "plan warning")} ({crit.Value:N0} critical) in the top queries' plans"
                : $"{Plural(warns.Value, "plan warning")} in the top queries' plans",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// #3538 A9: the sentence that ties a symptom card to the long-running job when RUNNING_JOBS FIRED
    /// this window (running-long count above zero — the same predicate the maintenance edges in
    /// <see cref="RelationshipGraph"/> use, so the prose and the incident clustering agree on when the
    /// link exists). States the job facts the engine collected (count, worst overrun, longest runtime)
    /// rather than re-describing the symptom. Empty when the fact is absent or no job is running long,
    /// so a caller can append it unconditionally. Leading space included. The RUNNING_JOBS fact's
    /// metadata is counts and durations; since #3653 the collectors also put the worst job's NAME on the
    /// fact's ObjectName, and the job card (<see cref="ComposeRunningJobs"/>) states it. This clause is
    /// the symptom cards' text (SCH_M, IO_WRITE_LATENCY_MS, WRITELOG) and still points at the Running Jobs
    /// view for the name; carrying the name into it is those cards' change, not the job card's.
    /// </summary>
    private static string LinkedJobClause(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!Fired(facts, "RUNNING_JOBS"))
            return string.Empty;
        var longCount = FactMeta(facts, "RUNNING_JOBS", "running_long_count");
        if (longCount is not > 0)
            return string.Empty;

        var pct = FactMeta(facts, "RUNNING_JOBS", "max_percent_of_average");
        var dur = FactMeta(facts, "RUNNING_JOBS", "max_duration_seconds");
        var sb = new StringBuilder($" RUNNING_JOBS fired in the same window — {Plural(longCount.Value, "Agent job")} running well past normal duration");
        if (pct is > 0)
            sb.Append($", the worst at {pct.Value:N0}% of its historical average");
        if (dur is > 0)
            sb.Append($" (about {Duration(dur.Value * 1000)} so far)");
        sb.Append(" — so this finding and that job are ONE incident, not two: the Running Jobs view names the job, and moving or shortening it is the fix for both cards.");
        return sb.ToString();
    }

    /// <summary>
    /// SCH_M composed (#3538 A9): states the schema-lock wait totals and, when RUNNING_JOBS fired, names
    /// the long-running job as the likely holder instead of telling the operator to go and look for one.
    /// Falls back to the static block when the wait metadata is absent AND no job is running long.
    /// </summary>
    private static AdviceBlock ComposeSchM(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["SCH_M"];
        var numbers = WaitNumbers(facts, "SCH_M", "Schema-modification lock waits");
        var linked = LinkedJobClause(facts);
        if (numbers.Length == 0 && linked.Length == 0)
            return fallback;

        var concept =
            "SCH-M is the most exclusive lock SQL Server takes — it is incompatible with everything, including the IS lock a SELECT requires, so a session holding SCH-M on a hot table blocks the entire workload against that table. Sources: `ALTER TABLE`, `CREATE/DROP INDEX`, partition operations, and certain statistics updates.";
        var inv = new StringBuilder(numbers).Append(concept);
        if (linked.Length > 0)
            inv.Append(linked);
        else
            inv.Append(" No Agent job was running past its normal duration this window, so look for ad-hoc DDL or an index operation issued outside the job schedule.");

        return fallback with
        {
            Headline = linked.Length > 0
                ? "Schema-modification lock waits while an Agent job runs long — its index maintenance is blocking the workload"
                : fallback.Headline,
            Investigation = inv.ToString()
        };
    }

    /// <summary>
    /// RUNNING_JOBS composed: states how many jobs overran and by how much, and (#3653) WHICH one.
    /// <para>The collectors put the name of the job furthest past its own history on the fact's
    /// ObjectName (both SKUs, one name, running-long rows only — the SQL comment there states the choice).
    /// When it is present the headline, the first sentence and the remediation name the job; when it is
    /// absent — a finding persisted before the collectors carried it, or a window where jobs ran but none
    /// ran long — every sentence is the pre-#3653 one, so the frozen text of old findings and the
    /// nothing-long case read exactly as before. The name is guarded on the count as well as the slot:
    /// a name with a zero count cannot come from the collectors (the FILTER yields NULL) and naming a job
    /// that did not run long would be the lie this arm exists to stop. The overrun figures stay attributed
    /// to "the worst", not to the named job, because <c>max_percent_of_average</c> and
    /// <c>max_duration_seconds</c> are window maxima over EVERY running row, long or not, while the name
    /// is chosen among the long rows only; the two coincide in the ordinary case and the wording does not
    /// depend on it.</para>
    /// </summary>
    private static AdviceBlock ComposeRunningJobs(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["RUNNING_JOBS"];
        var longCount = FactMeta(facts, "RUNNING_JOBS", "running_long_count");
        if (longCount is null)
            return fallback;

        var pct = FactMeta(facts, "RUNNING_JOBS", "max_percent_of_average");
        var dur = FactMeta(facts, "RUNNING_JOBS", "max_duration_seconds");
        var name = longCount.Value > 0 && facts.TryGetValue("RUNNING_JOBS", out var jobFact) && !string.IsNullOrEmpty(jobFact.ObjectName)
            ? jobFact.ObjectName
            : null;
        var others = longCount.Value - 1;
        var inv = new StringBuilder(name is null
            ? $"{Plural(longCount.Value, "Agent job")} ran well past normal duration this window"
            : others > 0
                ? $"{Plural(longCount.Value, "Agent job")} ran well past normal duration this window, `{name}` the furthest past its own history (the Running Jobs view lists the {Plural(others, "other")})"
                : $"Agent job `{name}` ran well past normal duration this window");
        if (pct is > 0)
            inv.Append($", the worst at {pct.Value:N0}% of its historical average");
        if (dur is > 0)
            inv.Append($" (longest current runtime about {Duration(dur.Value * 1000)})");
        inv.Append(". A job running far past its average is usually stuck — blocked, waiting on a resource, or hung — rather than simply busy.");
        // #3538 A9: the reverse link — the job card names the symptoms whose graph edges point at it, so
        // whichever card the operator opens first says the incident is one thing. "Fired" here is the
        // scorer's >0 working set (the same set the edge predicates evaluate against); a symptom that also
        // ROOTED its own finding is clustered with this one, a trace-level one is merely linked.
        var symptoms = new List<string>();
        if (longCount.Value > 0 && Fired(facts, "SCH_M")) symptoms.Add("SCH_M (schema-modification lock waits — the job is probably index maintenance holding schema locks)");
        if (longCount.Value > 0 && Fired(facts, "IO_WRITE_LATENCY_MS")) symptoms.Add("IO_WRITE_LATENCY_MS (its write volume is a candidate for the latency)");
        if (longCount.Value > 0 && Fired(facts, "WRITELOG")) symptoms.Add("WRITELOG (a fully logged rebuild or reload drives log flushes)");
        if (symptoms.Count > 0)
            inv.Append($" Also elevated this window and linked to this job by the engine: {string.Join("; ", symptoms)}. Where one of them rooted its own finding, it and this card are ONE incident, not two.");

        var rem =
            (name is null
                ? "Check the long-running jobs in Agent history: one at several times its normal runtime is typically "
                : $"Check `{name}` in Agent history first{(others > 0 ? ", then the others" : string.Empty)}: a job at several times its normal runtime is typically ") +
            "blocked (look for it in the blocking findings) or waiting on a resource. Decide whether to let it finish " +
            "or stop it, and address the cause — a job that regularly overruns its window usually needs its schedule " +
            "or its workload rethought.";

        return fallback with
        {
            Headline = name is null
                ? $"{Plural(longCount.Value, "Agent job")} running well past normal — likely stuck, not busy"
                : others > 0
                    ? $"Agent job `{name}` and {Plural(others, "other")} running well past normal — likely stuck, not busy"
                    : $"Agent job `{name}` running well past normal — likely stuck, not busy",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>DISK_SPACE composed: states the tightest volume's free space (MB; avoids the pct unit ambiguity).</summary>
    private static AdviceBlock ComposeDiskSpace(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["DISK_SPACE"];
        var mb = FactMeta(facts, "DISK_SPACE", "min_free_mb");
        if (mb is null)
            return fallback;

        var totalFree = FactMeta(facts, "DISK_SPACE", "total_free_mb");
        var vols = FactMeta(facts, "DISK_SPACE", "volume_count");
        var inv = new StringBuilder($"The tightest volume on this server is down to {mb.Value:N0} MB free");
        if (vols is > 1 && totalFree is > 0)
            inv.Append($" (of {vols.Value:N0} volumes monitored, {totalFree.Value:N0} MB free in total)");
        inv.Append(". Low free space on a database volume risks autogrowth failures, and at zero a database can no longer accept writes.");

        var rem =
            "Free space on the tightest volume first: clear old backups and files that do not belong, and check " +
            "whether a runaway autogrowth or an unexpectedly large object is consuming it (the object-growth finding " +
            "flags those). If the data is legitimately growing, add space before the file fills — an out-of-space data " +
            "or log file takes the database read-only or offline.";

        return fallback with
        {
            Headline = $"A volume is down to {mb.Value:N0} MB free — out-of-space risk",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// BAD_ACTOR_{hash} composed: states the offending query's executions and per-exec cost, and
    /// PERMUTES on frequent-lightweight vs rare-heavyweight (the fix differs). The fact is keyed by
    /// the full BAD_ACTOR_{hash}, so the value reads use that key directly.
    /// </summary>
    private static AdviceBlock ComposeBadActor(string key, IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["BAD_ACTOR"];
        if (!facts.TryGetValue(key, out var f))
            return fallback;

        var exec = f.Metadata.GetValueOrDefault("execution_count");
        var cpu = f.Metadata.GetValueOrDefault("avg_cpu_ms");
        var reads = f.Metadata.GetValueOrDefault("avg_reads");
        var db = f.DatabaseName;

        var inv = new StringBuilder($"A single query{(string.IsNullOrEmpty(db) ? string.Empty : $" in {db}")} took an outsized share of the server's work this window");
        if (exec > 0)
            inv.Append($": {Plural(exec, "execution")}");
        if (cpu > 0)
            inv.Append($", averaging {cpu:0.#} ms CPU");
        if (reads > 0)
            inv.Append($" and {reads:N0} reads each");
        inv.Append(". ");
        var frequent = exec >= 1000;
        inv.Append(frequent
            ? "It is a high-frequency, individually-cheap query run so often that the total cost is large — the win is reducing how often it runs (caching, batching at the application) or shaving its per-execution cost."
            : "It is a low-frequency but very expensive query — run rarely, costly each time — so the win is tuning the single execution: the right indexes and a better plan shape.");

        var rem =
            "The attached detail has the query text and a live plan analysis. " +
            (frequent
                ? "For a frequent query, first ask whether it needs to run this often at all; then make each execution cheap — a covering index, a tighter predicate, a cached result."
                : "For a heavyweight query, tune the single execution — eliminate the scans and spills its plan shows, and give it the indexes it needs.");

        return fallback with
        {
            Headline = frequent
                ? "One frequently-run query dominated the server's work this window"
                : "One expensive query dominated the server's work this window",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>Server-health advisories (IFI / LPIM / memory dumps) — clean, tool-name-free.</summary>
    private static AdviceBlock ComposeServerHealth(string key, IReadOnlyDictionary<string, Fact> facts) => key switch
    {
        "CONFIG_IFI_DISABLED" => _byKey[key] with
        {
            Investigation = "Instant File Initialization is off, so SQL Server zero-fills every new data-file allocation — every data-file autogrowth and every database restore or creation writes zeroes across the whole new region before it can be used, turning a fast growth into a long stall.",
            Remediation = "Grant the 'Perform volume maintenance tasks' right to the SQL Server service account (Local Security Policy) and restart the service — data-file growth, restores, and file creation become near-instant. It does NOT affect the transaction log, which is always zero-initialized."
        },
        "CONFIG_LPIM_DISABLED" => ComposeLpim(facts),
        "SERVER_MEMORY_DUMPS" => ComposeMemoryDumps(facts),
        _ => _byKey[key]
    };

    private static AdviceBlock ComposeLpim(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["CONFIG_LPIM_DISABLED"];
        var ram = FactMeta(facts, "CONFIG_LPIM_DISABLED", "physical_memory_mb");
        var inv = "Lock Pages in Memory is off, so Windows can page out SQL Server's buffer pool under memory pressure — trimming its working set to disk and causing sudden severe slowdowns when the OS reclaims memory aggressively, often after another memory-hungry process starts on the host.";
        if (ram is > 0)
            inv += $" This server has {ram.Value:N0} MB of physical RAM.";
        var rem = "On a dedicated SQL Server with a sane max-server-memory cap, granting 'Lock pages in memory' to the service account stops the OS paging out the buffer pool. Pair it with a max-server-memory setting that leaves the OS headroom — LPIM without a cap can starve the OS. It matters most on physical hosts; weigh it against your virtualization and memory configuration.";
        return fallback with { Investigation = inv, Remediation = rem };
    }

    private static AdviceBlock ComposeMemoryDumps(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["SERVER_MEMORY_DUMPS"];
        var n = FactMeta(facts, "SERVER_MEMORY_DUMPS", "memory_dump_count");
        if (n is null or <= 0)
            return fallback;
        var inv = $"SQL Server has written {Plural(n.Value, "memory dump")}. A dump means SQL Server hit an exception serious enough to snapshot its state — an access violation, a scheduler or latch deadlock, or detected corruption. These are not normal background events.";
        var rem = "Look at the dump timestamps and the SQL Server error log around them to find what triggered each; recurring dumps point at a specific bug or a hardware/driver problem. Confirm you are on a current cumulative update (many dump-causing bugs are already fixed), and open a support case with the dumps if they recur on a patched build.";
        return fallback with
        {
            Headline = $"SQL Server has written {Plural(n.Value, "memory dump")} — it hit serious exceptions",
            Investigation = inv,
            Remediation = rem
        };
    }

    /// <summary>
    /// ANOMALY_OBJECT_GROWTH composed: names the table (from the fact's ObjectName carrier) and states
    /// the day-over-day growth in MB and percent. Falls back to the static block when the fact is absent.
    /// </summary>
    private static AdviceBlock ComposeAnomalyObjectGrowth(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["ANOMALY_OBJECT_GROWTH"];
        if (!facts.TryGetValue("ANOMALY_OBJECT_GROWTH", out var f))
            return fallback;

        var growth = f.Metadata.GetValueOrDefault("growth_mb");
        var pct = f.Metadata.GetValueOrDefault("growth_pct");
        var current = f.Metadata.GetValueOrDefault("current_mb");
        var name = string.IsNullOrEmpty(f.ObjectName) ? "A table" : f.ObjectName;
        var dbClause = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";

        var inv = new StringBuilder($"{name}{dbClause} grew {growth:N0} MB day-over-day");
        if (pct > 0)
            inv.Append($" (+{pct:0.#}%)");
        if (current > 0)
            inv.Append($", now {current:N0} MB reserved");
        inv.Append(". This is measured from the two most recent daily object-size snapshots, so it is a genuine day-over-day jump rather than a one-off reading.");

        var rem =
            "Decide whether this is expected load or a problem. Expected growth — an archive import, a backfill, " +
            "an index rebuild that temporarily doubles space — needs only capacity awareness; confirm the volume " +
            "has headroom. Unexpected growth points at a runaway insert without cleanup, a disabled or broken purge " +
            "job, or a heap that needs a clustered index; for genuinely large, fast-growing tables, evaluate PAGE " +
            "compression and partitioning. Steep sustained growth against limited volume free space is the early " +
            "warning for an out-of-space outage.";

        return fallback with
        {
            Headline = $"{name} grew {growth:N0} MB day-over-day{(pct > 0 ? $" (+{pct:0.#}%)" : string.Empty)}",
            Investigation = inv.ToString(),
            Remediation = rem
        };
    }

    /// <summary>
    /// ANOMALY_OBJECT_CONTENTION composed: names the index (from ObjectName) and states the new
    /// lock-wait time and any new lock escalations. Falls back to the static block when absent.
    /// </summary>
    private static AdviceBlock ComposeAnomalyObjectContention(IReadOnlyDictionary<string, Fact> facts)
    {
        var fallback = _byKey["ANOMALY_OBJECT_CONTENTION"];
        if (!facts.TryGetValue("ANOMALY_OBJECT_CONTENTION", out var f))
            return fallback;

        var msDelta = f.Metadata.GetValueOrDefault("lock_wait_ms_delta");
        var esc = f.Metadata.GetValueOrDefault("escalation_delta");
        var name = string.IsNullOrEmpty(f.ObjectName) ? "An index" : f.ObjectName;
        var dbClause = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";

        var inv = new StringBuilder($"{name}{dbClause} accrued {Duration(msDelta)} of new row-lock wait time since the previous daily snapshot");
        if (esc > 0)
            inv.Append($", with {Plural(esc, "new lock escalation")} to a coarser table or page lock");
        inv.Append(". Contention on this object jumped day-over-day — measured from consecutive snapshots taken at the same instance start time, so it is real accrual, not a counter reset.");

        var rem = new StringBuilder("Find the queries hitting this object and reduce how long they hold locks: shorten the transactions, add the index that lets writers touch fewer rows, or batch large modifications. Reader-versus-writer contention is removed by RCSI on the database. ");
        rem.Append(esc > 0
            ? "The lock escalations mean a statement is locking enough rows to escalate to a whole-object lock — keep those modifications under the escalation threshold by batching, or as a last resort disable escalation on the specific table after confirming memory headroom for the extra row locks."
            : "Cross-reference the blocking findings for the same window to see the actual chains.");

        return fallback with
        {
            Headline = $"{name} accrued {Duration(msDelta)} of new lock-wait time — contention jumped day-over-day",
            Investigation = inv.ToString(),
            Remediation = rem.ToString()
        };
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
                "the wait type's series against the rest of the breakdown for the longer-window trajectory.",
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
                "SOS_SCHEDULER_YIELD is logged when a worker uses its full 4 ms quantum and voluntarily yields, so a high count means workers ran CPU-bound for long stretches — most often large scans chewing through pages already in the buffer pool. High SOS on its own does not prove the box was short of CPU. The deciding signal is the runnable queue: how many tasks were ready to run but stacked up waiting their turn on a scheduler. A deep runnable queue alongside the yields is genuine CPU pressure — more demand than the schedulers could serve; a shallow queue means the CPUs were simply busy doing real work, not oversubscribed. The top CPU-consuming queries for this window are attached — those are what burned the CPU.",
            Remediation:
                "Tune the attached top queries first — the wait points straight at queries burning CPU, and cutting that work is cheaper than adding cores either way; fix what their plans show. If tuning cannot bring CPU demand back under capacity, the server is legitimately CPU-bound for its workload and more cores are justified. If parallelism waits were also elevated this window, raise cost threshold for parallelism to 50 and cap MAXDOP at the logical processors in one NUMA node (no more than 8).");

        // THREADPOOL (thread exhaustion) is attribution-keyed by InferenceEngine.ClassifyThreadpool:
        // the finding roots on THREADPOOL_PARALLEL / THREADPOOL_BLOCKING / THREADPOOL_MIXED when the
        // co-cause is clear, else the generic THREADPOOL below. Only the parallel flavor is a
        // MAXDOP/CTFP problem; blocking-driven exhaustion is fixed by clearing the blocking.
        t["THREADPOOL"] = new AdviceBlock(
            Headline:
                "THREADPOOL waits — SQL Server has run out of worker threads, with no clear parallel or blocking cause this window",
            Investigation:
                "Sustained THREADPOOL (it cleared the wait-time + per-wait-average gate, so this isn't pool grow/shrink noise) means new sessions cannot start until a worker frees up. Neither CXPACKET/high-DOP (parallelism) nor blocking co-fired above their own thresholds this window, so the engine could not attribute it — which happens when a co-cause stayed just under threshold or, commonly, the collector could not sample the peak. Decide it by hand: if CXPACKET / high-DOP are present treat it as parallelism (below); if a blocking chain is present treat it as blocking. The waiting-tasks and scheduler-pressure views show what is queued.",
            Remediation:
                "Resolve the dominant cause — parallelism (raise `cost threshold for parallelism` to 50, cap `MAXDOP` at the logical processors in a single NUMA node, ≤ 8) or blocking. This is a report on a window that has already passed, so the worker pool has long since recovered: aim at stopping the recurrence, not a one-off `KILL`. For the blocking case that means fixing the abandoned-transaction code path (a BEGIN TRAN left open on an error/timeout path; SET XACT_ABORT ON) or the slow operation under the held lock. Do NOT raise `max worker threads` to mask it — that trades thread exhaustion for memory pressure (each worker reserves a thread stack outside max server memory) without fixing the cause. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_PARALLEL"] = new AdviceBlock(
            Headline:
                "Thread exhaustion driven by parallelism — too many parallel queries each reserving worker threads",
            Investigation:
                "THREADPOOL fired alongside CXPACKET and/or high-DOP queries: a parallel query reserves up to DOP workers per parallel branch, so a moderate-concurrency workload at high DOP can drain the pool with zero blocking. This is the MAXDOP/CTFP flavor of thread exhaustion. The attached top parallel queries rank the offenders; check whether they genuinely benefit from the degree of parallelism they are getting.",
            Remediation:
                "Guard parallelism: raise `cost threshold for parallelism` to 50 so trivial queries stop going parallel, and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). When both are already at guidance and the pool still exhausts, the driver is the volume of concurrent parallel queries — lower MAXDOP further and/or raise CTFP for that concurrency level, and tune the specific high-DOP queries. Do NOT raise `max worker threads` to mask it (trades thread exhaustion for memory pressure). Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_BLOCKING"] = new AdviceBlock(
            Headline:
                "Thread exhaustion driven by blocking — workers are pinned on blocked (not running) sessions",
            Investigation:
                "THREADPOOL fired alongside blocking: every session waiting on a lock keeps its worker thread assigned, so a wide or deep blocking chain ties up one worker per blocked session and can exhaust the pool. Lowering MAXDOP frees nothing here — the workers are not running parallel work, they are parked on locks. The blocking-chain finding shows the chain's head, whether it was sleeping, its depth, and its victim count.",
            Remediation:
                "Clearing the blocking is what frees the workers — but this is a report on a window that has already passed, so the pile-up is gone and a `KILL` now buys nothing. Aim at the recurrence. If the chain was headed by a sleeping session, that is the abandoned-transaction signature: fix the code path that opens a BEGIN TRAN and never reaches COMMIT/ROLLBACK on the error or client-timeout path, and SET XACT_ABORT ON so an aborted batch rolls back automatically. If the apex was active, there is one slow operation everyone queued behind — fix it, and enable RCSI where the contention is readers blocked by writers. MAXDOP/CTFP are not the levers for this. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["THREADPOOL_MIXED"] = new AdviceBlock(
            Headline:
                "Thread exhaustion with both parallelism and blocking elevated",
            Investigation:
                "THREADPOOL fired with BOTH CXPACKET/high-DOP and blocking co-elevated, so workers are being consumed from two directions: parallel queries reserving DOP workers, and blocked sessions parking workers on locks. Look at the blocking chain (its head, depth, and victim count) and the attached top parallel queries together.",
            Remediation:
                "Collapse the blocking first — it pins workers on sessions that are not even running, so it is the bigger win. This is a report on a past window, so go after the recurrence rather than a one-off kill: fix the abandoned-transaction code path (BEGIN TRAN left open on an error/timeout path; SET XACT_ABORT ON) or the slow operation under the held lock. Then guard parallelism: raise `cost threshold for parallelism` to 50 and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). Do NOT raise `max worker threads` to mask either cause. Note on the data: under live thread exhaustion the collector is itself a query waiting for a worker, so expect a gap in collection during the worst of it — a missing interval in Collection Health around this window corroborates the event, it is not a separate problem.");

        t["CXPACKET"] = new AdviceBlock(
            Headline:
                "Parallelism waits — queries are spending real time waiting for their parallel workers to synchronize",
            Investigation:
                "Every CX* wait (CXPACKET, CXCONSUMER, CXSYNC_PORT, CXSYNC_CONSUMER) is grouped into one parallelism-wait figure. That grouping hides producer/consumer skew — CXCONSUMER on its own specifically means one branch is doing the work while siblings stall waiting for rows. If the per-execution duration of the offending queries is closer to their CPU time than CPU÷DOP would predict, parallelism is mostly contending with itself, not paying for itself. Open the Wait Stats tab to see the breakdown over the analysis window (the server's current CTFP and MAXDOP are stated in the remediation below). The high-DOP queries — those running above DOP 8 — are the ones to look at first; the attached top parallel queries rank them.",
            Remediation:
                "Most OLTP queries should not go parallel — raise `cost threshold for parallelism` to 50 and cap `MAXDOP` at the logical processors in a single NUMA node (≤ 8). When MAXDOP and CTFP are already at guidance and CXPACKET persists, the parallelism is coming from the plans themselves — examine the attached top parallel queries and fix the plan, or the estimate driving it; that is what removes the wait.");

        t["CPU_SQL_PERCENT"] = new AdviceBlock(
            Headline:
                "SQL Server process CPU is sustained above the threshold — the workload is eating real CPU, not waiting on something else",
            Investigation:
                "Open the CPU tab to see the SQL vs. other-process split over the analysis window — that confirms SQL is the consumer rather than antivirus, a runaway agent job, or another tenant on the VM. The top CPU-consuming queries for the window are attached, ranked by total CPU. SOS_SCHEDULER_YIELD co-elevation on the Wait Stats tab means schedulers are saturated; a CPU spike means the load is bursty rather than steady.",
            Remediation:
                "Tune the attached top queries — almost always cheaper than buying cores. If parameter sensitivity or a plan regression fired alongside this, they point at the specific cause: a plan cached for one parameter value reused for a worse one, or a plan worse than the same query used to run. For a plan regression, force the historically faster plan as a fast fix while you address why the worse one was chosen.");

        t["CPU_SPIKE"] = new AdviceBlock(
            Headline:
                "CPU spike detected — peak usage is well above the period average, not sustained saturation",
            Investigation:
                "Spikes differ from steady saturation: the box has headroom most of the time but something briefly burns it all. The exact peak time and the sessions active around it are attached. Open the CPU tab and zoom to that timestamp, then cross-check the Wait Stats tab for what waits dominated. A co-fired plan regression, parameter sensitivity, or CXPACKET points at the cause.",
            Remediation:
                "If a plan regression co-fired, force the historically faster plan as the fast fix. If parameter sensitivity co-fired, do NOT force a plan — that locks in the wrong plan for the other parameter values; use OPTION (RECOMPILE) on the affected statement, or branch the procedure by parameter value. For an ad-hoc reporting spike matching neither, Resource Governor or moving the report off-peak is the durable fix.");

        t["RUNNABLE_TASKS"] = new AdviceBlock(
            Headline:
                "Runnable-task queue is backed up — tasks are ready to run but waiting for a CPU scheduler",
            Investigation:
                "Every runnable task is a query that has everything it needs EXCEPT a free scheduler — the CPUs are the bottleneck, not I/O or locks. The count is the latest cpu_scheduler_stats snapshot's total across all schedulers (the runnable_tasks_warning flag trips when the queue reaches roughly one task per core). Open the CPU tab and the Wait Stats tab: SOS_SCHEDULER_YIELD co-elevation confirms scheduler starvation, and a THREADPOOL wait means it has escalated to worker-thread exhaustion. Cross-check the top CPU-consuming queries for the window.",
            Remediation:
                "CPU pressure — tune the CPU-heavy queries first (almost always cheaper than adding cores); the top consumers for the window are attached. Excessive parallelism is a frequent driver: if CXPACKET or high-DOP queries co-fired, lower MAXDOP / raise the Cost Threshold for Parallelism so small queries stop going parallel and monopolizing schedulers. If the queries are already tuned and the queue stays deep, the box is genuinely under-provisioned for the load — add CPU.");

        // ─────────────────────────────────────────────────────────────────
        // Memory pressure
        // ─────────────────────────────────────────────────────────────────

        t["PAGEIOLATCH_SH"] = new AdviceBlock(
            Headline:
                "PAGEIOLATCH_SH waits — SQL is reading data pages from disk into the buffer pool faster than it can serve them",
            Investigation:
                "The classic buffer-pool-too-small or scan-too-large signal — SQL is evicting pages it still needs and reading them straight back from disk. Open File I/O → File I/O Latency to separate workload-driven pressure from underlying storage: high PAGEIOLATCH_SH with sub-20 ms read latency means the workload is reading too much, not that disk is slow. Severity is boosted when read latency is also high and when memory-grant waiters are present.",
            Remediation:
                "Find the scan and add a covering nonclustered index — one missing index can drop gigabytes of reads per execution and the wait collapses with it; the optimizer's missing-index suggestions on the offending queries are the starting point. If indexing is genuinely right and the wait persists, the buffer pool is too small for the working set — raise `max server memory` if it is capped below the working set (leaving headroom for the OS), or add RAM.");

        t["PAGEIOLATCH_EX"] = new AdviceBlock(
            Headline:
                "PAGEIOLATCH_EX waits — SQL is waiting to write data pages, usually under heavy modification workload",
            Investigation:
                "Shows up under heavy bulk inserts, large updates, or tempdb workspace writes. Open File I/O → File I/O Latency for the write column to separate workload-driven pressure from underlying disk pressure; the per-database write latency is attached. If TEMPDB_USAGE co-fired, the writes are workspace (hash/sort spills) — that finding names the spilling queries. Open the TempDB Stats sub-tab under Resource Metrics to see whether the version store or internal objects drove the growth.",
            Remediation:
                "For tempdb-driven PAGEIOLATCH_EX, fix the spilling queries — update statistics with FULLSCAN to correct the cardinality estimates that produced too-small grants, or rewrite the operator that spills. For modification-workload pressure, batch large operations into smaller chunks so the buffer pool can flush between batches. If the storage itself is slow (write latency consistently above 10 ms on data, 2 ms on log), no amount of query tuning will recover it — the storage is the bottleneck and needs hardware-side investigation.");

        t["CONFIG_PRIORITY_BOOST"] = new AdviceBlock(
            Headline:
                "Priority boost is enabled — SQL Server threads run at above-normal Windows scheduling priority, which can starve OS-critical threads",
            Investigation:
                "`sp_configure 'priority boost'` raises the Windows scheduling priority of SQL Server's threads above normal. Microsoft recommends against it for essentially all servers: it can starve OS threads (network, disk, cluster heartbeat), destabilize the instance, and cause connectivity or failover problems — with no reliable throughput gain. It is almost always a leftover from bad tuning advice rather than a deliberate, measured choice.",
            Remediation:
                "Disable it: `EXECUTE sp_configure 'priority boost', 0; RECONFIGURE;` — the change takes effect after a SQL Server service restart. There is effectively no workload for which priority boost is the right fix.");

        t["CONFIG_LIGHTWEIGHT_POOLING"] = new AdviceBlock(
            Headline:
                "Lightweight pooling (fiber mode) is enabled — it breaks OLEDB and other in-process components and is not recommended",
            Investigation:
                "`sp_configure 'lightweight pooling'` (fiber mode) switches SQL Server to fiber-based scheduling. It disables features that assume thread-based scheduling — most OLEDB providers (linked servers, some CLR, extended stored procedures) fail or misbehave — and its narrow high-end NUMA / context-switch benefit almost never applies. Microsoft recommends leaving it off for essentially all workloads.",
            Remediation:
                "Disable it: `EXECUTE sp_configure 'lightweight pooling', 0; RECONFIGURE;` — takes effect after a service restart. Only consider re-enabling it under a specific, measured high-end OLTP/NUMA scenario with vendor guidance, and never with linked servers / OLEDB in use.");

        t["RESOURCE_SEMAPHORE"] = new AdviceBlock(
            Headline:
                "RESOURCE_SEMAPHORE waits — queries are queueing for memory grants because the workspace pool is exhausted",
            Investigation:
                "Memory grants are reserved up front for sorts, hashes, and parallel operators. When the workspace pool fills, large queries queue behind it and small ones can starve. Open the Memory Grants sub-tab under Memory to see grant pressure over the analysis window, and the Memory Pressure Events sub-tab for the corresponding ring-buffer notifications. If MEMORY_GRANT_PENDING co-fired, its live waiter count and per-snapshot granted-vs-used memory are attached.",
            Remediation:
                "The usual cause is over-granting: a query gets a grant much larger than it actually uses because cardinality estimation was wrong. Pull the offending queries' plans and compare the operator-level row estimates against actuals; update statistics with FULLSCAN on the affected tables, or add filtered indexes if a subset of the data drives the bad estimate. Per-query stopgap: `OPTION (MAX_GRANT_PERCENT = X)` caps a single offender's grant without affecting others. Resource Governor workload-group caps are the durable answer if one workload chronically starves the others.");

        t["MEMORY_CLERKS"] = new AdviceBlock(
            Headline:
                "The TokenAndPermUserStore security cache has grown large — it can cause memory pressure and CPU spent on cache lookups",
            Investigation:
                "TokenAndPermUserStore caches security tokens for permission checks. Under lots of ad-hoc queries, dynamic SQL, or frequent security-context switching (EXECUTE AS, cross-database ownership chaining) it can balloon into gigabytes, crowding out useful plans and making every permission check walk a huge cache. The USERSTORE_TOKENPERM clerk size for this server is attached; the Memory → Memory Clerks view shows it alongside the other top clerks.",
            Remediation:
                "Clear it during a maintenance window with DBCC FREESYSTEMCACHE('TokenAndPermUserStore') for immediate relief. For a durable fix, cut the churn that grows it: parameterize ad-hoc queries so plans and security tokens are reused, and reduce security-context switching (EXECUTE AS, cross-database ownership chaining). If the growth recurs, investigate what is generating the volume of distinct security contexts.");

        t["RESOURCE_SEMAPHORE_QUERY_COMPILE"] = new AdviceBlock(
            Headline:
                "Query compile gateway waits — too many concurrent compilations of expensive queries",
            Investigation:
                "SQL Server gates expensive plan compilations through a semaphore (the big-plan gateway). When too many big plans need compiling at once, sessions wait at the gateway instead of running. The cause is almost always poor parameterisation: ad-hoc queries with literal values, or RPC calls without proper parameter typing, each producing a unique plan that has to be compiled from scratch. Open Memory → Plan Cache to see the single-use plan ratio. The compile-heavy queries — a high execution count against high per-compile cost — are the signature.",
            Remediation:
                "Fix the app to parameterise its calls — that's the only durable cure. As a server-side stopgap, `ALTER DATABASE ... SET PARAMETERIZATION FORCED` makes the optimizer parameterise more aggressively, but test it on a workload copy first because it changes plan-selection behaviour in ways that can hurt other queries. Enabling `optimize for ad hoc workloads` reduces plan-cache bloat from one-off queries but does nothing for the compile cost of any individual large plan. Plan guides on the worst-offender patterns are a per-query escape hatch.");

        t["MEMORY_GRANT_PENDING"] = new AdviceBlock(
            Headline:
                "Queries are queued waiting for memory grants — the workspace pool is full",
            Investigation:
                "This finding carries the live waiter count, plus the granted-vs-used memory and any timeouts or forced grants for the snapshots where waiters were present. One sustained waiter is concerning, five is critical, forced grants or timeouts are emergency. Open the Memory Grants sub-tab under Memory for the trend over the window. RESOURCE_SEMAPHORE co-elevation confirms pool exhaustion is the cause; QUERY_SPILLS co-elevation means queries are running with grants smaller than they needed and spilling to tempdb.",
            Remediation:
                "Same playbook as RESOURCE_SEMAPHORE: the offenders are over-granting because of bad cardinality estimates. Pull the heavy queries' plans and find where the estimate diverged from actuals; update statistics with FULLSCAN, add the right indexes, and the grants shrink. `OPTION (MAX_GRANT_PERCENT = X)` is a per-query stopgap that caps the worst offender without affecting others.");

        t["PLAN_CACHE_BLOAT"] = new AdviceBlock(
            Headline:
                "Single-use plans are bloating the plan cache — a large share of cached plans have been used exactly once, wasting the memory that holds them",
            Investigation:
                "The finding carries the single-use plan count and size against the totals for the latest snapshot; a single-use ratio above 20% is flagged, above 50% is severe. Each single-use plan is a query that compiled, ran once, and will almost certainly never be reused — most often unparameterized ad-hoc SQL or dynamic SQL that embeds literal values, so every distinct value produces its own one-shot plan. That memory is stolen from the buffer pool and from plans that WOULD be reused. Open Memory → Plan Cache to see the single-use vs. multi-use composition and the trend; RESOURCE_SEMAPHORE_QUERY_COMPILE or high SQL Compilations/sec co-firing confirms the same churn is also costing CPU on constant recompilation.",
            Remediation:
                "The low-risk first step is `optimize for ad hoc workloads` at the server level — SQL then caches only a small stub for a plan's first execution and the full plan only if it is reused, which collapses single-use bloat without changing plan choice. The durable fix is to stop generating the churn: parameterize the offending ad-hoc/dynamic SQL in the application so one plan serves every parameter value. Where the app cannot be changed, `ALTER DATABASE ... SET PARAMETERIZATION FORCED` makes the optimizer parameterize more aggressively — test it on a workload copy first, because forced parameterization can change plan selection for other queries. (Sourced from the Dashboard's report.plan_cache_bloat recommendation, install/47:1493.)");

        t["MEMORY_PRESSURE_EVENTS"] = new AdviceBlock(
            Headline:
                "SQL Server signalled physical-memory pressure — the resource monitor logged RESOURCE_MEMPHYSICAL_LOW notifications in the window",
            Investigation:
                "These come from the RING_BUFFER_RESOURCE_MONITOR ring buffer: when the OS or SQL Server itself flags low available physical memory, the resource monitor raises an indicator (0-1 normal, 2 medium, 3+ severe) and SQL responds by trimming caches — evicting buffer-pool pages and plan cache to free memory. The finding carries the count of pressure events and the peak process/system indicators for the window. That cache trimming shows up downstream as more physical reads (PAGEIOLATCH_SH) and more recompiles as evicted plans are rebuilt, so check the Memory tab and the Wait Stats tab around the event times for that fallout.",
            Remediation:
                "Confirm whether the pressure is SQL's or the box's: check `max server memory` against physical RAM and what else runs on the host. If `max server memory` is unset or too high, SQL can starve the OS and other processes into signalling system-wide low memory — cap it leaving headroom for the OS (and for other instances/services on the box). If `max server memory` is already conservative and the system indicator is what fired, an external consumer (another instance, an app, a runaway agent) is taking the memory — find and constrain it. If the server is genuinely short of memory for its workload after those checks, add RAM.");

        // ─────────────────────────────────────────────────────────────────
        // Blocking / lock contention
        // ─────────────────────────────────────────────────────────────────

        t["BLOCKING_EVENTS"] = new AdviceBlock(
            Headline:
                "Blocked process reports are firing above the per-hour threshold — sessions are waiting on locks held by other sessions",
            Investigation:
                "Open Blocking → Blocked Process Reports and zoom to the analysis window — that's the same source the engine just analyzed, with the XML available per row. The top blocking chains are attached, ranked by wait time, each with the database, the blocked and blocking sessions, the lock mode, and truncated SQL for both sides. The blocking-rate trend over the window shows whether it is climbing or a single spike.",
            Remediation:
                "Look at the locks involved in the attached chains: a shared/exclusive mismatch (readers blocked by writers) is RCSI territory — enable READ_COMMITTED_SNAPSHOT on those databases (`ALTER DATABASE [db] SET READ_COMMITTED_SNAPSHOT ON`) so readers read row versions instead of taking shared locks. Writer/writer contention (X/U/IX modes) is not helped by RCSI; the answer is shorter transactions, indexes that let the writer find rows faster instead of locking a range while scanning, and consistent object access order between procedures. For chronic blocking driven by one slow operation, fix that one query — the lock-hold duration is usually proportional to query duration.");

        t["DEADLOCKS"] = new AdviceBlock(
            Headline:
                "Deadlocks are firing above the per-hour threshold — at least one transaction is being killed every few minutes",
            Investigation:
                "Open Blocking → Deadlocks and zoom to the window — every row carries the deadlock graph XML, viewable in-app. The most recent victims (with truncated victim SQL) are attached. SQL Server's deadlock detector runs every 5 seconds, so any sustained rate above the threshold means active recurring contention — not transient. If the graph shows reader victims being killed for writers, READ_COMMITTED_SNAPSHOT on that database eliminates the class — readers read row versions instead of taking the shared locks that deadlock with writers.",
            Remediation:
                "Most deadlocks are caused by inconsistent object access order between two procedures (proc A locks table X then Y; proc B locks Y then X). Read the graph, identify the two paths, and pick one access order for both. For reader/writer deadlocks specifically, enabling READ_COMMITTED_SNAPSHOT on the database eliminates the entire class — readers stop taking shared locks and read row versions instead. Raising deadlock priority on the loser does not prevent the deadlock; it only changes which side dies. Shortening transactions (moving non-transactional work outside `BEGIN TRAN`) shrinks the window in which the deadlock can occur.");

        t["BLOCKING_CHAIN"] = new AdviceBlock(
            Headline:
                "Multi-level blocking chain reconstructed — a session is blocking blockers, fanning out into a pile-up",
            Investigation:
                "The reconstructed chains are attached: up to three, each with its head session, whether the head was sleeping, the depth, the victim count, the longest wait, and a per-level breakdown of the blocked and blocking sessions, lock modes, and the SQL on both sides. A sleeping head is the abandoned-transaction signature — an application started a BEGIN TRAN and never reached COMMIT/ROLLBACK on the error path. THREADPOOL co-elevation means every blocked victim is also holding a worker thread, and the server is at risk of thread exhaustion. Open Blocking → Blocked Process Reports to walk the same chain in the UI.",
            Remediation:
                "This is a report on a window that has already passed — the pile-up has cleared and a `KILL` now buys nothing, so aim at the recurrence. If the top chain's head was sleeping, that is the abandoned-transaction signature: an application opened a BEGIN TRAN and never reached COMMIT/ROLLBACK on its error or client-timeout path. Fix that code path, and SET XACT_ABORT ON so an aborted batch rolls back automatically — killing the session only clears one occurrence. For an active head, there is one slow operation everyone else is queued behind — a missing index forcing a scan under a held lock, an UPDATE whose WHERE clause has no supporting index, or an unindexed foreign key that makes a parent-side UPDATE/DELETE scan the child table. Fix that one operation and the chain dissolves.");

        t["LCK"] = new AdviceBlock(
            Headline:
                "General lock contention is significant in wait stats — writer/writer or update/exclusive lock conflicts dominating",
            Investigation:
                "LCK groups X (exclusive), U (update), IX (intent exclusive), SIX, and BU waits — the writer/writer side of lock contention. Unlike LCK_M_S/LCK_M_IS (readers blocked by writers, fixable with RCSI), these are real serialization points: two writers genuinely cannot proceed at the same time. The per-mode breakdown is attached: every LCK% wait type ranked by total wait time and number of waits over the window — that tells you exactly which lock modes are dominating. Open the Wait Stats tab to see the LCK family on the chart.",
            Remediation:
                "RCSI does NOT help writer/writer contention — it only addresses readers blocked by writers. The fixes are mechanical: shorten transactions so locks are held briefly, fix the queries causing long lock holds (a missing index turning a seek into a scan extends every lock the scan takes), and partition hot tables so independent parts of the workload do not collide on the same rows. An UPDATE without a useful WHERE-clause index is by itself frequently the entire problem — its plan will show the scan and the missing-index suggestion.");

        t["LCK_M_S"] = new AdviceBlock(
            Headline:
                "LCK_M_S waits — readers are being blocked by writers, classic shared/exclusive lock contention",
            Investigation:
                "SELECT queries are queueing behind UPDATE/INSERT/DELETE transactions on the same rows or pages. The per-mode breakdown shows LCK_M_S ranked against the other lock modes for the window; when DB_CONFIG co-fired, the databases where RCSI is off are the candidates for the fix. Open Configuration → Database Configuration in-app to see the full per-database RCSI/auto-shrink/auto-close/page-verify state. Reader/writer deadlocks are the same pattern escalated — if DEADLOCKS co-fired, the graph will show shared-lock victims.",
            Remediation:
                "Enable READ_COMMITTED_SNAPSHOT on the affected database: `ALTER DATABASE <db> SET READ_COMMITTED_SNAPSHOT ON;`. Readers stop taking shared locks and instead read the previous-committed row version, which eliminates the entire wait class for everything running at READ COMMITTED or below. The ALTER needs a brief exclusive lock on the database, so do it at a quiet moment, and the versioning is not free: row versions live in tempdb, where a long-running reader can grow the version store. If the application uses NOLOCK / READUNCOMMITTED hints to work around this same problem today, RCSI is strictly better — it returns committed data instead of dirty reads — but test on a copy first if any code relies on dirty-read behaviour.");

        t["LCK_M_IS"] = new AdviceBlock(
            Headline:
                "LCK_M_IS waits — intent-shared locks are being blocked, usually shared locks waiting for exclusive operations to finish",
            Investigation:
                "Intent-shared locks are the page/table-level locks SELECT takes to declare 'I will be reading rows on this page'. Waiting on IS means the page or table is held in an incompatible mode by a writer — same reader-blocked-by-writer pattern as LCK_M_S, one level up the lock hierarchy. The per-mode breakdown shows LCK_M_IS ranked against the other lock modes for the window. When DB_CONFIG co-fired, the databases with RCSI off are the candidates. Open Configuration → Database Configuration to see the full per-database state.",
            Remediation:
                "Enable READ_COMMITTED_SNAPSHOT on the affected database: `ALTER DATABASE <db> SET READ_COMMITTED_SNAPSHOT ON;`. This eliminates reader/writer blocking for everything at or below READ COMMITTED — the most common isolation level by far. The ALTER needs a brief exclusive lock on the database, so do it at a quiet moment, and the versioning is not free: row versions live in tempdb, where a long-running reader can grow the version store. If the application currently uses READ UNCOMMITTED with NOLOCK hints to avoid these waits, RCSI is the strictly better mechanism — it returns committed data rather than dirty reads, and you can remove the hints once it's enabled — but test on a copy first if any code relies on dirty-read behaviour.");

        t["SCH_M"] = new AdviceBlock(
            Headline:
                "Schema modification lock waits — DDL or index operations are blocking everything else on the affected object",
            Investigation:
                "SCH-M is the most exclusive lock SQL Server takes — it's incompatible with everything, including the IS lock SELECT requires. A query holding SCH-M on a hot table blocks the entire workload against that table. Sources: `ALTER TABLE`, `CREATE/DROP INDEX`, partition operations, and certain statistics updates. Open the Running Jobs tab to see whether a scheduled maintenance job is running during the wrong window — if RUNNING_JOBS also fired, it names the long-running job.",
            Remediation:
                "Move the DDL operation to an actual maintenance window — that's the cure for the scheduled-job case. For ongoing index maintenance on hot tables, `ONLINE = ON` rebuilds take SCH-M only briefly at the start and end of the operation instead of holding it for the whole rebuild; on Enterprise Edition, `WAIT_AT_LOW_PRIORITY` lets the rebuild yield to user queries when there's contention. Statistics updates with `WITH FULLSCAN` on huge tables can also drive SCH-M — schedule them, or use sampled updates instead.");

        t["LCK_RANGE"] = new AdviceBlock(
            Headline:
                "Range-lock waits — SERIALIZABLE isolation is escalating to row-range locks, blocking other readers and writers",
            Investigation:
                "Range locks (LCK_M_RS_*, LCK_M_RIn_*, LCK_M_RX_*) appear only under SERIALIZABLE — they prevent phantom reads by locking ranges of an index rather than just the rows being read. (REPEATABLE READ holds its row/page locks to end-of-transaction but does NOT take key-range locks, which is exactly why phantoms remain possible under it.) Either the application is explicitly requesting SERIALIZABLE, or a `BEGIN TRANSACTION` somewhere is defaulting to it. The most common silent source is .NET's `System.Transactions.TransactionScope`, which defaults to SERIALIZABLE if you do not pass `TransactionOptions`. The dominant range-lock type and the waiting sessions' program names point at the source — the apex is often a specific application name.",
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
                "The per-database, per-file-type latency breakdown is attached, ranked — that tells you which files are slow: data, log, or tempdb. Above 20 ms is concerning, 50 ms is critical for OLTP storage. Open File I/O → File I/O Latency in-app to see the same data graphed over time. PAGEIOLATCH_SH co-elevation means the workload is read-heavy enough to expose the latency; PAGEIOLATCH_SH near zero with high latency means storage is slow but the workload isn't hitting it hard.",
            Remediation:
                "Check the workload first — a missing index forcing scans drives enormous read I/O that exposes any storage as slow. Find the queries with the most logical reads per execution, pull their plans, and add the indexes those plans recommend. The latency often disappears without any storage change. If indexing is right and latency persists, the storage layer is the bottleneck and needs hardware-side action: IOPS, queue depth, whether data files are on appropriate-tier storage, whether another tenant on the SAN is contending. Latency that varies by time of day is almost always shared-infrastructure contention.");

        t["IO_WRITE_LATENCY_MS"] = new AdviceBlock(
            Headline:
                "Average write latency is above the storage-health threshold — disk writes are slow",
            Investigation:
                "The per-database, per-file-type write-latency breakdown is attached, ranked. The transaction log is the usual suspect — every commit blocks on WRITELOG, so even modest log latency directly hurts throughput. Above 10 ms on data files is concerning, 2 ms on log is the OLTP target. Open File I/O → File I/O Latency for the write series over time. WRITELOG co-elevation confirms log is the bottleneck; PAGEIOLATCH_EX co-elevation means data file writes are slow under modification load.",
            Remediation:
                "Move the transaction log to its own low-latency device (NVMe, log-tier SAN), and never share that device with non-database workloads. For data-file write pressure, look at the modification workload — bulk inserts and large updates are the usual drivers, and batching them so the buffer pool can flush between batches helps. A too-wide index set inflates write I/O: every INSERT and DELETE maintains all nonclustered indexes, and an UPDATE maintains every index that includes one of the changed columns — so dropping unused or redundant indexes directly cuts modification cost. The plans of the modification queries will surface specific opportunities.");

        t["WRITELOG"] = new AdviceBlock(
            Headline:
                "WRITELOG waits — transactions are waiting for the log to flush before they can commit",
            Investigation:
                "WRITELOG is the wait at COMMIT: every transaction blocks until its log records are durably written to disk. Two independent causes — slow log storage, or too many small commits. When an I/O finding co-fired, the log file's write latency is attached. Open File I/O → File I/O Latency to see the log series over the window. If log latency is low but WRITELOG is still high, the workload is committing too frequently — chatty client code with one tiny transaction per call. Open the Perfmon tab and add Transactions/sec to see the commit rate.",
            Remediation:
                "Storage fix: log file on its own low-latency device (NVMe, dedicated log-tier SAN), with no contention from data files or tempdb. Workload fix: batch many small writes into fewer larger transactions where the consistency model allows — turning 1000 single-row inserts per second into ten 100-row batches collapses WRITELOG dramatically. `ALTER DATABASE ... SET DELAYED_DURABILITY = FORCED` trades durability for latency and is appropriate when losing the last few committed transactions on a crash is acceptable; not appropriate for financial or audit workloads.");

        // #3538 A5: the availability-group half of commit latency. Advise-only and evidence-gated
        // (ComposeWaitByKey prepends the measured totals); the remediation names the counter-objective
        // out loud — synchronous commit is a recovery-point policy, and the OtterTune doctrine the
        // engine review graded against forbids advising a durability trade for speed — so the block
        // points at the replica and at which databases NEED the guarantee, never at flipping the mode.
        t["HADR_SYNC_COMMIT"] = new AdviceBlock(
            Headline:
                "HADR_SYNC_COMMIT waits — commits are waiting on a synchronous availability-group secondary",
            Investigation:
                "HADR_SYNC_COMMIT is the time a committing transaction on the primary waits for a synchronous-commit secondary to acknowledge that it has hardened the log block. It is the availability-group half of commit latency (the local log flush is WRITELOG), so on a synchronous AG every commit pays both. Sustained HADR_SYNC_COMMIT means the round trip to the secondary is slow — its own log write, the network between the replicas, or a secondary busy with redo or readable-secondary queries. Open the Wait Stats tab for the HADR_SYNC_COMMIT series over the window and compare it with WRITELOG: if WRITELOG is low and HADR_SYNC_COMMIT high, the primary's log is fine and the replica path is the cost.",
            Remediation:
                "Measure the replica side first: the secondary's transaction-log write latency and redo queue, and the network latency between the replicas — a secondary on slower storage or in another region sets the primary's commit latency, and that is where the fix is. Then review which databases actually need synchronous commit: it is a durability and failover-readiness POLICY (zero data loss on failover), so only the databases whose recovery-point objective requires it should carry it, and the rest can be asynchronous by design. Do NOT switch a database to asynchronous commit to make this wait disappear — that trades away the guarantee the setting exists for, and the decision belongs to whoever owns the recovery-point objective, not to a wait-stats finding.");

        // ─────────────────────────────────────────────────────────────────
        // Latch contention
        // ─────────────────────────────────────────────────────────────────

        // #4149: LATCH_EX excludes buffer and transaction-mark latches (sys.dm_os_wait_stats), so the
        // headline and body no longer describe page latches or point straight at tempdb/last-page-insert
        // — those are PAGELATCH_EX/PAGELATCH_UP's territory below. The class from get_latch_stats decides
        // the cause.
        t["LATCH_EX"] = new AdviceBlock(
            Headline:
                "Exclusive latch contention on a non-buffer resource — the class decides the cause (LATCH_EX)",
            Investigation:
                "LATCH_EX is contention on in-memory structures OTHER than buffer-pool pages or transaction-mark latches — it is not last-page insert contention or tempdb allocation contention, which surface as PAGELATCH_EX and PAGELATCH_UP. Get the latch class with get_latch_stats first: ACCESS_METHODS_DATASET_PARENT and ACCESS_METHODS_SCAN_RANGE_GENERATOR point at parallel scan coordination, FGCB_ADD_REMOVE points at file growth or shrink, and LOG_MANAGER points at log growth. Co-elevated CXPACKET or SOS_SCHEDULER_YIELD corroborates the parallelism shape.",
            Remediation:
                "Act on the class, not on the wait type. For ACCESS_METHODS_DATASET_PARENT or ACCESS_METHODS_SCAN_RANGE_GENERATOR, reduce unnecessary parallelism (cost threshold for parallelism, MAXDOP) or fix why the optimizer is choosing wide parallel scans. For FGCB_ADD_REMOVE, pre-size the file and fix small autogrowth increments. For LOG_MANAGER, pre-size the transaction log and address whatever is generating excess log records.");

        t["LATCH_SH"] = new AdviceBlock(
            Headline:
                "Shared latch contention on a non-buffer resource — the class decides the cause (LATCH_SH)",
            Investigation:
                "LATCH_SH is shared-latch contention on in-memory structures OTHER than buffer-pool pages — sys.dm_os_wait_stats excludes buffer latches from this wait type, so it is not concurrent readers thrashing a hot page. Get the latch class with get_latch_stats first: ACCESS_METHODS_DATASET_PARENT and ACCESS_METHODS_SCAN_RANGE_GENERATOR point at parallel scan coordination, FGCB_ADD_REMOVE points at file growth or shrink, and LOG_MANAGER points at log growth. CXPACKET co-elevation means parallel operations are amplifying the contention.",
            Remediation:
                "Act on the class, not on the wait type. For ACCESS_METHODS_DATASET_PARENT or ACCESS_METHODS_SCAN_RANGE_GENERATOR, reduce unnecessary parallelism (cost threshold for parallelism, MAXDOP) or fix why the optimizer is choosing wide parallel scans. For FGCB_ADD_REMOVE, pre-size the file and fix small autogrowth increments. For LOG_MANAGER, pre-size the transaction log and address whatever is generating excess log records.");

        // #4149: last-page-insert / OPTIMIZE_FOR_SEQUENTIAL_KEY moved here from LATCH_EX — CREATE INDEX's
        // own docs name PAGELATCH_EX as the wait type this guidance is for.
        t["PAGELATCH_EX"] = new AdviceBlock(
            Headline:
                "Exclusive page-latch contention — in-memory contention on hot pages, often last-page insert hotspots",
            Investigation:
                "Page latches are short-term synchronization on individual buffer-pool pages. PAGELATCH_EX waits usually mean parallel inserts into a heap or narrow index (every session contending for the same allocation page), or insert hotspots on tables with monotonically increasing keys. Co-elevated CXPACKET or SOS_SCHEDULER_YIELD tells you which shape this is. Open the Latch Stats sub-tab under Resource Metrics for the per-latch-class breakdown over the window.",
            Remediation:
                "For a last-page insert hotspot — an ever-increasing (IDENTITY / sequential) leading key on a B-tree, where every insert latches the same trailing page — the direct fix is `OPTIMIZE_FOR_SEQUENTIAL_KEY = ON` on that index (2019+); otherwise a non-sequential leading key, hash partitioning, or in-memory OLTP. Adding a clustered index on a sequential key CREATES this contention rather than relieving it, so that is not the fix. For parallel inserts contending on a heap's allocation pages (PFS/IAM), spread or partition the insert workload — a sequential clustered key will only move the contention to the index's last page.");

        t["PAGELATCH_UP"] = new AdviceBlock(
            Headline:
                "Tempdb allocation contention — PFS/GAM/SGAM page-latch waits (PAGELATCH_UP)",
            Investigation:
                "PAGELATCH_UP waits are contention on tempdb's allocation bitmap pages (PFS/GAM/SGAM). Sessions that rapidly create and drop temp tables, or spill sorts and hashes to tempdb, all latch the same small set of allocation pages in each data file — the classic 'too few tempdb data files for the core count' signal. It is an in-memory latch, distinct from PAGEIOLATCH (disk reads) and from row locks. Open the TempDB tab and the Latch Stats sub-tab under Resource Metrics to confirm the allocation shape.",
            Remediation:
                "Add tempdb data files so allocations spread across more bitmap pages — one per logical core up to 8, all the same size, with equal autogrowth (the source recommendation is add tempdb files / TF 1118). Confirm MIXED_PAGE_ALLOCATION is OFF (the default since 2016; TF 1118 is the pre-2016 equivalent that forces uniform extents). Then cut the churn that drives it: reduce how many temp objects the hot procedures create, and right-size the queries spilling to tempdb (accurate statistics give adequate grants) so fewer sessions hit the allocation pages at once.");

        // ─────────────────────────────────────────────────────────────────
        // TempDB
        // ─────────────────────────────────────────────────────────────────

        t["TEMPDB_USAGE"] = new AdviceBlock(
            Headline:
                "TempDB usage is above the configured threshold — workspace allocation or version store growth is heavy",
            Investigation:
                "The per-snapshot breakdown of user objects, internal objects, version store, and unallocated space is attached, ranked by total usage — that immediately tells you which of the three drivers is the problem. User objects = explicit `#temp` tables and table variables from application code. Internal objects = spills from hash joins, sorts, and hash aggregates running with too-small grants. Version store = RCSI/SI row versions held by a long-running reader transaction. Open the TempDB tab in-app to see the same series graphed. QUERY_SPILLS co-elevation confirms the spill shape and names the offending queries.",
            Remediation:
                "Match the fix to the driver. Spill-driven: update statistics on the offending tables (the cardinality estimate fed the too-small grant), add the missing index the plan analysis surfaces, or use `OPTION (MIN_GRANT_PERCENT = X)` as a per-query stopgap. User-object-driven: look at the worst-offender procedures and ask whether every `#temp` is needed; chains of `SELECT INTO #x FROM #y` are often a single CTE in disguise. Version-store growth: this reports a window that has passed, so the version-holding transaction is long gone — the durable fix is the code path that holds a transaction open across the version-store read (no transaction over 5 minutes is normal for OLTP); shorten it or commit sooner. Tempdb: one data file per core up to 8, sized identically, with autogrowth left ON at an equal fixed-MB increment — pre-size the files so growth is rare rather than disabling it (a hard stop on growth turns a busy window into query failures).");

        // ─────────────────────────────────────────────────────────────────
        // Query-level
        // ─────────────────────────────────────────────────────────────────

        t["QUERY_SPILLS"] = new AdviceBlock(
            Headline:
                "Query spills — operators are running out of granted memory and spilling intermediate results to tempdb",
            Investigation:
                "A hash join, sort, or hash aggregate gets a grant smaller than it needs and falls back to writing to tempdb — usually 10-100x slower than the in-memory operator. Root cause is almost always a bad cardinality estimate: the optimizer guessed 1,000 rows and the operator actually saw 1,000,000. The top spilling queries for the window are attached, ranked by spill count, with the database, the query, and how often it ran. Open Queries → Top Queries by Duration in-app and sort by spill columns; the query's plan will show the operator-level estimate-vs-actual divergence.",
            Remediation:
                "Update statistics with FULLSCAN on the tables that drive the bad estimate — stale statistics are the single most common cause. Add filtered indexes if a subset of the data is the hot spot, or rewrite the operator (a hash join over a sorted input that should have been a merge join is a classic). Per-query stopgaps: `OPTION (RECOMPILE)` re-estimates at each execution against current statistics; `OPTION (MIN_GRANT_PERCENT = X)` forces a larger grant. If many queries spill consistently and grant pressure is high, the workspace pool may simply need more memory available, so check whether `max server memory` is capped too low for the workload.");

        t["QUERY_HIGH_DOP"] = new AdviceBlock(
            Headline:
                "Queries are running with high degree of parallelism — DOP above 8 is unusual outside of warehousing workloads",
            Investigation:
                "Queries at DOP > 8 consume disproportionate scheduler and thread-pool resources for what's usually a small per-query benefit, and they amplify CXPACKET and THREADPOOL pressure. The attached top queries carry each query's DOP — that's the direct signal. Open Queries → Top Queries by Duration to see DOP alongside other per-query metrics. Look at per-execution CPU vs. duration: if duration is roughly CPU÷DOP, parallelism is paying for itself; if duration is close to CPU, the workers are mostly contending with each other.",
            Remediation:
                "Cap `MAXDOP` at the instance level — the logical processors in a single NUMA node (≤ 8) — and raise `cost threshold for parallelism` to 50 so trivial queries stop going parallel at all. For specific queries that genuinely benefit from higher DOP (analytics, reporting), use `OPTION (MAXDOP X)` as a per-query override rather than raising the instance default. After the change, watch CXPACKET on the Wait Stats tab — if it drops without total CPU regressing, the change was correct.");

        t["PARAMETER_SENSITIVITY"] = new AdviceBlock(
            Headline:
                "Parameter-sensitive plan — one cached plan is wildly different in cost across different parameter values",
            Investigation:
                "The affected queries are attached: up to five with their worst cost ratio (max/min per-execution CPU for the same plan), whether the memory grant diverges across executions, and whether the plan spills on some parameter values but not others. A cost ratio above 10x — the detector threshold — means the plan is catastrophic for some inputs: usually a plan compiled for a selective value being reused for an unselective one, or vice versa. Open Queries → Top Queries by Duration in-app and look up the query for its full plan history; running it with the extreme parameter values shows the two divergent shapes.",
            Remediation:
                "**Do not force a plan.** Forcing locks in a plan that is bad for some parameter values — the whole point of detecting parameter sensitivity is that no single plan works for every input. The correct fixes: `OPTION (RECOMPILE)` for a fresh plan per execution (good when values vary widely and the query is not called thousands of times per minute, since compile cost matters), `OPTION (OPTIMIZE FOR ...)` to deliberately compile for the worst-case value, plan guides for specific branches, or splitting the query into two procedures by parameter shape. On SQL Server 2022, Parameter Sensitive Plan optimization (PSP) handles some plan shapes automatically — check the database compatibility level and consider raising it if the workload is on the supported shape list.");

        t["PLAN_REGRESSION"] = new AdviceBlock(
            Headline:
                "Plan regression — a query is running a worse plan than one it has performed well with in the past",
            Investigation:
                "The regressed queries are attached: up to five with their regression factor (latest cost ÷ historical-best cost per execution), the latest and best plans, and the per-execution CPU and duration for both. The 14-day window means the historical best plan may not be in plan cache anymore — Query Store has it. Open Queries → Query Store by Duration in-app to walk the plan history and visualize the regression. If a forced plan is in place but failing to apply, SQL is already silently falling back to the regressed plan — examine the force-failure reason in Query Store, because forcing the same plan again will not help.",
            Remediation:
                "Forcing the historical-better plan is the fast fix while you investigate why the worse plan was chosen — the engine attaches a ready-to-run force statement with the IDs filled in. **Confirm the better plan is still better against current data before forcing** — schema changes, data growth, or statistics updates may have made the old plan stale. Run both plans against representative parameter values first. Forcing is reversible: the snippet includes a commented unforce line for back-out. For the durable fix, address the root cause — stale statistics, a parameter-sensitivity swing on a new value, or a recently-dropped index are the usual culprits.");

        /* The static fallback for SAME_STATEMENT_PILEUP (#3467). The detector freezes value-stated
           advice — the measured session count, elapsed floor, waits, and baseline — into StoryText at
           detection time, so this block serves the frozen path's two fallbacks: legacy rows and the
           co-fired title lookup. Kept mechanism-true rather than value-true for that reason. */
        t[SameStatementPileupDetector.RootFactKey] = new AdviceBlock(
            Headline:
                "Same-statement pileup — concurrent copies of one normally-fast statement all stuck at once on IO waits",
            Investigation:
                "The active-query snapshot caught several sessions running the SAME statement at one instant, every copy seconds deep on a statement whose recent snapshots showed sub-second in-flight observations, with at least one copy suspended on an IO-class wait. That is the live signature of a shared parameterized plan flipping to an IO-heavy shape: every caller inherits the flip simultaneously, the copies convoy on page reads, and client command timeouts follow within seconds. The drill-down quotes the piled-up sessions (elapsed, waits, reads) and the sub-second baseline observations. This finding is computed entirely from the active-query snapshot collector — deliberately, because on some deployments the Query Store readers are disabled (#2296) and a QS-keyed detector would be blind exactly where this shape hurts.",
            Remediation:
                "Evict the statement's current plan (DBCC FREEPROCCACHE with the plan handle, or sp_recompile on the object) — this incident class self-clears when the plan churns out, and eviction forces the churn now. Where Query Store is readable, the next analysis pass's PLAN_REGRESSION finding confirms the flip with plan history and offers the durable fix (forcing the historically-good plan). Recurring episodes fold onto one incident id; a statement that keeps re-drawing its bad plan on cache churn needs the parameter-sensitivity playbook (recompile hints, plan guides, or a rewrite that splits the sensitive predicate).");

        // ─────────────────────────────────────────────────────────────────
        // DB config
        // ─────────────────────────────────────────────────────────────────

        t["DB_CONFIG"] = new AdviceBlock(
            Headline:
                "Database-level configuration issues detected — one or more databases have settings that cause measurable harm",
            Investigation:
                "The per-database breakdown is attached, naming the specific problems on each (auto_shrink ON, auto_close ON, RCSI off, page verify below CHECKSUM). Each issue has a different cost: AUTO_SHRINK causes catastrophic fragmentation by repeatedly shrinking and re-growing; AUTO_CLOSE adds connection-time overhead on every first query against an idle database; page verify below CHECKSUM weakens torn-page detection on modern storage; RCSI off makes reader/writer blocking unavoidable. Open Configuration → Database Configuration in-app for the full per-database state.",
            Remediation:
                "Mechanical fixes for the unambiguous ones: `ALTER DATABASE <db> SET AUTO_SHRINK OFF` — always. `ALTER DATABASE <db> SET AUTO_CLOSE OFF` — always for any database with real workload. `ALTER DATABASE <db> SET PAGE_VERIFY CHECKSUM` — always on modern storage. RCSI is the only nuanced one: enabling it eliminates reader/writer blocking but adds version-store overhead to writes — test on a copy first if the application uses NOLOCK hints or relies on default isolation semantics in surprising ways. The amplifier on this finding boosts severity when LCK_M_S/LCK_M_IS are also high; that combination is the strong signal RCSI would actually help.");

        t["FILE_AUTOGROWTH_PERCENT"] = new AdviceBlock(
            Headline:
                "Large file(s) growing in percentage steps",
            Investigation:
                "The offending files are attached (database, logical name, type, size, configured percent). Percentage growth on a large file means an ever-larger single allocation — 10% of a 200 GB file is a 20 GB extend, and every transaction that triggers it waits for the whole allocation. Log growths are always zeroed, so a large percentage log growth stalls every writer until it finishes.",
            Remediation:
                "Switch the flagged files to a fixed-MB autogrowth so each growth is bounded and predictable: 1024 MB for data files, 64 MB for log files. A ready-to-run ALTER statement per file is attached that applies exactly that. This is a metadata-only change and is safe to run online.");

        // ─────────────────────────────────────────────────────────────────
        // Server config (WS3) — one advisory per per-setting CONFIG_* root key
        // ─────────────────────────────────────────────────────────────────

        t["CONFIG_MAXDOP"] = new AdviceBlock(
            Headline:
                "MAXDOP is 0 — a single query can fan out across every scheduler (up to 64)",
            Investigation:
                "`max degree of parallelism` at 0 lets one query use all available schedulers, up to a hard limit of 64 processors. On a server with more cores than a single NUMA node, that lets one large query monopolize schedulers and drive up CXPACKET / SOS_SCHEDULER_YIELD; on a small box — a single NUMA node of 8 or fewer logical processors — MAXDOP 0 is within Microsoft's guidance and is far less likely to be the problem. Open Configuration → Server Configuration to see the value alongside the socket/core layout. Note this is the SERVER default: MAXDOP can be overridden per database with `ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = N`, which takes precedence for queries running in that database — so if the parallelism is concentrated in one database, set it there rather than changing the instance default. (The analysis engine scores only the server default today; per-database overrides live in the database scoped configuration.)",
            Remediation:
                "Set MAXDOP from CPU topology, not edition — the SKU is irrelevant to the right value. Microsoft's guidance keys on logical processors per NUMA node: on a single NUMA node keep MAXDOP at or under the processor count, capped at 8; only on multi-NUMA hardware with more than 16 logical processors per node does it go higher (half the per-node count, max 16). The Apply button sets it to this server's cores-per-socket capped at 8 — cores-per-socket is the NUMA-node proxy, since NUMA node count isn't collected — followed by RECONFIGURE, an online metadata change. On a box with more than 16 logical processors per NUMA node you can raise it by hand. Raise Cost Threshold for Parallelism (the companion finding, if it fired) in the same pass.");

        t["CONFIG_CTFP"] = new AdviceBlock(
            Headline:
                "Cost Threshold for Parallelism is at (or below) the default 5",
            Investigation:
                "`cost threshold for parallelism` is the optimizer-cost cutoff above which a query gets a parallel plan. The shipped default of 5 is from the 1990s: on modern hardware it sends trivial queries parallel, paying thread-coordination overhead (CXPACKET) for no gain.",
            Remediation:
                "Raise CTFP to 50 as a starting point (then tune up if CXPACKET persists on genuinely large queries). The Apply button runs `sp_configure 'cost threshold for parallelism', 50` + RECONFIGURE — an online metadata change. Pair it with a sane MAXDOP (the companion finding).");

        t["CONFIG_MAX_MEMORY_MB"] = new AdviceBlock(
            Headline:
                "max server memory is unconfigured — SQL Server can consume all the RAM",
            Investigation:
                "`max server memory (MB)` is at its ~2 PB default, so SQL Server will grow its buffer pool until the OS is under memory pressure — which can page out SQL's own working set and destabilize the whole host (and any other instances). Open Server Configuration for the current value and the server's total physical RAM.",
            Remediation:
                "Cap max server memory below total RAM, leaving headroom for the OS, the SQL Server thread stacks, and anything else on the box (a common starting point is total RAM minus 4 GB for the OS plus ~1 GB per 4 GB beyond 16 GB, but the right number depends on what else runs here). This is intentionally NOT auto-applied — the correct value is workload- and host-specific. Copy the statement, set the value you've chosen, and run `sp_configure 'max server memory (MB)', <your MB>` + RECONFIGURE.");

        t["CONFIG_MIN_MAX_MEMORY_NARROW"] = new AdviceBlock(
            Headline:
                "min server memory is pinned near max server memory",
            Investigation:
                "`min server memory (MB)` is set within ~20% of `max server memory (MB)`. min server memory is a floor SQL will not release BELOW once reached — pinning it near max means SQL effectively never gives memory back to the OS, which starves other processes and defeats the OS's ability to reclaim under pressure. The finding states the configured min and max.",
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

        // #3653 A10 (Q2, and slice two): the static fallback for CONFIG_CHANGED — a finding persisted before
        // the composer existed, or a read that cannot find the fact. The composed block (ComposeConfigChanged)
        // states the setting, the values, the observation time and the compare; this one only says what the
        // family is — all three of them, since slice two the fact covers a server setting, a database option
        // or a trace flag (or several observed at one connect).
        t[ConfigChangeAttribution.FactKey] = new AdviceBlock(
            Headline:
                "A configuration setting changed inside the analysis window — a server setting, a database option or a trace flag",
            Investigation:
                "A configuration snapshot (taken on each connect) observed a value that differs from the previous snapshot's — a sys.configurations setting, a sys.databases option on one database, or a trace flag enabled or disabled — and the engine compared the four hours before the change with the four hours after it, banding each metric's move on the server's own dispersion where a baseline exists and on the scorer's ladder otherwise. A server setting is anchored on the default trace's sp_configure line (msg 15457) when the store holds one for that option; every other change is anchored on the snapshot that first observed it, and the prose says which. The frozen finding text states what changed, old → new, when it changed or was first observed, and which metrics moved beyond their band — or that none did. `get_server_config_changes`, `get_database_config_changes` and `get_trace_flag_changes` list the change; `compare_analysis` reruns the compare over any pair of windows.",
            Remediation:
                "An attribution, not an accusation: a before/after around one change is not a causal test. If a metric moved the wrong way and stays moved on later passes, the change is the first suspect; `audit_config` grades a new server value against guidance, the DB_CONFIG advisory grades the database options this engine has an opinion on, and `get_trace_flags` lists what is on now. If nothing moved, there is nothing to do.");

        // ─────────────────────────────────────────────────────────────────
        // Query-plan advisories (WS4) — advise-only: missing indexes, plan warnings.
        // Parsed from the already-collected plans of the top queries by cost. The
        // specific suggested indexes / warnings are in the finding's drill-down detail;
        // no Apply (index + query changes are judgement calls, tested per workload).
        // ─────────────────────────────────────────────────────────────────

        // #3805: the card is CORROBORATION, never a diagnosis and never the engine's headline. It opens with the
        // one fixed caveat sentence (MissingIndexCaveat, byte-identical to the plan tools' per-row caveat) in
        // both the static and the composed block, and FactScorer roots it at the Information rung, below every
        // standing misconfiguration — a request never outranks a setting that is wrong today.
        t["MISSING_INDEX"] = new AdviceBlock(
            Headline:
                "The optimizer asked for indexes that don't exist — top queries are scanning where they could seek",
            Investigation:
                MissingIndexCaveat + " SQL Server records a missing-index request in the query plan whenever the optimizer believes an index it couldn't find would have materially lowered a query's cost. This finding parsed the actual plans of your most expensive queries and collected those requests; the drill-down lists each one with its table, the optimizer's estimated impact %, and the suggested CREATE INDEX. Treat them as a STARTING POINT, not a prescription: the engine's suggestions are naive — it proposes one index per query in isolation, often with the key column order wrong, with wide INCLUDE lists, and with no awareness of indexes you already have or of the write cost. Weigh each by impact AND by how often the query actually runs.",
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
                "Open the Running Jobs tab in-app: the engine shows each currently-running job with its current duration alongside historical average and p95, with jobs above their normal duration flagged. Common patterns: a maintenance job (CHECKDB, index rebuild) that overlapped into business hours, a job stuck on a blocked spid (BLOCKING_EVENTS will co-fire), or a job whose data volume grew beyond what its design assumed. SCH_M co-elevation usually means a long index-maintenance job is now blocking the regular workload. For long-window history, `msdb.dbo.sysjobs` and `sysjobhistory` carry job-history detail beyond the engine's analysis window.",
            Remediation:
                "If the job is still running, you can decide kill-vs-wait: killing mid-CHECKDB or mid-rebuild is safe — both unwind cleanly, you just redo the work in a real maintenance window. But this finding is about the recurrence, not a one-off kill: for chronically-long jobs the workload has usually outgrown the implementation: CHECKDB on a 10 TB database isn't a job, it's a project. Partial substitutes: `WITH PHYSICAL_ONLY` for the daily check plus a less-frequent full check, partitioning so maintenance runs on one partition at a time, incremental statistics so updates touch one partition. For jobs blocked behind blocking, the blocking is the real problem — work the BLOCKING_EVENTS playbook.");

        t["DISK_SPACE"] = new AdviceBlock(
            Headline:
                "Disk free space is below the healthy-headroom threshold — risk of database growth being denied",
            Investigation:
                "Below 10% free is concerning, below 5% is critical. Open File I/O → File I/O Latency or the TempDB tab to see per-file growth over the analysis window. The Database Sizes view (FinOps) has per-file totals, auto-growth settings, and the volume free-space numbers. Common shapes: tempdb growth from a runaway spilling query (QUERY_SPILLS will co-fire and name it), a log file that has not been backed up (FULL recovery without log backups means the log cannot truncate and grows indefinitely), or a data file with overly-aggressive auto-growth that filled the volume.",
            Remediation:
                "Free space first, root cause second. For FULL-recovery databases with bloated logs, take a log backup — that's the only way to truncate. For tempdb that grew from a one-time spill event, `DBCC SHRINKFILE` is one of the few legitimate uses of shrink (do it after the spilling query is fixed, or it'll just regrow). For data files, expand the volume if business-as-usual growth filled it. Going forward: set fixed-MB auto-growth (not percentage), pre-size based on observed growth, and confirm Instant File Initialization is granted to the service account so growths don't block writes while the file is zeroed.");

        t["BAD_ACTOR"] = new AdviceBlock(
            Headline:
                "One query is dominating workload cost — its frequency × per-execution impact rank it as the top offender",
            Investigation:
                "The offending query is attached with its database, text, execution count, and per-execution CPU, elapsed, reads, spills, and DOP, plus a live plan analysis carrying the warnings and missing-index suggestions the optimizer generated. Shape matters: a query running 100,000 times at 5 ms is a parameterisation or caching opportunity; one running once at 30 seconds is a single-query tuning project. Open Queries → Top Queries by Duration in-app and search for the query for its full plan history.",
            Remediation:
                "Match the fix to the shape. High-frequency lightweight: does the WHERE clause have a covering index? Can the application cache results so it doesn't ask 100,000 times per minute? Is parameterisation working, or is every execution compiling? Low-frequency heavyweight: this is a tuning project — apply the missing-index suggestions from the attached plan analysis, rewrite subqueries to joins (or back), update statistics on the driving tables. The attached plan analysis lists the optimizer's own missing-index suggestions with their estimated impact and CREATE statements; treat those as the starting point, not the answer.");

        // ─────────────────────────────────────────────────────────────────
        // Anomaly facts (first-class)
        // ─────────────────────────────────────────────────────────────────

        t["ANOMALY_CPU_SPIKE"] = new AdviceBlock(
            Headline:
                "CPU is anomalously elevated compared to its baseline for this time bucket",
            Investigation:
                "The anomaly detector compares this window's CPU to the same hour-of-week historical baseline over 30 days; 2σ trips the warning, 4σ trips critical. The baseline context — deviation, ratio, baseline mean, sample count — is in the finding. The exact peak timestamp and the sessions active around it are attached. Open the CPU tab and zoom to that peak to see the SQL vs. other-process split. Common drivers: a recently-cached bad plan (check whether PLAN_REGRESSION also fired), a one-off workload (marketing campaign, backfill job), or another process on the host.",
            Remediation:
                "Confirm one-time vs. sustained. Transient (a deploy, an ad-hoc report) needs no action beyond noting it — the anomaly framing means 'unusual right now', not 'automatically bad'. If the elevation persists, the threshold-based CPU_SQL_PERCENT finding will fire on the next analysis window and the standard CPU-pressure playbook applies: tune the top CPU queries, guard parallelism, and force any regressed plans.");

        t["ANOMALY_BLOCKING_SPIKE"] = new AdviceBlock(
            Headline:
                "Blocking events are anomalously elevated compared to baseline — a ratio above 3x normal rate",
            Investigation:
                "Ratio-based scoring: 3x baseline = 0.5 score, 10x = 1.0, so this finding means the blocking rate is well above the workload's normal pattern for this hour-of-week bucket. When BLOCKING_EVENTS also surfaced data, the top chains are attached, including whether the head was sleeping (the abandoned-transaction signature). Open Blocking → Blocked Process Reports and zoom to the window to see whether the rate is climbing or a one-shot spike. A sudden ratio spike often points to a recent deploy, schema change, or new query pattern that introduced fresh contention.",
            Remediation:
                "First question: what changed? Recent deploy, schema migration, new query pattern, job running outside its normal window. If a sleeping head topped the chain, that is the abandoned-transaction signature — but this is a report on a window that has passed, so the chain has cleared and a KILL buys nothing; fix the code path that left a BEGIN TRAN open on its error/timeout path (SET XACT_ABORT ON) so it stops recurring. For systemic increases without an obvious change, the BLOCKING_EVENTS standard playbook applies: RCSI for reader/writer waits, shorter transactions for writer/writer, index tuning to reduce lock-hold duration on the slow operations everyone is queueing behind.");

        t["ANOMALY_DEADLOCK_SPIKE"] = new AdviceBlock(
            Headline:
                "Deadlock rate is anomalously elevated — a sudden jump in deadlocks compared to normal",
            Investigation:
                "Deadlocks normally run at a low steady-state rate determined by the workload's transaction patterns; a ratio spike against the hour-of-week baseline means a new deadlock-prone interaction was just introduced. The most recent victims are attached. Open Blocking → Deadlocks and zoom to the window — the new pattern will dominate the list, each row has the full graph XML viewable in-app, and the rate trend shows whether it is still climbing.",
            Remediation:
                "Find the new pattern in the graphs and fix the interaction — almost always by enforcing consistent object access order between the two procedures involved, or by shortening the transaction so the window for the deadlock is smaller. If a recent deploy correlates with the spike, the new code is the prime suspect — review the latest changes to the procedures named in the deadlock graphs. For reader/writer deadlocks specifically (LCK_M_S victims in the graph), RCSI on the affected database eliminates the entire class.");

        t["ANOMALY_READ_LATENCY"] = new AdviceBlock(
            Headline:
                "Read latency is anomalously elevated compared to its baseline — storage was slower than normal during this window",
            Investigation:
                "Hour-of-week baseline comparison: reads are slower than they usually are at this specific time bucket. When scoring also surfaced data, the per-file latency breakdown names which files are affected — data, log, or tempdb. Open File I/O → File I/O Latency and zoom to the window for the trend. Two shapes: storage-tier anomalies (another tenant on the SAN, a backup job that overlapped) usually resolve on their own; workload-tier anomalies (a new scan-heavy query, a missing-index regression) persist until the workload is fixed.",
            Remediation:
                "Check the attached top queries (when CPU is co-elevated) for unusually high logical reads per execution — if one stands out, that's the workload-tier cause, and fixing the index or the query catches the storage back up. If the queries all look normal but latency is up, it's a storage-layer issue: check with the storage team about other tenants, scheduled jobs (backups, replication, snapshots) on the same infrastructure. Transient storage-side anomalies need only monitoring; sustained ones become an IO_READ_LATENCY_MS threshold finding on the next window and the standard playbook applies.");

        t["ANOMALY_WRITE_LATENCY"] = new AdviceBlock(
            Headline:
                "Write latency is anomalously elevated compared to its baseline — disk writes were slower than normal during this window",
            Investigation:
                "Same shape as the read-latency anomaly, for writes. Log file write latency is usually the most consequential — every commit blocks on WRITELOG, so even a brief elevation hurts throughput. When scoring surfaced it, the per-file latency breakdown names the slow files; open File I/O → File I/O Latency for the trend. WRITELOG co-elevation means the workload was committing through the slow log during this window. Storage-side events — a SAN snapshot, a failover, a backup that overlapped — often correlate with anomalous write latency.",
            Remediation:
                "If a specific external event correlates (overlapping backup, SAN maintenance window), let it pass — that's expected behaviour with no SQL-side fix. If the elevation is sustained without an obvious external cause, the storage layer needs investigation: the log file should live on its own low-latency device. Workload-side, a sudden burst of large writes (bulk insert, big update) can drive transient write latency anomalies — if the burst recurs, the WRITELOG playbook applies (batch writes, separate log storage, delayed durability for non-financial workloads).");

        t["ANOMALY_BATCH_REQUESTS"] = new AdviceBlock(
            Headline:
                "Batch requests per second are anomalously elevated — the server is fielding far more queries than usual",
            Investigation:
                "Batch rate is the rawest measure of workload, so a sigma spike means traffic surged for this hour-of-week bucket. Open the Perfmon tab and add Batch Requests/sec for the trend. When CPU also spiked, the sessions active at the peak are attached; the high-frequency queries, by execution count, are the ones to rank. Common drivers: an application loop that broke (chatty retries, polling that should be event-driven), a new caller hitting the database directly, or a legitimate burst (marketing event, batch import).",
            Remediation:
                "Confirm intentional vs. broken. Misbehaving application — uncached lookups, retry storms, polling instead of subscribing — is fixed at the source; no amount of SQL tuning will help a server hit with 100,000 unnecessary requests per second. Legitimate burst that's the new normal: scale the workload through app-layer caching, connection pooling, or splitting reads to a replica. If the new load level persists going forward, it'll appear as a new BAD_ACTOR or CPU finding on subsequent analysis windows and the standard playbooks apply.");

        t["ANOMALY_SESSION_SPIKE"] = new AdviceBlock(
            Headline:
                "Active session count is anomalously elevated — far more connections than normal for this time bucket",
            Investigation:
                "Session-count spikes usually mean clients are holding connections open longer than expected. Open the Session Stats sub-tab under Resource Metrics (Dashboard) to see the trend with the top application and host names already aggregated. Typical causes: a long-blocked workload (BLOCKING_EVENTS or BLOCKING_CHAIN co-elevation confirms it), connection-pool exhaustion in the app, or a runaway client opening connections without closing them.",
            Remediation:
                "If the count climbed because workers are blocked, blocking is the real problem — sessions free themselves as blocking resolves and the BLOCKING_EVENTS playbook applies. If a specific application is accumulating connections without releasing (the Session Stats top-app text will name it), the connection-pool configuration there is wrong: pool size, idle timeout, or an outright leak. Watch for sustained session counts approaching `max user connections` or the worker-thread limit — at that point the spike is an availability problem, not just a workload anomaly.");

        t["ANOMALY_QUERY_DURATION"] = new AdviceBlock(
            Headline:
                "Query duration is anomalously elevated — the median or P95 query is slower than baseline for this time bucket",
            Investigation:
                "Duration anomalies are workload-shape signals: either the same queries are running slower, or a different mix is running than usual. Open Queries → Top Queries by Duration and zoom to the window, sorted by elapsed time for the leaders. If one specific query's duration jumped, PLAN_REGRESSION and PARAMETER_SENSITIVITY are the right places to look — both will surface the root cause cleanly with the drill-down already populated.",
            Remediation:
                "Track down the slow query through the standard channels: its plan, statistics, indexes. If multiple queries got slower at once, the host itself is slower — co-elevated CPU, I/O, or memory findings should pinpoint which resource. Duration anomalies without any other resource finding almost always mean blocking; check the Blocking → Blocked Process Reports view and the BLOCKING_EVENTS fact even if it didn't score above its own threshold this window.");

        t["ANOMALY_MEMORY_PRESSURE"] = new AdviceBlock(
            Headline:
                "Server memory usage is anomalously elevated vs. baseline — SQL's total memory has moved unusually against its target for this time bucket",
            Investigation:
                "This anomaly compares the ratio of `Total Server Memory` to `Target Server Memory` (the two memory counters the collectors store) against its hour-of-week baseline. A spike usually means the OS forced SQL's target down under external memory pressure, or the buffer pool grew unusually fast. Open Memory → Overview to see total vs. target and the buffer pool across the window, and Memory → Memory Clerks to see where the bytes went. If MEMORY_GRANT_PENDING co-fired, grant pressure is part of the story. QUERY_SPILLS co-elevation means queries are running with grants too small and spilling to tempdb.",
            Remediation:
                "Match the fix to the shape. If total server memory dropped below target, the OS is reclaiming memory from SQL — check whether `max server memory` is capped too low and whether another process on the host is the aggressor; on a dedicated box, Lock Pages in Memory (the CONFIG_LPIM_DISABLED finding) prevents the paging. If the buffer pool grew fast and grants are pending, an offender is consuming a too-large grant from a bad cardinality estimate — its plan shows the estimate-vs-actual divergence, and FULLSCAN statistics or a filtered index fixes it. Anomalies that resolve on their own are typically one-time reporting queries; sustained ones become standard RESOURCE_SEMAPHORE or memory-grant findings on the next window.");

        t["ANOMALY_WAIT_PROFILE"] = new AdviceBlock(
            Headline:
                "The server's wait profile shifted — its overall wait rate is anomalously elevated vs. baseline for this time bucket",
            Investigation:
                "This anomaly compares the whole-server all-types wait rate (ms of wait per second) against its hour-of-week baseline, rather than any single wait type — so a shift driven by a minority-but-real wait (RESOURCE_SEMAPHORE, a lock mode, ASYNC_NETWORK_IO) surfaces even while CXPACKET or SOS dominate the raw totals. The named contributors are the wait types that made up most of the window's wait time. Open the Wait Stats tab and zoom to the window to see the full breakdown and each type's trajectory.",
            Remediation:
                "Chase the leading contributor with its normal playbook — the profile shift only tells you the server spent unusually long waiting this window, not what to do about it. If the elevated profile persists across analysis windows, the threshold-based finding for the dominant wait type fires and the standard advice for that wait applies.");

        t["ANOMALY_OBJECT_GROWTH"] = new AdviceBlock(
            Headline:
                "A table grew sharply day-over-day — its reserved size jumped well above its recent footprint",
            Investigation:
                "Computed from the two most recent daily object-size snapshots: a table's total reserved size (all its indexes summed) rose by more than the trip threshold in both percentage and absolute terms. Open FinOps → Object Sizes & Growth and sort by Growth 30d / Daily Rate to see the trend and the next-largest growers. Distinguish a one-time load (an archive import, a backfill, an index rebuild that temporarily doubles space) from sustained organic growth — the daily-rate column over several days tells you which.",
            Remediation:
                "If it's expected load, no action beyond capacity awareness — confirm the volume has headroom (FinOps → Database Sizes shows volume free space). If it's unexpected: check for a runaway process inserting without cleanup, a disabled or broken purge job, an index rebuild leaving the old allocation, or a heap that needs a clustered index. For genuinely large, fast-growing tables, evaluate PAGE compression and partitioning. Persistent steep growth against limited volume free space is the early warning for an out-of-space outage — act before the file fills.");

        t["ANOMALY_OBJECT_CONTENTION"] = new AdviceBlock(
            Headline:
                "An index accrued significant new lock-wait time — contention on this object jumped since the last snapshot",
            Investigation:
                "Computed from consecutive daily object snapshots (same instance start time, so not a counter reset): an index's cumulative row-lock wait time grew by more than the trip threshold day-over-day. Open FinOps → Locking & Contention to see this object and the other top-contended indexes, and cross-reference Blocking → Blocked Process Reports for the same window to see the actual chains. Check whether a lock-escalation spike accompanied it — escalation to a table lock serializes the whole object.",
            Remediation:
                "Find the queries hitting this object (Query Performance filtered to the database/table) and reduce how long they hold locks: shorten transactions, add the missing index so writers touch fewer rows, or batch large modifications. Reader/writer contention (S vs X) is eliminated by RCSI on the database. If lock escalation is the driver, either fix the query touching too many rows or, as a last resort, disable escalation on the specific table (ALTER TABLE ... SET (LOCK_ESCALATION = DISABLE)) after confirming memory headroom for the extra row locks.");

        return t;
    }
}
