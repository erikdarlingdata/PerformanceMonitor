using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// One hop of a story's path whose fact NAMES something — the application that parked a transaction, the
/// lead-blocker session, the Agent job, the table, the statement — carried on the story so the root's card
/// can say it (#3691, "a CRITICAL leaf consumed mid-chain is never NAMED by the root").
/// </summary>
/// <param name="Key">The hop fact's key, as it appears in <see cref="AnalysisStory.Path"/>.</param>
/// <param name="Identity">The operator-facing name <see cref="FactIdentity.Describe"/> rendered for the fact —
/// short, quoted where it is a name, never a number dressed as prose.</param>
/// <param name="Severity">The hop fact's FINAL severity (after amplifiers), so the root's sentence can rank it the
/// way the engine did.</param>
public sealed record NamedHop(string Key, string Identity, double Severity);

/// <summary>
/// The IDENTITY contract for facts: which fact keys name a thing, and how that name is read off the fact.
///
/// <para><b>The defect this closes (#3691 v2 exit check §WRONG 2).</b> <c>PG_IDLE_IN_TRANSACTION</c> at 1.2
/// names its holder — application, role, database — in its own advice, but when the greedy traversal consumed it
/// as a hop under <c>PG_CONNECTION_SATURATION</c> (1.25) the only card the operator saw was the root's, whose
/// advice never named the holder; <c>grep billing</c> over the whole <c>analyze_server</c> payload found nothing.
/// The SQL Server engine has the same shape: <c>LCK → BLOCKING_CHAIN</c> hides the lead-blocker session behind
/// the wait's card, <c>SCH_M → RUNNING_JOBS</c> the job behind the schema-lock's. The fix is engine-level and
/// engine-neutral: <see cref="InferenceEngine.BuildStories"/> records the identity-bearing hops it consumed on
/// the story (<see cref="AnalysisStory.NamedHops"/>) and <see cref="FactAdvice.PopulateStoryText"/> composes one
/// sentence per hop onto the root's frozen advice, so every surface that renders the root card — the viewer,
/// e-mail, <c>analyze_server</c>, <c>get_analysis_findings</c> — carries the name without any of them changing.</para>
///
/// <para><b>Why a roster over the existing string seams, not a new field on <see cref="Fact"/>.</b> The facts
/// that name something already carry the name: <see cref="Fact.ObjectName"/> (the one string seam the
/// doubles-only <see cref="Fact.Metadata"/> cannot replace — a table, a slot, "application as role"),
/// <see cref="Fact.DatabaseName"/>, the key's own suffix for the per-statement families
/// (<c>BAD_ACTOR_&lt;query_hash&gt;</c>, <c>PG_BAD_ACTOR_&lt;queryid&gt;</c>), and one numeric seam
/// (<c>BLOCKING_CHAIN</c>'s <c>worst_apex_spid</c>). A third string property would be a second copy of the same
/// name that every collector had to remember to fill, and the sixteen collector partials belong to sixteen
/// lanes. What is NOT the same as "has an ObjectName": the wait facts carry their wait event's display name there
/// (the key restated, not a who), and the config-change fact carries the setting names (never a hop — it has no
/// edges). So membership is an explicit, pinned list per engine, and a key joins it by a one-row edit here that
/// says what it names; a fact whose key is on the roster but whose seam is empty (a Lock wait with no top
/// relation, a job fact with no name) has no identity and is simply not named.</para>
///
/// <para><b>What it does not do.</b> It does not change severity, confidence, the path, the story hash or which
/// facts a story consumes — the traversal is untouched. It does not name the root (the root's own advice does).
/// It does not persist anything new: the sentence rides in <c>StoryText</c>, which both finding stores already
/// round-trip; <see cref="AnalysisStory.NamedHops"/> is ephemeral like the rest of the story's typed context. The
/// MCP payload files are untouched — they render the composed advice they always did.</para>
/// </summary>
public static class FactIdentity
{
    /// <summary>
    /// How many named hops a root's advice will carry, highest severity first. Three is the reading budget of
    /// one card: the longest traversal is <see cref="InferenceEngine.MaxPathDepth"/> hops and a pathological
    /// chain naming ten things would bury the root's own investigation under its leaves. The story's
    /// <see cref="AnalysisStory.Path"/> still lists every hop for the reader who wants the rest.
    /// </summary>
    public const int MaxNamedHops = 3;

    /// <summary>The prefix every composed sentence starts with — one spelling, so a test can find the sentence
    /// and a reader can recognise the shape across engines.</summary>
    public const string SentenceMarker = "Behind it:";

    /// <summary>
    /// The SQL Server keys that name a thing (exact keys; <see cref="SqlServerBadActorPrefix"/> is the one
    /// dynamic family). <c>BLOCKING_CHAIN</c> names its lead-blocker session (<c>worst_apex_spid</c>);
    /// <c>RUNNING_JOBS</c> the Agent job running longest past its history (<see cref="Fact.ObjectName"/>,
    /// #3693); the two object anomalies the table they observed. Today's SQL Server graph reaches the first two
    /// as hops (<c>LCK → BLOCKING_CHAIN</c>, <c>SCH_M / WRITELOG / IO_WRITE_LATENCY_MS → RUNNING_JOBS</c>); the
    /// others are rostered because they NAME something, which is this contract's question — not because an edge
    /// reaches them today.
    /// </summary>
    public static readonly IReadOnlyList<string> SqlServerKeys = new[]
    {
        "BLOCKING_CHAIN",
        "RUNNING_JOBS",
        "ANOMALY_OBJECT_GROWTH",
        "ANOMALY_OBJECT_CONTENTION",
    };

    /// <summary>The SQL Server per-statement family, keyed <c>BAD_ACTOR_&lt;query_hash&gt;</c>; the hash IS the name.</summary>
    public const string SqlServerBadActorPrefix = "BAD_ACTOR_";

    /// <summary>
    /// The PostgreSQL-target keys that name a thing, each through the seam its collector partial already fills:
    /// the three session-holder facts carry "application as role" in <see cref="Fact.ObjectName"/> and the
    /// database in <see cref="Fact.DatabaseName"/> (lanes 3/14, 15, 17); the lock-wait rollup its top relation;
    /// the vacuum and bloat facts their table or index; the xmin hold its <c>source:holder</c> subject; the
    /// replication facts the standby's <c>application_name</c> or the slot. <see cref="PgTargetFactKeys.BadActorKeyPrefix"/>
    /// is the dynamic family (<c>queryid</c> is the name).
    /// </summary>
    public static readonly IReadOnlyList<string> PostgresKeys = new[]
    {
        PgTargetFactKeys.IdleInTransaction,
        PgTargetFactKeys.BlockingChain,
        PgTargetFactKeys.LongRunningQuery,
        PgTargetFactKeys.LockWaitEvents,
        PgTargetFactKeys.AutovacuumBacklog,
        PgTargetFactKeys.XminHold,
        PgTargetFactKeys.BloatTrend,
        PgTargetFactKeys.IndexBloatTrend,
        PgTargetFactKeys.ReplicationLag,
        PgTargetFactKeys.SlotRetention,
        PgTargetFactKeys.SlotXmin,
    };

    /// <summary>
    /// True when the key is on either engine's roster (exact or by the two per-statement prefixes). Does not say
    /// the fact HAS an identity — <see cref="Describe"/> answers that, because the seam may be empty.
    /// </summary>
    public static bool IsIdentityBearingKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
            return false;
        if (SqlServerKeys.Contains(key, StringComparer.Ordinal) || PostgresKeys.Contains(key, StringComparer.Ordinal))
            return true;
        return key.StartsWith(SqlServerBadActorPrefix, StringComparison.Ordinal) && key.Length > SqlServerBadActorPrefix.Length
            || key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) && key.Length > PgTargetFactKeys.BadActorKeyPrefix.Length;
    }

    /// <summary>
    /// The operator-facing identity of a fact, or null when the key is not on a roster or its seam is empty.
    /// Names are back-quoted the way the frozen advice already quotes them (<c>Agent job `name`</c>); a
    /// database, when the fact carries one, follows as "on `db`" for a session holder and "in `db`" for an object
    /// or statement — the same prepositions the composers use.
    /// </summary>
    public static string? Describe(Fact? fact)
    {
        if (fact is null || string.IsNullOrEmpty(fact.Key))
            return null;
        var key = fact.Key;
        var subject = Subject(fact);

        if (key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            return subject is null ? null : InDatabase($"statement {subject}", fact.DatabaseName);
        if (key.StartsWith(SqlServerBadActorPrefix, StringComparison.Ordinal))
            return subject is null ? null : InDatabase($"query hash `{subject}`", fact.DatabaseName);

        switch (key)
        {
            case "BLOCKING_CHAIN":
            {
                if (!fact.Metadata.TryGetValue("worst_apex_spid", out var spid) || spid <= 0)
                    return null;
                var sleeping = fact.Metadata.TryGetValue("worst_apex_sleeping", out var s) && s > 0;
                return $"lead-blocker session {((long)Math.Round(spid)).ToString(CultureInfo.InvariantCulture)}{(sleeping ? ", sleeping" : string.Empty)}";
            }
            case "RUNNING_JOBS":
                return subject is null ? null : $"Agent job `{subject}`";
            case "ANOMALY_OBJECT_GROWTH":
            case "ANOMALY_OBJECT_CONTENTION":
                return subject is null ? null : InDatabase($"table `{subject}`", fact.DatabaseName);
        }

        if (key == PgTargetFactKeys.IdleInTransaction || key == PgTargetFactKeys.LongRunningQuery)
            return subject is null ? null : OnDatabase($"`{subject}`", fact.DatabaseName);
        if (key == PgTargetFactKeys.BlockingChain)
            return subject is null ? null : OnDatabase($"lead blocker `{subject}`", fact.DatabaseName);
        if (key == PgTargetFactKeys.LockWaitEvents)
            return subject is null ? null : $"relation `{subject}`";
        if (key == PgTargetFactKeys.AutovacuumBacklog || key == PgTargetFactKeys.BloatTrend)
            return subject is null ? null : InDatabase($"table `{subject}`", fact.DatabaseName);
        if (key == PgTargetFactKeys.IndexBloatTrend)
            return subject is null ? null : InDatabase($"index `{subject}`", fact.DatabaseName);
        if (key == PgTargetFactKeys.XminHold)
            return subject is null ? null : $"horizon holder `{subject}`";
        if (key == PgTargetFactKeys.ReplicationLag)
            return subject is null ? null : $"standby `{subject}`";
        if (key == PgTargetFactKeys.SlotRetention || key == PgTargetFactKeys.SlotXmin)
            return subject is null ? null : InDatabase($"slot `{subject}`", fact.DatabaseName);

        return null;
    }

    /// <summary>
    /// The RAW name the identity is built from — <see cref="Fact.ObjectName"/>, or the key suffix for the two
    /// per-statement families. Null for a numeric seam (<c>BLOCKING_CHAIN</c> reads its spid from metadata in
    /// <see cref="Describe"/> directly).
    /// </summary>
    private static string? Subject(Fact fact)
    {
        if (!string.IsNullOrWhiteSpace(fact.ObjectName))
            return fact.ObjectName.Trim();
        var key = fact.Key ?? string.Empty;
        if (key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) && key.Length > PgTargetFactKeys.BadActorKeyPrefix.Length)
            return key.Substring(PgTargetFactKeys.BadActorKeyPrefix.Length);
        if (key.StartsWith(SqlServerBadActorPrefix, StringComparison.Ordinal) && key.Length > SqlServerBadActorPrefix.Length)
            return key.Substring(SqlServerBadActorPrefix.Length);
        return null;
    }

    /// <summary>
    /// The identity-bearing hops of a traversal path — every node AFTER the root whose fact
    /// <see cref="Describe"/>s to a name — highest severity first (ties keep path order), capped at
    /// <see cref="MaxNamedHops"/>. The root is excluded on purpose: its own advice names it.
    /// </summary>
    public static List<NamedHop> NamedHopsOf(IReadOnlyList<string> path, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        if (path is null || path.Count < 2 || factsByKey is null)
            return [];
        var hops = new List<NamedHop>();
        for (var i = 1; i < path.Count; i++)
        {
            if (!factsByKey.TryGetValue(path[i], out var fact))
                continue;
            var identity = Describe(fact);
            if (identity is null)
                continue;
            hops.Add(new NamedHop(path[i], identity, fact.Severity));
        }
        return hops
            .OrderByDescending(h => h.Severity)
            .Take(MaxNamedHops)
            .ToList();
    }

    /// <summary>
    /// The one sentence a root's investigation gains per named hop, with a leading space so it appends to the
    /// existing prose the way the recurrence and fold sentences do:
    /// <c> Behind it: `PG_IDLE_IN_TRANSACTION` (severity 1.20) — `billing-worker as billing` on `appdb`: billing-worker
    /// as billing idle in transaction for 15 min — …</c>. One shape, always: the identity is the label (it is the
    /// thing this exists to surface, and it carries the database the hop's headline may not), the hop's own
    /// composed <paramref name="headline"/> follows as what the leaf concluded. A composer that names its subject in
    /// its headline (the job composer, the PostgreSQL session composers) therefore says the name twice in one
    /// sentence — accepted over a dedupe rule that would drop the database, or the identity, depending on how a
    /// sibling lane phrased its headline. A hop with no composable advice gets the identity alone.
    /// </summary>
    public static string Sentence(NamedHop hop, string? headline)
    {
        if (hop is null)
            return string.Empty;
        var severity = hop.Severity.ToString("0.00", CultureInfo.InvariantCulture);
        var trimmedHeadline = string.IsNullOrWhiteSpace(headline) ? null : headline.Trim();
        var body = trimmedHeadline is null ? hop.Identity : $"{hop.Identity}: {trimmedHeadline}";
        return $" {SentenceMarker} `{hop.Key}` (severity {severity}) — {body}{(EndsWithTerminator(body) ? string.Empty : ".")}";
    }

    private static bool EndsWithTerminator(string text) =>
        text.Length > 0 && (text[^1] == '.' || text[^1] == '!' || text[^1] == '?');

    private static string InDatabase(string identity, string? databaseName) =>
        string.IsNullOrWhiteSpace(databaseName) ? identity : $"{identity} in `{databaseName}`";

    private static string OnDatabase(string identity, string? databaseName) =>
        string.IsNullOrWhiteSpace(databaseName) ? identity : $"{identity} on `{databaseName}`";
}
