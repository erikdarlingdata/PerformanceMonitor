namespace PerformanceMonitor.Exporter;

/// <summary>
/// `--procedure-plan-reuse`: whether `procedure_plan` skips fetching plan XML for a digest whose cached plan
/// (plan_handle + cached_time) was already fetched recently.
/// </summary>
public enum ProcedurePlanReuseMode
{
    /// <summary>Today's behavior exactly. The identity table in the state file is not touched.</summary>
    Off,

    /// <summary>Decide and record identities, but fetch every unknown digest as today. Counts what `On` would skip.</summary>
    Shadow,

    /// <summary>Skip the digests the decision says to skip.</summary>
    On,
}

public static class ProcedurePlanReuseModes
{
    public static readonly string[] Names = ["off", "shadow", "on"];

    public static bool TryParse(string? text, out ProcedurePlanReuseMode mode)
    {
        switch (text)
        {
            case "off": mode = ProcedurePlanReuseMode.Off; return true;
            case "shadow": mode = ProcedurePlanReuseMode.Shadow; return true;
            case "on": mode = ProcedurePlanReuseMode.On; return true;
            default: mode = ProcedurePlanReuseMode.Shadow; return false;
        }
    }

    public static string Name(ProcedurePlanReuseMode mode) => mode switch
    {
        ProcedurePlanReuseMode.Off => "off",
        ProcedurePlanReuseMode.Shadow => "shadow",
        _ => "on",
    };
}

/// <summary>One cached plan of one module: the object plus the plan_handle and cached_time that identify the cached plan.</summary>
public sealed record ProcedurePlanIdentity(
    string? ServerName, string? DatabaseName, string? SchemaName, string? ObjectName, string PlanHandle, DateTime CachedTime);

/// <summary>An identity whose plan XML was fetched at <see cref="LastFetchUtc"/>, for digest <see cref="LastDigest"/> (hex).</summary>
public sealed record ProcedurePlanIdentityFetch(ProcedurePlanIdentity Identity, DateTime LastFetchUtc, string LastDigest);

/// <summary>Why a digest is fetched or skipped. The names are the manifest counters.</summary>
public enum ProcedurePlanReuseClass
{
    /// <summary>plan_handle or cached_time is NULL: no identity, so fetch as today.</summary>
    IdentityNull,

    /// <summary>The identity is not in state: fetch.</summary>
    IdentityNew,

    /// <summary>The identity is known and was last fetched at least the refetch interval ago: fetch.</summary>
    IdentityRefetch,

    /// <summary>The identity is known and was fetched less than the refetch interval ago: skip.</summary>
    IdentitySkip,
}

/// <summary>What <see cref="ProcedurePlanReuse.Evaluate"/> decided for one run's unknown digests.</summary>
public sealed class ProcedurePlanReuseEvaluation
{
    /// <summary>The class of each unknown digest (hex). A digest with several (object, identity) keys is skipped only when every key says skip; otherwise it takes the first of new, refetch, null that applies.</summary>
    public Dictionary<string, ProcedurePlanReuseClass> ClassByDigest { get; } = new(StringComparer.Ordinal);

    /// <summary>The digests (hex) the decision says to skip. In `on` they are not fetched; in `shadow` they are fetched anyway and counted.</summary>
    public HashSet<string> SkipDigests { get; } = new(StringComparer.Ordinal);

    /// <summary>For each identity the decision fetches (new or refetch), the unknown digests (hex) that carry it.</summary>
    public Dictionary<ProcedurePlanIdentity, List<string>> FetchDigestsByIdentity { get; } = new();

    /// <summary>The identities the decision fetched as a refetch (known, last fetched at least the interval ago), mapped to the shape_hash state holds for their previous last_digest (null: no shape known).</summary>
    public Dictionary<ProcedurePlanIdentity, string?> PreviousShapeByRefetchIdentity { get; } = new();

    public long Count(ProcedurePlanReuseClass cls) => ClassByDigest.Values.Count(c => c == cls);

    /// <summary>
    /// What the 6-hour refetch found, per refetched identity: the identity's newly fetched digest (the one
    /// <see cref="IdentitiesToRecord"/> would record, so an identity with no content is not counted) resolves to
    /// <paramref name="shapeOf"/>, compared with the shape state held for its previous last_digest. No previous shape:
    /// <c>PrevUnknown</c>, no comparison.
    /// </summary>
    public (long Changed, long Same, long PrevUnknown) CompareRefetchShapes(Func<string, string?> shapeOf)
    {
        long changed = 0, same = 0, prevUnknown = 0;
        foreach (var (identity, previousShape) in PreviousShapeByRefetchIdentity)
        {
            if (!FetchDigestsByIdentity.TryGetValue(identity, out var digests)) continue;
            var pick = digests.Where(d => shapeOf(d) is not null).OrderBy(d => d, StringComparer.Ordinal).Cast<string?>().FirstOrDefault();
            if (pick is null) continue; // nothing found in the store for this identity
            if (previousShape is null) prevUnknown++;
            else if (string.Equals(previousShape, shapeOf(pick), StringComparison.Ordinal)) same++;
            else changed++;
        }

        return (changed, same, prevUnknown);
    }

    /// <summary>
    /// The identities to record after a successful run: one entry per identity the decision fetches, with the smallest
    /// digest that carried it and whose content was found (<paramref name="hasContent"/>). An identity whose digests
    /// all had no content in the store is not recorded, so it keeps being fetched as today. Shadow records the same
    /// set as `on`, so what shadow counts is what `on` would do next run. An identity the decision skips is never
    /// recorded: its last_fetch_utc stays where it was, so the 6 h window is measured from the last real fetch.
    /// </summary>
    public List<ProcedurePlanIdentityFetch> IdentitiesToRecord(Func<string, bool> hasContent, DateTime nowUtc)
    {
        var result = new List<ProcedurePlanIdentityFetch>();
        foreach (var (identity, digests) in FetchDigestsByIdentity)
        {
            var pick = digests.Where(hasContent).OrderBy(d => d, StringComparer.Ordinal).Cast<string?>().FirstOrDefault();
            if (pick is not null)
            {
                result.Add(new ProcedurePlanIdentityFetch(identity, ProcedurePlanReuse.PhaseSlotStart(identity, nowUtc), pick));
            }
        }

        return result.OrderBy(f => f.LastDigest, StringComparer.Ordinal).ToList();
    }
}

/// <summary>The outcome of <see cref="ProcedurePlanReuse.Plan"/>: the effective mode and, unless it is off, the decisions.</summary>
public sealed class ProcedurePlanReusePlan
{
    public required ProcedurePlanReuseMode Mode { get; init; }

    /// <summary>Null when <see cref="Mode"/> is off.</summary>
    public ProcedurePlanReuseEvaluation? Evaluation { get; init; }

    /// <summary>The digests the walk must not fetch: the skip set in `on`, empty otherwise.</summary>
    public IReadOnlySet<string> DigestsToDrop { get; init; } = new HashSet<string>();
}

/// <summary>
/// The procedure plan reuse decision. Pure logic over the state file and an injected clock: no store access and no
/// <c>DateTime.UtcNow</c>.
/// </summary>
public static class ProcedurePlanReuse
{
    /// <summary>How long a known cached plan is trusted before a new digest under it is fetched again.</summary>
    public static readonly TimeSpan RefetchInterval = TimeSpan.FromHours(6);

    /// <summary>
    /// The fetch time recorded for <paramref name="identity"/> fetched at <paramref name="nowUtc"/>: the start of the
    /// identity's own 6-hour slot that contains nowUtc. Each identity's slots are shifted by a whole number of hours
    /// (0-5) from a stable hash of the identity, so refetches spread over the six hourly runs instead of all landing
    /// in one. Before this, every identity first seen at 19:20Z was refetched together at 01:20Z: one hour fetched
    /// about 4,000 plans (43 MB) while the hours between fetched about 300. The recorded time is never later than the
    /// real fetch, and the next refetch is at the next slot start, so a known plan is still refetched at most 6 h
    /// after its last real fetch. Classify's rule (age &gt;= 6 h) is unchanged.
    /// </summary>
    public static DateTime PhaseSlotStart(ProcedurePlanIdentity identity, DateTime nowUtc)
    {
        var hours = (long)RefetchInterval.TotalHours;
        var phase = (long)(StableHash(identity) % (ulong)hours);
        var hourIndex = nowUtc.Ticks / TimeSpan.TicksPerHour;
        var slotStartHour = ((hourIndex + phase) / hours) * hours - phase;
        return new DateTime(slotStartHour * TimeSpan.TicksPerHour, DateTimeKind.Utc);
    }

    /// <summary>FNV-1a 64 over the identity's fields. Stable across processes and runs (string.GetHashCode is not).</summary>
    internal static ulong StableHash(ProcedurePlanIdentity identity)
    {
        const ulong offset = 14695981039346656037UL, prime = 1099511628211UL;
        var h = offset;
        foreach (var part in new[] { identity.ServerName ?? "\0", identity.DatabaseName ?? "\0", identity.SchemaName ?? "\0",
                     identity.ObjectName ?? "\0", identity.PlanHandle, identity.CachedTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture) })
        {
            foreach (var b in System.Text.Encoding.UTF8.GetBytes(part)) { h ^= b; h *= prime; }
            h ^= 0x1F; h *= prime; // field separator
        }
        return h;
    }

    /// <summary>Identities not fetched for this many days are pruned from state, matching the store's plan retention.</summary>
    public const int IdentityRetentionDays = 14;

    /// <summary>The class for one key: <paramref name="identity"/> null means plan_handle or cached_time was NULL; <paramref name="lastFetchUtc"/> null means the identity is not in state.</summary>
    public static ProcedurePlanReuseClass Classify(ProcedurePlanIdentity? identity, DateTime? lastFetchUtc, DateTime nowUtc)
    {
        if (identity is null) return ProcedurePlanReuseClass.IdentityNull;
        if (lastFetchUtc is null) return ProcedurePlanReuseClass.IdentityNew;
        return nowUtc - lastFetchUtc.Value >= RefetchInterval
            ? ProcedurePlanReuseClass.IdentityRefetch
            : ProcedurePlanReuseClass.IdentitySkip;
    }

    /// <summary>
    /// The whole per-run step: with mode off, or with <paramref name="ignoreState"/>, nothing is read or written (the
    /// identity table is not touched, not even to prune it) and the plan is "off". Otherwise the identity table is
    /// pruned once, and every unknown digest is decided. In `on` the skip set is dropped from the walk.
    /// </summary>
    public static ProcedurePlanReusePlan Plan(
        StateStore stateStore, string sourceStore, ProcedurePlanReuseMode requestedMode, bool ignoreState,
        IReadOnlyCollection<(string DigestHex, ProcedurePlanIdentity? Identity)> unknownKeys, DateTime nowUtc)
    {
        if (requestedMode == ProcedurePlanReuseMode.Off || ignoreState)
        {
            return new ProcedurePlanReusePlan { Mode = ProcedurePlanReuseMode.Off };
        }

        stateStore.PruneOldProcedurePlanIdentities(nowUtc, IdentityRetentionDays);
        var evaluation = Evaluate(stateStore, sourceStore, unknownKeys, nowUtc);
        return new ProcedurePlanReusePlan
        {
            Mode = requestedMode,
            Evaluation = evaluation,
            DigestsToDrop = requestedMode == ProcedurePlanReuseMode.On ? evaluation.SkipDigests : new HashSet<string>(),
        };
    }

    /// <summary>
    /// Decides every key of <paramref name="unknownKeys"/> (one per object and unknown digest, with the identity the key
    /// query picked) against `procedure_plan_identity`. Reads state only; <paramref name="nowUtc"/> is the run time.
    /// </summary>
    public static ProcedurePlanReuseEvaluation Evaluate(
        StateStore stateStore, string sourceStore,
        IReadOnlyCollection<(string DigestHex, ProcedurePlanIdentity? Identity)> unknownKeys, DateTime nowUtc)
    {
        var evaluation = new ProcedurePlanReuseEvaluation();
        var lastFetchByIdentity = stateStore.LookupProcedurePlanIdentities(
            sourceStore, unknownKeys.Where(k => k.Identity is not null).Select(k => k.Identity!));

        var refetchIdentities = new HashSet<ProcedurePlanIdentity>();
        foreach (var group in unknownKeys.GroupBy(k => k.DigestHex, StringComparer.Ordinal))
        {
            var classes = new List<ProcedurePlanReuseClass>();
            foreach (var (_, identity) in group)
            {
                DateTime? last = identity is not null && lastFetchByIdentity.TryGetValue(identity, out var found) ? found : null;
                var cls = Classify(identity, last, nowUtc);
                classes.Add(cls);
                if (identity is not null && cls == ProcedurePlanReuseClass.IdentityRefetch) refetchIdentities.Add(identity);
                if (identity is not null && cls is ProcedurePlanReuseClass.IdentityNew or ProcedurePlanReuseClass.IdentityRefetch)
                {
                    if (!evaluation.FetchDigestsByIdentity.TryGetValue(identity, out var digests))
                    {
                        digests = [];
                        evaluation.FetchDigestsByIdentity[identity] = digests;
                    }

                    if (!digests.Contains(group.Key)) digests.Add(group.Key);
                }
            }

            var digestClass = classes.Contains(ProcedurePlanReuseClass.IdentityNew) ? ProcedurePlanReuseClass.IdentityNew
                : classes.Contains(ProcedurePlanReuseClass.IdentityRefetch) ? ProcedurePlanReuseClass.IdentityRefetch
                : classes.Contains(ProcedurePlanReuseClass.IdentityNull) ? ProcedurePlanReuseClass.IdentityNull
                : ProcedurePlanReuseClass.IdentitySkip;
            evaluation.ClassByDigest[group.Key] = digestClass;
            if (digestClass == ProcedurePlanReuseClass.IdentitySkip)
            {
                evaluation.SkipDigests.Add(group.Key);
            }
        }

        foreach (var (identity, shape) in stateStore.LookupProcedurePlanPreviousShapes(sourceStore, refetchIdentities))
        {
            evaluation.PreviousShapeByRefetchIdentity[identity] = shape;
        }

        return evaluation;
    }
}

/// <summary>
/// Identities fetched by this run's procedure_plan export, held until the whole run succeeds, then written by
/// <see cref="StateStore.RecordExportedKeys"/> in the same transaction as the other keys. Same hand-off as
/// <see cref="PendingKeyRecorder"/>.
/// </summary>
public static class PendingProcedurePlanIdentities
{
    private static readonly Dictionary<string, List<ProcedurePlanIdentityFetch>> ByStore = new();

    public static void Set(string store, List<ProcedurePlanIdentityFetch> identities) => ByStore[store] = identities;

    public static bool Has(string store) => ByStore.TryGetValue(store, out var list) && list.Count > 0;

    /// <summary>Returns and clears this store's pending identities.</summary>
    public static List<ProcedurePlanIdentityFetch> Take(string store) => ByStore.Remove(store, out var list) ? list : [];
}

/// <summary>
/// The procedure plan reuse counters written on the `procedure_plan` manifest entry. <paramref name="IdentitySkipRows"/>
/// (shadow only, null otherwise): rows this run wrote whose example digest `on` would not have fetched, so the
/// shape changes `on` would have delayed by up to the refetch interval. It is the cost side of identity_skip_bytes.
/// </summary>
public sealed record ProcedurePlanReuseStats(
    string Mode, long IdentityNew, long IdentityRefetch, long IdentitySkip, long IdentityNull, long? IdentitySkipBytes,
    long? IdentitySkipRows = null,
    long IdentityRefetchRows = 0, long IdentityNewRows = 0, long IdentityNullRows = 0,
    long IdentityRefetchShapeChanged = 0, long IdentityRefetchShapeSame = 0, long IdentityRefetchPrevUnknown = 0);
