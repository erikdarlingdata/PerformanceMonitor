using System;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// What kind of thing a fact's collector source read. A source is the collector's own statement about that, so
/// asking it never drifts the way a hand-kept list of fact or finding keys does (#6132).
/// </summary>
public static class FactKinds
{
    /// <summary>
    /// True for the settings sources: the SQL Server <c>config</c> and <c>database_config</c> collectors and the
    /// PostgreSQL <see cref="PgTargetSources.ConfigSource"/> and <see cref="PgTargetSources.PostureSource"/> ones.
    /// Their facts are read from the latest row whatever the analysis window, so they hold the same value at every
    /// hour of the week: no measured symptom is emitted on a settings source. Everything else (waits, queries,
    /// blocking, memory and the rest) is a time series.
    /// </summary>
    public static bool IsSettingsSource(string? source) =>
        source is "config" or "database_config"
        || string.Equals(source, PgTargetSources.ConfigSource, StringComparison.Ordinal)
        || string.Equals(source, PgTargetSources.PostureSource, StringComparison.Ordinal);

    /// <summary>
    /// True for a source that reads a standing state: every settings source (<see cref="IsSettingsSource"/>) and
    /// <c>disk</c>, the free-space level of a volume (#7416). A standing state holds at every hour of the week, so
    /// a story ROOTED in one has no schedule to repeat on. A time series that spikes in the same hour each week
    /// is not one: it has an hour in it.
    /// </summary>
    public static bool IsStandingStateSource(string? source) =>
        IsSettingsSource(source) || string.Equals(source, "disk", StringComparison.Ordinal);

    /// <summary>
    /// The same question asked of a persisted finding's root fact KEY, for the readers that hold a row and not
    /// the pass's facts (#7416). True for free space, the engine's standing-advisory roots (a setting, a missing
    /// index, plan-cache bloat, see <c>InferenceEngine.IsConfigAdvisoryRoot</c>) and the settings keys the engine
    /// does not root on their own: the <c>CONFIG_</c> and <c>SERVER_</c> families, trace flags, database size and
    /// the PostgreSQL posture and version keys. No running query can explain such a card, and none of them keeps a
    /// schedule.
    /// </summary>
    public static bool IsStandingStateKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
            return false;

        return key is "DISK_SPACE" or "TRACE_FLAGS" or "DATABASE_TOTAL_SIZE_MB" or "DB_CONFIG" or "PG_SERVER_MAJOR_VERSION"
            || key.StartsWith("CONFIG_", StringComparison.Ordinal)
            || key.StartsWith("SERVER_", StringComparison.Ordinal)
            || key.StartsWith("PG_POSTURE_", StringComparison.Ordinal)
            || InferenceEngine.IsConfigAdvisoryRoot(key);
    }
}
