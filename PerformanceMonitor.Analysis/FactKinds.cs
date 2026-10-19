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
}
