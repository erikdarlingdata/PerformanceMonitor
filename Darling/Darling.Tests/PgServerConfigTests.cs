// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V102 (#2658) — the server's own configuration, from <c>pg_settings</c>.
///
/// <para>The two reads answer questions nothing else in the stack can. "What is this set to" was simply
/// missing on PostgreSQL; "what changed" is worse than missing, because a configuration history that was
/// never recorded cannot be reconstructed from the server afterwards at any price.</para>
/// </summary>
public sealed class PgServerConfigTests
{
    /* ---------------- the rung ---------------- */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal("pg-server-config", PgMigrations.Scripts.Single(s => s.Version == 102).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /* ---------------- the collector ---------------- */

    /// <summary>
    /// Core catalog only, so it runs on every PostgreSQL target rather than being Aurora-gated — the same
    /// tier as the wraparound collector.
    /// </summary>
    [Fact]
    public void TheCollectorRunsOnEveryPostgresTarget_AndReadsOnlyPgSettings()
    {
        Assert.True(PgServerConfigCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 14 }));
        Assert.True(PgServerConfigCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 18, IsAurora = true }));

        var sql = PgServerConfigCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("FROM pg_catalog.pg_settings", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>source</c> is what separates the server's configuration from the collector's own session, so the
    /// collector must STORE it — and must not filter on it. Dropping a session-scoped row at collection
    /// time makes the evidence unrecoverable and leaves every later read guessing; the read does the
    /// filtering, where it can be explained.
    /// </summary>
    [Fact]
    public void TheCollectorStoresSourceAndDoesNotFilterOnIt()
    {
        var columns = PgServerConfigCollector.Instance.PayloadColumns.Select(c => c.Name).ToList();

        Assert.Contains("source", columns);
        Assert.Contains("boot_val", columns);
        Assert.Contains("pending_restart", columns);
        Assert.Contains("context", columns);

        var sql = PgServerConfigCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotContain("WHERE", sql, StringComparison.Ordinal);
    }

    /* ---------------- the reads ---------------- */

    /// <summary>
    /// <c>pg_settings</c> is a per-BACKEND view, so a <c>client</c>- or <c>session</c>-source row describes
    /// the monitoring connection. Presenting one as the server's configuration would be wrong; reporting one
    /// as a CHANGE would be worse, because the collector reconnects and <c>application_name</c> moves, and
    /// the read would announce a configuration change nobody made. Both reads exclude them.
    /// </summary>
    [Fact]
    public void BothReadsExcludeSessionScopedRows()
    {
        Assert.Contains("'client'", DarlingPgServerConfigReader.SessionScopedSources, StringComparison.Ordinal);
        Assert.Contains("'session'", DarlingPgServerConfigReader.SessionScopedSources, StringComparison.Ordinal);

        Assert.Contains("SESSION_SCOPED", DarlingPgServerConfigReader.CurrentConfigSql, StringComparison.Ordinal);
        Assert.Contains("SESSION_SCOPED", DarlingPgServerConfigReader.ConfigChangesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// PostgreSQL's own <c>source</c> decides what counts as default, NOT a text comparison against
    /// <c>boot_val</c>. Measured on a live 17.11 target, the string comparison invents non-defaults on a
    /// server nobody configured: <c>data_directory_mode</c> reads <c>0700</c> against a <c>boot_val</c> of
    /// <c>448</c> — one value in octal and decimal — <c>archive_command</c> reads <c>(disabled)</c> against
    /// an empty default, and <c>commit_timestamp_buffers</c> reads 32 against 0 because 0 means auto-tune.
    /// All three have <c>source = 'default'</c>.
    /// </summary>
    [Fact]
    public void DefaultnessComesFromSource_NotFromComparingTextToBootVal()
    {
        var sql = DarlingPgServerConfigReader.CurrentConfigSql;

        Assert.Contains("(coalesce(c.source, 'default') = 'default') AS is_default", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("c.setting IS NOT DISTINCT FROM c.boot_val", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The current-config read is anchored on the newest snapshot, never on an hours window. A
    /// configuration has no window — it is the state now — and an hours filter would return NOTHING for a
    /// server whose hourly collector last ran just outside it, which reads as "this server has no
    /// configuration" rather than "ask again".
    /// </summary>
    [Fact]
    public void TheCurrentReadAnchorsOnTheNewestSnapshot_NotAWindow()
    {
        var sql = DarlingPgServerConfigReader.CurrentConfigSql;

        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time >=", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A setting APPEARING is not a change, and this guard is the whole difference between a useful read and
    /// a useless one. <c>LAG</c> returns NULL for the first snapshot of every setting, so without it the
    /// first collection would report several hundred fabricated changes — verified on the rig, where the
    /// first snapshot carried 415 settings and the read correctly reported ONE change after a real edit.
    /// It would fire again for every extension whose GUCs appear when the library is loaded.
    /// </summary>
    [Fact]
    public void TheChangesReadIgnoresASettingAppearingForTheFirstTime()
    {
        var sql = DarlingPgServerConfigReader.ConfigChangesSql;

        Assert.Contains("LAG(c.setting) OVER (PARTITION BY c.name ORDER BY c.collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE prev_time IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("setting IS DISTINCT FROM prev_setting", sql, StringComparison.Ordinal);
    }

    private static CollectorContext MakeContext() => new()
    {
        ServerId = 42,
        ServerName = "pg-target",
        CollectionTime = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc),
        Deltas = NoDeltas.Instance,
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = 17,
        },
        ExcludedDatabases = Array.Empty<string>(),
    };

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public static readonly NoDeltas Instance = new();

        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }
}
