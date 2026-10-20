/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6206: which query each PostgreSQL collector sends to a target of a given major version, and of a given
/// pg_stat_statements version. The old-version texts were also run against real PostgreSQL 10 and 12 servers; these
/// tests hold the choice itself, so a gate cannot move without a failing test.
/// </summary>
public class PgOldVersionQueryTests
{
    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }

    private static CollectorContext Context(int major, string? statementsVersion = null) => new()
    {
        ServerId = 1,
        ServerName = "pg",
        CollectionTime = DateTime.UtcNow,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major, PostgresVersionNum = major * 10000 },
        PgExtensionVersions = statementsVersion is null ? null : new Dictionary<string, string> { ["pg_stat_statements"] = statementsVersion },
    };

    [Theory]
    [InlineData(10, false)]
    [InlineData(11, false)]
    [InlineData(12, true)]
    [InlineData(17, true)]
    [InlineData(0, true)]
    public void TheMaterializedKeywordIsSentOnlyToPostgres12AndNewer(int major, bool keeps)
    {
        const string sql = "WITH a AS MATERIALIZED (SELECT 1), b AS NOT MATERIALIZED (SELECT 2) SELECT * FROM a, b";

        var result = PgVersionSql.ForMajor(sql, major);

        Assert.Equal(keeps, result.Contains("MATERIALIZED", StringComparison.Ordinal));
        if (!keeps)
        {
            Assert.Equal("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b", result);
        }
    }

    [Theory]
    [InlineData(10)]
    [InlineData(12)]
    public void EveryLogTailQueryIsFreeOfMaterializedBelow12Only(int major)
    {
        foreach (var text in new[]
        {
            PgServerLogTail.TailCteSql, PgServerLogTail.TailCteBinarySql, PgServerLogTail.TailCsvCteSql,
            PgServerLogTail.TailCsvCteBinarySql, PgServerLogTail.TailJsonCteSql, PgServerLogTail.TailJsonCteBinarySql,
        })
        {
            var query = PgServerLogTail.WithResume(text, Context(major));

            Assert.Equal(major >= 12, query.Text.Contains("MATERIALIZED", StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData("1.4", 1, 4)]
    [InlineData("1.11", 1, 11)]
    [InlineData("1.8devel", 1, 8)]
    [InlineData("2", 2, 0)]
    public void AnExtensionVersionParsesToItsNumber(string text, int major, int minor)
        => Assert.Equal(new Version(major, minor), PgVersionSql.ParseExtensionVersion(text));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("devel")]
    public void AnUnreadableExtensionVersionIsUnknown(string? text)
        => Assert.Null(PgVersionSql.ParseExtensionVersion(text));

    [Theory]
    [InlineData(10, null, false, false, false, false)]
    [InlineData(12, null, false, false, false, false)]
    [InlineData(13, null, true, false, false, false)]
    [InlineData(14, null, true, true, false, false)]
    [InlineData(16, null, true, true, false, false)]
    [InlineData(17, null, true, true, true, true)]
    [InlineData(0, null, true, true, true, true)]
    // The extension's own version wins over the server major: an old extension kept through an upgrade.
    [InlineData(16, "1.7", false, false, false, false)]
    [InlineData(17, "1.8", true, false, false, false)]
    [InlineData(12, "1.8", true, false, false, false)]
    public void TheStatementsLayoutFollowsTheExtensionThenTheMajor(
        int major, string? extversion, bool execNames, bool toplevel, bool sharedBlk, bool statsSince)
    {
        var layout = PgVersionSql.StatementsLayout.For(major, extversion);

        Assert.Equal(execNames, layout.HasExecTimeNames);
        Assert.Equal(execNames, layout.HasWal);
        Assert.Equal(toplevel, layout.HasToplevel);
        Assert.Equal(sharedBlk, layout.HasSharedBlkTimeNames);
        Assert.Equal(statsSince, layout.HasStatsSince);
    }

    [Fact]
    public void StatementStatsBeforeExtension18ReadsTheOldTimeColumnsAndNullsTheWal()
    {
        var sql = PgStatementStatsCollector.Instance.BuildQuery(Context(12)).Text;

        Assert.Matches(@"\btotal_time\s+AS total_exec_time\b", sql);
        Assert.Matches(@"\bmin_time\s+AS min_exec_time\b", sql);
        Assert.Matches(@"\bmean_time\s+AS mean_exec_time\b", sql);
        Assert.Matches(@"NULL::bigint\s+AS wal_bytes\b", sql);
        Assert.DoesNotMatch(@"\btotal_exec_time\s+AS total_exec_time\b", sql);
        Assert.Matches(@"\btrue\s+AS toplevel\b", sql);
    }

    [Fact]
    public void StatementStatsOnANewServerWithAnOldExtensionStillReadsTheOldColumns()
    {
        var sql = PgStatementStatsCollector.Instance.BuildQuery(Context(16, "1.7")).Text;

        Assert.Matches(@"\btotal_time\s+AS total_exec_time\b", sql);
        Assert.Contains("AS toplevel", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("wal_bytes::bigint", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void StatementStatsOnExtension18ReadsTheNewColumnNames()
    {
        var sql = PgStatementStatsCollector.Instance.BuildQuery(Context(13)).Text;

        Assert.Matches(@"\btotal_exec_time\s+AS total_exec_time\b", sql);
        Assert.Contains("wal_bytes::bigint", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(12, "total_time", "true")]
    [InlineData(13, "total_exec_time", "true")]
    [InlineData(14, "total_exec_time", "s.toplevel")]
    public void TheStatementTextFetchUsesTheColumnsOfTheExtensionVersion(int major, string timeColumn, string toplevel)
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, major, Context(major));

        Assert.Contains($"s.{timeColumn} AS total_exec_time", sql, StringComparison.Ordinal);
        Assert.Contains($"AND   {toplevel}", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStatementTextFetchPrefersTheExtensionVersionOverTheMajor()
    {
        var sql = PgStatementText.FetchSqlFor(isAurora: false, 16, Context(16, "1.7"));

        Assert.Contains("s.total_time AS total_exec_time", sql, StringComparison.Ordinal);
        Assert.Contains("AND   true", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(10, false, false)]
    [InlineData(12, false, false)]
    [InlineData(13, true, false)]
    [InlineData(14, true, true)]
    public void ReplicationSlotsNameOnlyTheColumnsTheMajorHas(int major, bool walStatus, bool twoPhase)
    {
        var sql = PgReplicationSlotsCollector.BuildQueryText(major);

        Assert.Equal(walStatus, sql.Contains("s.wal_status", StringComparison.Ordinal));
        Assert.Equal(walStatus, sql.Contains("s.safe_wal_size", StringComparison.Ordinal));
        Assert.Equal(twoPhase, sql.Contains("s.two_phase", StringComparison.Ordinal));
        Assert.Contains("AS two_phase", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(10, false)]
    [InlineData(12, false)]
    [InlineData(13, true)]
    [InlineData(17, true)]
    public void SessionStatesNameLeaderPidOnlyOnPostgres13AndNewer(int major, bool usesLeaderPid)
    {
        var sql = PgSessionStatesCollector.BuildQueryText(major);

        Assert.Equal(usesLeaderPid, sql.Contains("a.leader_pid", StringComparison.Ordinal));
        Assert.Equal(!usesLeaderPid, sql.Contains("a.backend_type IS DISTINCT FROM 'parallel worker'", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(10, false)]
    [InlineData(13, false)]
    [InlineData(14, true)]
    [InlineData(0, true)]
    public void WriteStatsNameThePg14WalViewOnlyWhereItExists(int major, bool namesWalView)
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major };

        var sql = PgWriteStatsCollector.BuildQueryText(major, isAurora: false);

        Assert.True(PgWriteStatsCollector.Instance.AppliesTo(target));
        Assert.Equal(namesWalView, sql.Contains("pg_stat_wal", StringComparison.Ordinal));
        Assert.Contains("pg_catalog.pg_stat_bgwriter", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(10, true)]
    [InlineData(15, true)]
    [InlineData(16, false)]
    [InlineData(18, false)]
    public void IoStatsOnAServerWithoutPgStatIoSaysNotCollectedWithTheVersion(int major, bool gap)
    {
        var message = DarlingMcpPgIoTools.NotCollectedForVersionMessage("a-server", major);

        Assert.Equal(gap, message is not null);
        if (gap)
        {
            Assert.Contains($"PostgreSQL {major}", message, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    public void IoStatsMakesNoVersionClaimWhenTheMajorIsUnknown(int? major)
        => Assert.Null(DarlingMcpPgIoTools.NotCollectedForVersionMessage("a-server", major));
}
