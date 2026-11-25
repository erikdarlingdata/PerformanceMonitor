/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9535: the runner's three bounded plan fetch phases (query_stats' deferred fetch, procedure_stats' plan phase and
/// procedure_stats' deferred fetch), driven through a fake target that records every statement sent on the connection
/// and answers each key of a fetch command with a plan carrying that key's own plan handle. What these pin is the
/// runner's own wiring of <see cref="BoundedPlanFetch"/>: each row gets its own plan across several chunks, a plan
/// skipped on its own goes last next run, and the lock timeout is put back whatever ends the phase.
/// </summary>
public sealed class PlanFetchRunnerTests
{
    private const int Keys = 45;

    private static readonly DateTime s_now = new(2031, 5, 1, 12, 0, 0, DateTimeKind.Utc);

    private sealed class SlowPlan : Exception
    {
    }

    /// <summary>
    /// A SQL Server target that answers SET LOCK_TIMEOUT, records every statement in the order it arrives, and answers a
    /// fetch command with one row per key in it: the key's ordinal and a plan that names the key's plan handle.
    /// </summary>
    private sealed class Target : ITargetProvider
    {
        public List<string> Statements { get; } = [];

        /// <summary>The plan handles (hex, no prefix) each fetch command held, in the order the commands arrived.</summary>
        public List<string[]> Fetches { get; } = [];

        /// <summary>Handles whose plan takes longer than the command timeout: a command holding one ends in a command timeout.</summary>
        public HashSet<string> Slow { get; } = [];

        /// <summary>Raised by a fetch command that is not otherwise answered, by the command's number from 1.</summary>
        public Func<int, Exception?>? FailFetch { get; set; }

        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => throw new NotSupportedException();

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new Command(this, query.Text);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) =>
            exception is SlowPlan ? CollectorTargetFault.CommandTimeout : CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();

        public static string PlanOf(string handle) => "<ShowPlanXML Handle=\"0x" + handle + "\"/>";

        public bool Restored => Statements.Count > 0 && Statements[^1] == BoundedPlanFetch.RestoreLockTimeout.Text;

        private DbDataReader Answer(string text)
        {
            var handles = Regex.Matches(text, @"\((\d+), '?0x([0-9A-Fa-f]+)")
                .Select(m => (Ord: int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture), Handle: m.Groups[2].Value))
                .ToList();
            Statements.Add("FETCH");
            Fetches.Add(handles.Select(h => h.Handle).ToArray());

            if (handles.Any(h => Slow.Contains(h.Handle)))
            {
                throw new SlowPlan();
            }

            if (FailFetch?.Invoke(Fetches.Count) is { } failure)
            {
                throw failure;
            }

            var table = new DataTable();
            table.Columns.Add("ord", typeof(int));
            table.Columns.Add("query_plan_xml", typeof(string));
            table.Columns.Add("query_plan_xml_bytes", typeof(long));
            foreach (var (ord, handle) in handles)
            {
                var plan = PlanOf(handle);
                table.Rows.Add(ord, plan, (long)plan.Length);
            }

            return table.CreateDataReader();
        }

        private sealed class Command(Target owner, string text) : DbCommand
        {
            [System.Diagnostics.CodeAnalysis.AllowNull]
            public override string CommandText { get; set; } = text;
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection? DbConnection { get; set; }
            protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
            protected override DbTransaction? DbTransaction { get; set; }
            public override void Cancel() { }

            public override int ExecuteNonQuery()
            {
                Assert.StartsWith("SET LOCK_TIMEOUT", CommandText, StringComparison.Ordinal);
                owner.Statements.Add(CommandText);
                return -1;
            }

            public override object? ExecuteScalar() => throw new NotSupportedException();
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => owner.Answer(CommandText);

            protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return Task.FromResult(owner.Answer(CommandText));
            }
        }
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    private static DarlingCollectorRunner NewRunner() => new(
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x"), new CollectorDeltaCalculator());

    private static ServerRuntime Server() => new()
    {
        Config = new MonitoredServer { Name = "fetch-test", Host = "fetch-test" },
        ConnectionString = "Server=fetch-test;Initial Catalog=master",
        Target = new CollectorTargetInfo(),
        StorageName = "fetch-test",
        ServerId = 77,
    };

    private static CollectorContext Context(bool defer, bool identity) => new()
    {
        ServerId = 77,
        ServerName = "fetch-test",
        CollectionTime = s_now,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo(),
        CapturePlanXml = true,
        DeferPlanXmlFetch = defer,
        PlanIdentityColumns = identity,
    };

    /// <summary>The plan handle of row <paramref name="n"/>, as the main read ships it: 0x then hex.</summary>
    private static string HandleText(int n) => "0x05" + n.ToString("X8", System.Globalization.CultureInfo.InvariantCulture);

    private static string HandleHex(int n) => HandleText(n)[2..];

    private static List<QueryStatsCollector.Row> QueryStatsRows() => Enumerable.Range(0, Keys)
        .Select(n => new QueryStatsCollector.Row
        {
            PlanHandle = HandleText(n),
            DatabaseName = "app",
            CreationTime = s_now,
            PlanGenerationNum = 1,
            StatementStartOffset = 0,
            StatementEndOffset = -1,
        })
        .ToList();

    private static List<ProcedureStatsCollector.Row> ProcedureRows() => Enumerable.Range(0, Keys)
        .Select(n => default(ProcedureStatsCollector.Row) with
        {
            DatabaseName = "app",
            SchemaName = "dbo",
            ObjectName = "p" + n.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ObjectType = "P",
            CachedTime = s_now,
            SqlHandle = "0x03" + n.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            PlanHandle = HandleText(n),
            PlanStatementCount = 1,
            PlanLastStatementCompile = s_now,
            PlanGenerationSum = 1,
        })
        .ToList();

    public enum Site
    {
        QueryStatsDeferredFetch,
        ProcedureStatsPlanPhase,
        ProcedureStatsDeferredFetch,
    }

    /// <summary>Runs one site over <see cref="Keys"/> rows and returns the plan each row ended with, by row.</summary>
    private static async Task<string?[]> RunAsync(Site site, DarlingCollectorRunner runner, Target target, CancellationToken cancellationToken)
    {
        switch (site)
        {
            case Site.QueryStatsDeferredFetch:
            {
                var rows = QueryStatsRows();
                await runner.FetchDeferredQueryStatsPlansAsync(target, null!, Server(), Context(defer: true, identity: false), rows, cancellationToken);
                return rows.Select(r => r.QueryPlanXml).ToArray();
            }

            case Site.ProcedureStatsPlanPhase:
            {
                var rows = ProcedureRows();
                await runner.RunProcedureStatsPlanPhaseAsync(target, null, null!, Server(), Context(defer: false, identity: false), rows, cancellationToken);
                return rows.Select(r => r.QueryPlanXml).ToArray();
            }

            default:
            {
                var rows = ProcedureRows();
                await runner.ApplyProcedureStatsPlanReuseAsync(
                    target, null!, Server(), Context(defer: true, identity: true), rows, ProcedureStatsPlanFetchMode.On, cancellationToken);
                return rows.Select(r => r.QueryPlanXml).ToArray();
            }
        }
    }

    [Theory]
    [InlineData(Site.QueryStatsDeferredFetch)]
    [InlineData(Site.ProcedureStatsPlanPhase)]
    [InlineData(Site.ProcedureStatsDeferredFetch)]
    public async Task FortyFiveKeys_GoOutInThreeChunks_AndEveryRowGetsItsOwnPlanAndNoOthers(Site site)
    {
        var target = new Target();

        var plans = await RunAsync(site, NewRunner(), target, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 20, 20, 5 }, target.Fetches.Select(f => f.Length).ToArray());
        for (var n = 0; n < Keys; n++)
        {
            Assert.Equal(Target.PlanOf(HandleHex(n)), plans[n]);
        }
    }

    [Theory]
    [InlineData(Site.QueryStatsDeferredFetch, "success")]
    [InlineData(Site.QueryStatsDeferredFetch, "failure")]
    [InlineData(Site.QueryStatsDeferredFetch, "cancel")]
    [InlineData(Site.ProcedureStatsPlanPhase, "success")]
    [InlineData(Site.ProcedureStatsPlanPhase, "failure")]
    [InlineData(Site.ProcedureStatsPlanPhase, "cancel")]
    [InlineData(Site.ProcedureStatsDeferredFetch, "success")]
    [InlineData(Site.ProcedureStatsDeferredFetch, "failure")]
    [InlineData(Site.ProcedureStatsDeferredFetch, "cancel")]
    public async Task TheLockTimeoutIsPutBack_WhetherThePhaseFinishesFailsOrIsCancelled(Site site, string ending)
    {
        using var stop = new CancellationTokenSource();
        var target = new Target
        {
            FailFetch = number =>
            {
                if (number != 2)
                {
                    return null;
                }

                if (ending == "cancel")
                {
                    stop.Cancel();
                    return new OperationCanceledException(stop.Token);
                }

                return ending == "failure" ? new InvalidOperationException("not a timeout") : null;
            },
        };

        var run = RunAsync(site, NewRunner(), target, stop.Token);
        if (ending == "cancel")
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        }
        else
        {
            await run;
        }

        Assert.Equal(BoundedPlanFetch.SetLockTimeout.Text, target.Statements[0]);
        Assert.True(target.Restored, "the statement that puts the default back is the last one sent");
        Assert.Equal(1, target.Statements.Count(s => s == BoundedPlanFetch.RestoreLockTimeout.Text));
    }

    [Fact]
    public async Task AQueryStatsPlanThatTimesOutOnItsOwn_GoesLastOnTheNextRun_AndTheOthersAreNotHeldUp()
    {
        var runner = NewRunner();
        var slow = HandleHex(3);
        var target = new Target();
        target.Slow.Add(slow);

        var first = await RunAsync(Site.QueryStatsDeferredFetch, runner, target, TestContext.Current.CancellationToken);

        Assert.Null(first[3]);
        Assert.Equal(44, first.Count(p => p is not null));
        target.Fetches.Clear();

        var second = await RunAsync(Site.QueryStatsDeferredFetch, runner, target, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 20, 20, 4, 1 }, target.Fetches.Select(f => f.Length).ToArray());
        Assert.Equal(new[] { slow }, target.Fetches[^1]);
        Assert.All(target.Fetches.Take(3), f => Assert.DoesNotContain(slow, f));
        Assert.Equal(44, second.Count(p => p is not null));
    }

    [Fact]
    public async Task AProcedureStatsPlanThatTimesOutOnItsOwn_GoesLastOnTheNextRun_AndTheOthersAreNotHeldUp()
    {
        var runner = NewRunner();
        var slow = HandleHex(3);
        var target = new Target();
        target.Slow.Add(slow);

        var first = await RunAsync(Site.ProcedureStatsPlanPhase, runner, target, TestContext.Current.CancellationToken);

        Assert.Null(first[3]);
        Assert.Equal(44, first.Count(p => p is not null));
        target.Fetches.Clear();

        var second = await RunAsync(Site.ProcedureStatsPlanPhase, runner, target, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 20, 20, 4, 1 }, target.Fetches.Select(f => f.Length).ToArray());
        Assert.Equal(new[] { slow }, target.Fetches[^1]);
        Assert.All(target.Fetches.Take(3), f => Assert.DoesNotContain(slow, f));
        Assert.Equal(44, second.Count(p => p is not null));
    }
}
