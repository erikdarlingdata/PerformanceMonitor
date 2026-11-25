/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9529: query_snapshots renders at most <see cref="QuerySnapshotsCollector.MaxPlansPerRun"/> estimated and live plans
/// a run and renders one estimated plan for all the requests that share a plan key. These run the collector's real batch
/// and its own <c>ReadAsync</c> against a SQL Server whose requests are blocked behind one held row lock, so each
/// statement stays running, with its scan open, while the batch reads it, and they assert what each row comes back with.
///
/// <para><b>What the live plan needs a server for.</b> Inside an APPLY, <c>sys.dm_exec_query_statistics_xml</c> given a
/// NULL session id answers with the plan of the last session it was asked for, so the batch keeps a NULL out of the
/// function with a WHERE as well as with the argument. Only the server can say that a row past the cap, or a request with
/// no plan handle, comes back with no live plan and never with another session's.</para>
///
/// <para><b>Quiet server.</b> The cap and the sharing count every request on the target, so each test skips itself when
/// the batch returns a request that is not one of its own sessions. Run it against a lab or dev server.</para>
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. Each test creates one
/// scratch table in tempdb, drops it, and holds its connections unpooled. <c>DARLING_TEST_SQL_READONLY=1</c> skips the
/// class: it creates a table, and a server that holds real monitoring data must not get one.</para>
/// </summary>
public sealed class QuerySnapshotsLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live query_snapshots plan tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private const string NotQuietMessage =
        "The server has running requests besides this test's sessions, so the plan cap and the plan sharing counts are not this test's to assert. Run it against a lab or dev server.";

    private static string? ConnectionString(string applicationName)
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = applicationName,
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Sessions that each run one statement and stay running, blocked behind a row lock this object holds, with the
    /// statement's scan open. Each distinct statement reads its own scratch table, so the table name in a plan says which
    /// statement the plan is of, and sessions given the same statement number run the same text, so they share a plan key.
    /// </summary>
    private sealed class Pileup : IAsyncDisposable
    {
        private readonly string _adminConnectionString;
        private readonly string _suffix;
        private readonly IReadOnlyList<int> _tables;
        private readonly SqlConnection _holder;
        private readonly List<SqlConnection> _sessions = new();
        private readonly List<Task> _running = new();

        private Pileup(string adminConnectionString, string suffix, IReadOnlyList<int> tables, SqlConnection holder, string programName)
        {
            _adminConnectionString = adminConnectionString;
            _suffix = suffix;
            _tables = tables;
            _holder = holder;
            ProgramName = programName;
        }

        /// <summary>The program name every session of this pileup connects with, which is how its rows are told apart.</summary>
        public string ProgramName { get; }

        /// <summary>What a statement's text and its plans name: the end of its table's name.</summary>
        public static string MarkerFor(int statement) => "_s" + statement.ToString("D2", CultureInfo.InvariantCulture) + "]";

        private string Table(int statement) => "tempdb.dbo.[pm9529_" + _suffix + MarkerFor(statement);

        /// <summary>One session per entry of <paramref name="statements"/>, each running the statement of that number.</summary>
        public static async Task<Pileup> StartAsync(string baseConnectionString, IReadOnlyList<int> statements, CancellationToken ct)
        {
            var suffix = Guid.NewGuid().ToString("N")[..8];
            var programName = "pm-9529-live-" + suffix;
            var admin = new SqlConnectionStringBuilder(baseConnectionString) { ApplicationName = "pm-9529-live-admin-" + suffix }.ConnectionString;
            var tables = statements.Distinct().ToArray();

            var holder = new SqlConnection(admin);
            var pileup = new Pileup(admin, suffix, tables, holder, programName);
            try
            {
                await ExecuteAsync(
                    admin,
                    string.Concat(tables.Select(t => "CREATE TABLE " + pileup.Table(t) + " (c int NOT NULL); INSERT " + pileup.Table(t) + " (c) VALUES (1);")),
                    ct);

                await holder.OpenAsync(ct);
                await using (var hold = new SqlCommand(
                    "BEGIN TRANSACTION; " + string.Concat(tables.Select(t => "UPDATE " + pileup.Table(t) + " SET c = c WHERE c = 1;")), holder))
                {
                    await hold.ExecuteNonQueryAsync(ct);
                }

                var sessionString = new SqlConnectionStringBuilder(baseConnectionString) { ApplicationName = programName }.ConnectionString;
                foreach (var statement in statements)
                {
                    var connection = new SqlConnection(sessionString);
                    pileup._sessions.Add(connection);
                    await connection.OpenAsync(ct);

                    var command = new SqlCommand("SELECT COUNT(*) FROM " + pileup.Table(statement) + ";", connection) { CommandTimeout = 300 };
                    pileup._running.Add(Task.Run(async () =>
                    {
                        await using (command)
                        {
                            await command.ExecuteScalarAsync(CancellationToken.None);
                        }
                    }, CancellationToken.None));
                }

                await pileup.WaitUntilBlockedAsync(statements.Count, ct);
                return pileup;
            }
            catch
            {
                await pileup.DisposeAsync();
                throw;
            }
        }

        /* Waiting on the held row lock with a plan handle, which is the state the batch reads. A later session waits behind
           the first waiter, not behind the holder, so the wait type is what says it is blocked. */
        private async Task WaitUntilBlockedAsync(int expected, CancellationToken ct)
        {
            var deadline = DateTime.UtcNow.AddSeconds(90);
            while (true)
            {
                await using var command = new SqlCommand(
                    "SELECT COUNT_BIG(*) FROM sys.dm_exec_requests AS r JOIN sys.dm_exec_sessions AS s ON s.session_id = r.session_id "
                    + "WHERE s.program_name = @program AND r.wait_type LIKE N'LCK[_]M[_]%' AND r.plan_handle IS NOT NULL;",
                    _holder);
                command.Parameters.AddWithValue("@program", ProgramName);
                if ((long)(await command.ExecuteScalarAsync(ct))! == expected)
                {
                    return;
                }

                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException("The sessions did not all reach a blocked state with a plan.");
                }

                await Task.Delay(200, ct);
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_holder.State == System.Data.ConnectionState.Open)
                {
                    await using var release = new SqlCommand("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;", _holder);
                    await release.ExecuteNonQueryAsync(CancellationToken.None);
                }
            }
            finally
            {
                foreach (var task in _running)
                {
                    try
                    {
                        await task;
                    }
                    catch (SqlException)
                    {
                    }
                }

                foreach (var session in _sessions)
                {
                    await session.DisposeAsync();
                }

                await _holder.DisposeAsync();
                await ExecuteAsync(
                    _adminConnectionString,
                    string.Concat(_tables.Select(t => "DROP TABLE IF EXISTS " + Table(t) + ";")),
                    CancellationToken.None);
            }
        }
    }

    private static CollectorContext MakeContext(QuerySnapshotPlanCache? cache = null, IReadOnlyList<string>? excludedDatabases = null) => new()
    {
        ServerId = 1,
        ServerName = "live-sql",
        CollectionTime = DateTime.UtcNow,
        Deltas = null!,
        SnapshotPlanCache = cache,
        ExcludedDatabases = excludedDatabases ?? Array.Empty<string>(),
    };

    /// <summary>One row of the collector's own query, read by <c>ReadAsync</c>, with the run's measurements.</summary>
    private static async Task<(List<QuerySnapshotsCollector.Row> Rows, CollectorContext Context)> RunCollectorAsync(
        string connectionString, QuerySnapshotPlanCache? cache, CancellationToken ct, IReadOnlyList<string>? excludedDatabases = null)
    {
        var context = MakeContext(cache, excludedDatabases);
        var query = QuerySnapshotsCollector.Instance.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, ct);
        return (rows, context);
    }

    /// <summary>What the target returned for one request, before <c>ReadAsync</c> gave any row a shared or held plan.</summary>
    private sealed record RawRow(int SessionId, string? ProgramName, string? QueryText, string? QueryPlan, string? LiveQueryPlan, int? PlanState);

    /// <summary>The collector's own query read column by column, the way the live snapshot button reads its query.</summary>
    private static Task<List<RawRow>> RunRawAsync(string connectionString, string sql, CancellationToken ct)
        => ReadRawAsync(connectionString, new CollectorQuery(sql), ct);

    private static async Task<List<RawRow>> ReadRawAsync(string connectionString, CollectorQuery query, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using DbDataReader reader = await command.ExecuteReaderAsync(ct);

        var rows = new List<RawRow>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new RawRow(
                Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                reader.IsDBNull(22) ? null : reader.GetString(22),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetValue(5).ToString(),
                reader.FieldCount > 35 && !reader.IsDBNull(35) ? Convert.ToInt32(reader.GetValue(35), CultureInfo.InvariantCulture) : null));
        }

        return rows;
    }

    private static long? Measured(CollectorContext context, string label)
    {
        foreach (var measurement in context.Measurements)
        {
            if (string.Equals(measurement.Label, label, StringComparison.Ordinal))
            {
                return measurement.Value;
            }
        }

        return null;
    }

    /// <summary>The marker a plan names, or null when it names none. A plan carries the markers of its own statement only.</summary>
    private static string? MarkerIn(string? plan)
        => plan is null ? null : Enumerable.Range(0, 100).Select(Pileup.MarkerFor).FirstOrDefault(m => plan.Contains(m, StringComparison.Ordinal));

    /// <summary>The marker the statement a session runs carries, from the statement text the row itself came back with.</summary>
    private static string OwnMarker(QuerySnapshotsCollector.Row row)
        => MarkerIn(row.QueryText) ?? throw new InvalidOperationException("The row's statement text names no marker.");

    [Fact]
    public async Task TwoSessionsOnTwoStatements_EachRowCarriesTheLivePlanAndTheEstimatedPlanOfItsOwnStatement()
    {
        var connectionString = ConnectionString("pm-9529-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var pileup = await Pileup.StartAsync(connectionString!, new[] { 1, 2 }, ct);
        var (rows, context) = await RunCollectorAsync(connectionString!, new QuerySnapshotPlanCache(), ct);
        Assert.SkipWhen(rows.Any(r => r.ProgramName != pileup.ProgramName), NotQuietMessage);

        Assert.Equal(2, rows.Count);
        foreach (var row in rows)
        {
            var own = OwnMarker(row);
            Assert.Equal(own, MarkerIn(row.LiveQueryPlan));
            Assert.Equal(own, MarkerIn(row.QueryPlan));

            /* The plan carries the query hash of the request it was read for, which is the row's own. */
            Assert.NotNull(row.QueryHash);
            Assert.Contains("QueryHash=\"" + row.QueryHash + "\"", row.LiveQueryPlan, StringComparison.Ordinal);
        }
        Assert.Equal(2, rows.Select(r => r.QueryHash).Distinct().Count());

        Assert.Equal(new[] { Pileup.MarkerFor(1), Pileup.MarkerFor(2) }, rows.Select(OwnMarker).OrderBy(m => m, StringComparer.Ordinal));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.LivePlansOverCapMeasurement));
    }

    [Fact]
    public async Task SessionsPastTheCap_CarryNoPlanOfEitherKind_AndNeverAnotherSessionsPlan_AndTheRunCountsThem()
    {
        var connectionString = ConnectionString("pm-9529-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var cap = QuerySnapshotsCollector.MaxPlansPerRun;
        var statements = Enumerable.Range(0, cap + 2).ToArray();
        await using var pileup = await Pileup.StartAsync(connectionString!, statements, ct);
        var (rows, context) = await RunCollectorAsync(connectionString!, new QuerySnapshotPlanCache(), ct);
        Assert.SkipWhen(rows.Any(r => r.ProgramName != pileup.ProgramName), NotQuietMessage);

        Assert.Equal(cap + 2, rows.Count);

        /* Twenty rows have each plan, and a row that has one has the plan of its own statement. */
        Assert.Equal(cap, rows.Count(r => r.QueryPlan is not null));
        Assert.Equal(cap, rows.Count(r => r.LiveQueryPlan is not null));
        foreach (var row in rows)
        {
            var own = OwnMarker(row);
            Assert.True(row.QueryPlan is null || MarkerIn(row.QueryPlan) == own, "an estimated plan is its own statement's");
            Assert.True(row.LiveQueryPlan is null || MarkerIn(row.LiveQueryPlan) == own, "a live plan is its own statement's");
        }

        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
        Assert.Equal(2, Measured(context, QuerySnapshotsCollector.LivePlansOverCapMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansSharedMeasurement));
    }

    [Fact]
    public async Task SessionsOnOneStatement_HaveOnePlanRendered_AndEveryRowCarriesIt()
    {
        var connectionString = ConnectionString("pm-9529-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        /* Five sessions run statement 1, one runs statement 2: two plan keys. */
        await using var pileup = await Pileup.StartAsync(connectionString!, new[] { 1, 1, 1, 1, 1, 2 }, ct);

        var raw = await RunRawCollectorAsync(connectionString!, ct);
        Assert.SkipWhen(raw.Any(r => r.ProgramName != pileup.ProgramName), NotQuietMessage);
        Assert.Equal(6, raw.Count);
        Assert.Equal(2, raw.Count(r => r.QueryPlan is not null));
        Assert.Equal(2, raw.Count(r => r.PlanState == 0));
        Assert.Equal(4, raw.Count(r => r.PlanState == 4));
        Assert.All(raw.Where(r => r.PlanState == 4), r => Assert.Null(r.QueryPlan));

        var (rows, context) = await RunCollectorAsync(connectionString!, new QuerySnapshotPlanCache(), ct);
        Assert.Equal(6, rows.Count);
        Assert.All(rows, row => Assert.Equal(OwnMarker(row), MarkerIn(row.QueryPlan)));

        /* The live plan is a reading of one request and is not shared: every row has its own. */
        Assert.All(rows, row => Assert.Equal(OwnMarker(row), MarkerIn(row.LiveQueryPlan)));
        Assert.Equal(4, Measured(context, QuerySnapshotsCollector.PlansSharedMeasurement));
        Assert.Null(Measured(context, QuerySnapshotsCollector.PlansOverCapMeasurement));
    }

    [Fact]
    public async Task ADatabaseInTheExclusionList_LeavesItsRequestsOutOfTheResult()
    {
        var connectionString = ConnectionString("pm-9529-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        /* The sessions run in master. The exclusion sits inside the ranked request set, so the batch has to run with it. */
        await using var pileup = await Pileup.StartAsync(connectionString!, new[] { 1, 2 }, ct);

        var (excluded, _) = await RunCollectorAsync(connectionString!, new QuerySnapshotPlanCache(), ct, new[] { "master" });
        Assert.DoesNotContain(excluded, r => r.ProgramName == pileup.ProgramName);

        var (kept, _) = await RunCollectorAsync(connectionString!, new QuerySnapshotPlanCache(), ct, new[] { "msdb" });
        Assert.SkipWhen(kept.Any(r => r.ProgramName != pileup.ProgramName), NotQuietMessage);
        Assert.Equal(2, kept.Count);
        Assert.All(kept, row => Assert.Equal(OwnMarker(row), MarkerIn(row.QueryPlan)));
        Assert.All(kept, row => Assert.Equal(OwnMarker(row), MarkerIn(row.LiveQueryPlan)));
    }

    [Fact]
    public async Task TheDefaultBuilder_GivesEveryRowItsOwnEstimatedAndLivePlan_WithNoCapAndNoSharing()
    {
        var connectionString = ConnectionString("pm-9529-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        /* More statements than the cap, and three sessions on the first of them. */
        var statements = Enumerable.Range(0, QuerySnapshotsCollector.MaxPlansPerRun + 2).Concat(new[] { 0, 0 }).ToArray();
        await using var pileup = await Pileup.StartAsync(connectionString!, statements, ct);

        var raw = await RunRawAsync(connectionString!, QuerySnapshotsCollector.BuildSnapshotQuery(supportsLiveQueryPlan: true, isAzureSqlDatabase: false), ct);
        Assert.SkipWhen(raw.Any(r => r.ProgramName != pileup.ProgramName), NotQuietMessage);

        Assert.Equal(statements.Length, raw.Count);
        Assert.All(raw, r => Assert.NotNull(r.QueryPlan));
        Assert.All(raw, r => Assert.NotNull(r.LiveQueryPlan));
        Assert.All(raw, r => Assert.Equal(MarkerIn(r.QueryText), MarkerIn(r.QueryPlan)));
        Assert.All(raw, r => Assert.Equal(MarkerIn(r.QueryText), MarkerIn(r.LiveQueryPlan)));
    }

    /// <summary>The collector's own query as the scheduled path builds it, with a host cache that holds nothing yet, read column by column.</summary>
    private static Task<List<RawRow>> RunRawCollectorAsync(string connectionString, CancellationToken ct)
        => ReadRawAsync(connectionString, QuerySnapshotsCollector.Instance.BuildQuery(MakeContext(new QuerySnapshotPlanCache())), ct);
}
