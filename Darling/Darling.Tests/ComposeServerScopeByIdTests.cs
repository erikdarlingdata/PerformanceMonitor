/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5525: every composed fact read is scoped to its servers by <c>server_id</c> (the compression segment column, leading the time
/// index), resolved from <c>collect.servers</c> by the scoped names inside the SQL. The old scope, <c>server_name = ANY($n)</c>,
/// read every compressed chunk in full: on a 50-server store the filter removed 124 M rows of one 30 day statement read.
///
/// <para>These pins hold the compiled TEXT (no statement filters a fact table on <c>server_name = ANY</c>) and, live, the ROWS: a
/// multi-server scope returns the same rows by id as by name, a renamed server's whole history is included, and a scoped name that
/// no registry row carries keeps matching what it matched before.</para>
/// </summary>
public sealed class ComposeServerScopeByIdTests
{
    private static readonly DateTime Start = new(2026, 7, 23, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime End = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);
    private static readonly string[] TwoServers = ["SERVER-A", "SERVER-B"];

    /// <summary>The scope as the compiler spells it for names bound at <paramref name="n"/>.</summary>
    private static string ByIdScope(int n) =>
        $"server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY(${n})))";

    private static PanelPlan Plan(string json)
    {
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, []);
        Assert.True(error is null, error);
        return plan!;
    }

    private static ComposeRunContext Context(
        IReadOnlyList<string>? servers, RollupAvailability rollups, IReadOnlyList<string>? unregistered = null) =>
        new(servers, Start, End, ComposeRunContext.NoVariables, rollups, End, RollupCoverage.Unknown, UnregisteredServers: unregistered);

    private const string WaitPanel =
        "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"wait_type\"],\"includeOther\":true,\"viz\":\"line\"}";

    private const string QueryStorePanel =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private const string EdgePanel =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";

    private static readonly DateTime EdgeNow = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);

    private static ComposeCompiled Compile(PanelPlan plan, ComposeRunContext context)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        return compiled!;
    }

    /// <summary>One compiled statement per place the compiler emits the server scope: the raw fact read (ranked, with both passes), the
    /// Query Store dedupe, a CAGG route, the hourly-raw-edges route with its module map overlay, the annotation query and the server
    /// clock read.</summary>
    private static IEnumerable<(string Label, string Sql)> ScopedStatements(IReadOnlyList<string>? unregistered)
    {
        yield return ("raw wait_stats, ranked time series", Compile(Plan(WaitPanel), Context(TwoServers, RollupAvailability.None, unregistered)).Sql);
        yield return ("raw query_store_stats dedupe", Compile(Plan(QueryStorePanel), Context(TwoServers, RollupAvailability.None, unregistered)).Sql);

        var caggPlan = Plan("{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"timeBucket\":\"day\",\"viz\":\"line\"}");
        var cagg = new ComposeRunContext(
            TwoServers, End.AddDays(-120), End.AddDays(-119), ComposeRunContext.NoVariables, RollupAvailability.All, End, RollupCoverage.Unknown,
            UnregisteredServers: unregistered);
        var caggSql = Compile(caggPlan, cagg).Sql;
        Assert.Contains("collect.query_store_stats_corrected_daily AS f", caggSql, StringComparison.Ordinal);
        yield return ("rollup route", caggSql);

        var edgePlan = Plan(EdgePanel);
        var coverage = new RollupCoverage(
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStatsIntervalHourlyView] = EdgeNow.AddDays(-3) },
            new Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All,
            new Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStatsIntervalHourlyView] = EdgeNow.AddMinutes(-20) });
        var edgeStart = EdgeNow.AddHours(-24);
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(edgePlan, EdgeNow, edgeStart, EdgeNow, RollupAvailability.All, coverage);
        Assert.NotNull(candidate);
        var edge = new ComposeRunContext(
            TwoServers, edgeStart, EdgeNow, ComposeRunContext.NoVariables, RollupAvailability.All, EdgeNow, coverage,
            HourlyEdges: new ComposeHourlyEdgesVerdict(candidate!.SourceTable, candidate.HourStartUtc, candidate.HourEndUtc, TwoServers),
            ModuleMapThrough: EdgeNow.AddMinutes(-30), UnregisteredServers: unregistered);
        yield return ("hourly raw edges with module map overlay", Compile(edgePlan, edge).Sql);

        var annotated = Plan("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"annotations\":[\"deadlocks\"]}");
        foreach (var (name, compiled) in ComposeCompiler.CompileAnnotations(annotated, Context(TwoServers, RollupAvailability.None, unregistered), ComposeCompiler.NoServerClocks))
        {
            yield return ($"annotation {name}", compiled.Sql);
        }

        yield return ("server clock read", ComposeCompiler.CompileServerClockRead(Context(TwoServers, RollupAvailability.None, unregistered)).Sql);
    }

    /// <summary>The statement scope names the registry and filters the fact on its id. A bare <c>server_name = ANY</c> on a fact is the
    /// shape that read every compressed chunk in full; the only name filters left are the registry's own (<c>reg.</c>) and the module
    /// map's (<c>mm.</c>, a small table keyed by name with no server_id).</summary>
    [Fact]
    public void NoCompiledStatement_FiltersAFactTable_OnServerNameAnyAlone()
    {
        var statements = ScopedStatements(null).ToList();
        Assert.True(statements.Count >= 6, "the sweep must reach every emitter: " + string.Join(", ", statements.Select(s => s.Label)));

        foreach (var (label, sql) in statements)
        {
            Assert.Contains("server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY($", sql, StringComparison.Ordinal);
            foreach (Match m in Regex.Matches(sql, @"[\w.]*server_name = ANY\("))
            {
                Assert.True(
                    m.Value.StartsWith("reg.", StringComparison.Ordinal) || m.Value.StartsWith("mm.", StringComparison.Ordinal),
                    $"{label}: filters on '{m.Value}' - a fact scoped by name reads every compressed chunk in full (#5525).");
            }
        }
    }

    /// <summary>The hourly-raw-edges statement scopes its fact alias once, outside the union, and the planner pushes the id list into
    /// each arm; the procedure_stats overlay CTE carries its own scope.</summary>
    [Fact]
    public void HourlyRawEdges_ScopeTheFactAndTheModuleOverlay_ByIdAndTheMapByName()
    {
        var sql = ScopedStatements(null).Single(s => s.Label.StartsWith("hourly raw edges", StringComparison.Ordinal)).Sql;
        Assert.Contains("          AND " + ByIdScope(3) + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("  AND f." + ByIdScope(3) + "\n", sql, StringComparison.Ordinal);
        Assert.Contains("      AND mm.server_name = ANY($3)\n", sql, StringComparison.Ordinal);
    }

    /// <summary>A scoped name with no registry row can resolve to no id, so only those names also match the row's stored name. Every
    /// emitter adds the branch, bound once, and the parameter set stays fully cited.</summary>
    [Fact]
    public void UnregisteredNames_AreMatchedByStoredName_OnEveryEmitter_AndNothingElse()
    {
        string[] ghost = ["GHOST"];
        var statements = ScopedStatements(ghost).ToList();
        Assert.True(statements.Count >= 6, "the sweep must reach every emitter: " + string.Join(", ", statements.Select(s => s.Label)));
        foreach (var (label, sql) in statements)
        {
            /* EVERY by-id scope in the statement carries the arm: one match per statement would let a single CTE drop it. */
            var scopes = Regex.Matches(sql, @"server_id = ANY\(ARRAY\(SELECT reg\.server_id FROM collect\.servers AS reg WHERE reg\.server_name = ANY\(\$\d+\)\)\)").Count;
            var armed = Regex.Matches(sql, @"\(\w*\.?server_id = ANY\(ARRAY\(SELECT reg\.server_id FROM collect\.servers AS reg WHERE reg\.server_name = ANY\(\$\d+\)\)\) OR [\w.]*server_name = ANY\(\$\d+\)\)").Count;
            Assert.True(scopes > 0, $"{label}: no by-id scope");
            Assert.True(scopes == armed, $"{label}: {scopes} by-id scopes but {armed} carry the unregistered-name arm (#5525).");
            Assert.DoesNotContain("GHOST", sql, StringComparison.Ordinal);
        }

        /* The hourly-raw-edges statement scopes the fact alias and the procedure_stats overlay: two scopes, both armed. */
        var edges = statements.Single(x => x.Label.StartsWith("hourly raw edges", StringComparison.Ordinal)).Sql;
        Assert.True(Regex.Matches(edges, @"server_id = ANY\(ARRAY\(").Count >= 2);

        var plain = Compile(Plan(WaitPanel), Context(TwoServers, RollupAvailability.None, null));
        var withGhost = Compile(Plan(WaitPanel), Context(TwoServers, RollupAvailability.None, ghost));
        Assert.Equal(plain.Parameters.Count + 1, withGhost.Parameters.Count);
        Assert.Equal(ghost, (string[])withGhost.Parameters[3].Value!);
        Assert.Null(ComposeParameterCoverageTests.CoverageViolation("wait panel with an unregistered name", withGhost.Sql, withGhost.Parameters));
        Assert.Equal(
            ComposeParameterCoverageTests.PredictedParameterCount(Plan(WaitPanel), true, Context(TwoServers, RollupAvailability.None, ghost)),
            withGhost.Parameters.Count);

        var clock = ComposeCompiler.CompileServerClockRead(Context(TwoServers, RollupAvailability.None, ghost));
        Assert.Equal(ComposeParameterCoverageTests.PredictedServerClockReadParameterCount(true, unregistered: true), clock.Parameters.Count);
        Assert.Null(ComposeParameterCoverageTests.CoverageViolation("clock read with an unregistered name", clock.Sql, clock.Parameters));
    }

    /// <summary>The hourly-edges count guard proves the rollup over the scope the read takes: both of its sides carry the by-id scope
    /// (and, when the run has unregistered names, the same arm on both), where it once filtered both on <c>server_name = ANY($3)</c>
    /// and so never compared a renamed server's earlier-name hours.</summary>
    [Fact]
    public void TheCountGuard_ScopesTheLedgerAndTheRollup_ByTheSamePredicateTheReadUses()
    {
        var plain = IntervalRollupCountGuard.SqlFor(false);
        Assert.Equal(plain, IntervalRollupCountGuard.QueryStatsSql);
        Assert.Equal(2, Regex.Matches(plain, Regex.Escape(ByIdScope(3))).Count);
        Assert.DoesNotContain("server_name = ANY($3))", plain.Replace("reg.server_name = ANY($3)", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("$4", plain, StringComparison.Ordinal);

        var armed = IntervalRollupCountGuard.SqlFor(true);
        Assert.Equal(2, Regex.Matches(armed, Regex.Escape("(" + ByIdScope(3) + " OR server_name = ANY($4))")).Count);
        Assert.Equal(ComposeCompiler.ServerScope(string.Empty, "$3", "$4"), ServerScopeSql.Predicate(string.Empty, "$3", "$4"));
    }

    /// <summary>The runner seam. Deleting either lookup leaves the compiler correct and every compiler test green, but a scope over a
    /// name that has rows and no registry row would return nothing. The live tests below fail on a deleted call too; these pins read the
    /// order: the panel runner looks the names up BEFORE the hourly-edges snapshot takes its connection, hands the answer to the
    /// snapshot (for the count guard) and to the run context, and the alert sweep looks up once per server, outside the per-rule loop.</summary>
    [Fact]
    public void BothRunners_LookUpUnregisteredNames_BeforeTheyCompile_AndHandThemOn()
    {
        var web = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        var lookup = web.IndexOf("await ComposeServerScope.FindUnregisteredAsync(postgres, serverScope, cancellationToken", StringComparison.Ordinal);
        var snapshot = web.IndexOf("await BeginHourlyEdgesSnapshotAsync(postgres, hourlyEdgesCandidate", StringComparison.Ordinal);
        var context = web.IndexOf("new ComposeRunContext(serverScope, start, end", StringComparison.Ordinal);
        Assert.True(lookup > 0 && snapshot > lookup && context > snapshot, "lookup, then the snapshot, then the run context");
        Assert.Contains("cancellationToken, unregisteredServers);", web[snapshot..context], StringComparison.Ordinal);
        Assert.Contains("UnregisteredServers: unregisteredServers", web[context..(context + 700)], StringComparison.Ordinal);
        Assert.Single(Regex.Matches(web, @"ComposeServerScope\.FindUnregisteredAsync\("));

        var alerts = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "CustomAlertEvaluator.cs");
        Assert.Equal(2, Regex.Matches(alerts, @"ComposeServerScope\.FindUnregisteredAsync\(").Count);
        var sweep = alerts.IndexOf("ComposeServerScope.FindUnregisteredAsync(_viewer", StringComparison.Ordinal);
        var loop = alerts.IndexOf("foreach (var (row, def, _) in applicable)", StringComparison.Ordinal);
        Assert.True(sweep > 0 && loop > sweep, "the sweep looks the names up once per server, before the per-rule loop");
        Assert.Contains("UnregisteredServers: unregistered", alerts, StringComparison.Ordinal);
        Assert.Contains("resolvedUnregistered ?? await ComposeServerScope.FindUnregisteredAsync(pool", alerts, StringComparison.Ordinal);
    }

    /// <summary>A fleet-wide run names no servers: no scope, no parameter, no registry read.</summary>
    [Fact]
    public void AFleetWideRun_CarriesNoServerScope_EvenWithUnregisteredNamesSupplied()
    {
        var compiled = Compile(Plan(WaitPanel), Context(null, RollupAvailability.None, ["GHOST"]));
        Assert.DoesNotContain("server_id = ANY", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collect.servers", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain(compiled.Parameters, p => p.Value is string[] names && names.Contains("GHOST"));
    }
}

/// <summary>
/// #5525, live: the by-id scope returns the rows the by-name scope returned, a renamed server's whole history follows its id, and a
/// scoped name that no registry row carries still returns what it returned before.
/// </summary>
[Collection("live-postgres")]
public sealed class ComposeServerScopeByIdLiveTests
{
    private const int IdA = -5525001;
    private const int IdB = -5525002;
    private const int IdC = -5525003;
    private const int IdGhost = -5525009;
    private const string NameA = "s5525-a";
    private const string NameB = "s5525-b";
    private const string NameC = "s5525-c";
    private const string OldNameA = "s5525-a-before-rename";
    private const string GhostName = "s5525-ghost";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Panel =
        "{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"topN\":20,\"groupBy\":[\"server\",\"wait_type\"],\"viz\":\"bar\"}";

    private static ComposeCompiled CompileFor(string[] servers, DateTime end, IReadOnlyList<string>? unregistered = null)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(Panel)!, []);
        Assert.True(parseError is null, parseError);
        var (compiled, error) = ComposeCompiler.Compile(
            plan!,
            new ComposeRunContext(servers, end.AddHours(-3), end, ComposeRunContext.NoVariables, RollupAvailability.None, end, RollupCoverage.Unknown,
                UnregisteredServers: unregistered));
        Assert.True(error is null, error);
        return compiled!;
    }

    /// <summary>The same statement with the scope the compiler used before #5525, to compare rows against.</summary>
    private static string ByNameSql(string sql) =>
        sql.Replace("f.server_id = ANY(ARRAY(SELECT reg.server_id FROM collect.servers AS reg WHERE reg.server_name = ANY($3)))", "f.server_name = ANY($3)", StringComparison.Ordinal);

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, ComposeCompiled compiled, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = parameter.NpgsqlDbType, Value = parameter.Value });
        }

        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
            }

            rows.Add(string.Join("|", cells));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        await CleanAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, IdA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, IdB, NameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, IdC, NameC, ct);
        var t = end.AddHours(-1);
        await InsertAsync(connection, t, IdA, NameA, "CXPACKET", 100, ct);
        await InsertAsync(connection, t, IdA, NameA, "WRITELOG", 7, ct);
        await InsertAsync(connection, t, IdB, NameB, "CXPACKET", 200, ct);
        await InsertAsync(connection, t, IdB, NameB, "LCK_M_X", 11, ct);
        await InsertAsync(connection, t, IdC, NameC, "CXPACKET", 400, ct);
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, DateTime time, int serverId, string serverName, string waitType, long ms, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES (1, $1, $2, $3, $4, 1, $5, 0, 1, $5, 0)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(ms);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "wait_stats", "servers" })
        {
            await using var command = new NpgsqlCommand($"DELETE FROM collect.{table} WHERE server_id = ANY($1)", connection);
            command.Parameters.AddWithValue(new[] { IdA, IdB, IdC, IdGhost });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static (NpgsqlConnection Connection, DateTime End) Skip(out string? cs)
    {
        cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live server-scope test.");
        return (new NpgsqlConnection(cs), new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc));
    }

    [Fact]
    public async Task AMultiServerScope_ReturnsTheSameRows_ById_AsByName()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, end) = Skip(out var cs);
        await using var _ = connection;
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, end, ct);
            var compiled = CompileFor([NameA, NameB], end);
            Assert.Contains("f.server_id = ANY(", compiled.Sql, StringComparison.Ordinal);

            var byId = await RowsAsync(connection, compiled.Sql, compiled, ct);
            var byName = await RowsAsync(connection, ByNameSql(compiled.Sql), compiled, ct);

            Assert.NotEmpty(byName);
            Assert.Equal(byName, byId);
            Assert.DoesNotContain(byId, row => row.Contains(NameC, StringComparison.Ordinal));
            Assert.Contains(byId, row => row.Contains(NameA, StringComparison.Ordinal));
            Assert.Contains(byId, row => row.Contains(NameB, StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The registry holds ONE current name per id and fact rows keep the name they were written under. A scope that names the
    /// current name now includes the server's rows written under its earlier name. The old filter returned only the current spelling.</summary>
    [Fact]
    public async Task ARenamedServer_KeepsItsWholeHistory_UnderItsCurrentRegistryName()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, end) = Skip(out var cs);
        await using var _ = connection;
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, end, ct);
            await InsertAsync(connection, end.AddHours(-2), IdA, OldNameA, "CXPACKET", 1000, ct);
            var compiled = CompileFor([NameA], end);

            var byId = await RowsAsync(connection, compiled.Sql, compiled, ct);
            var byName = await RowsAsync(connection, ByNameSql(compiled.Sql), compiled, ct);

            Assert.DoesNotContain(byName, row => row.Contains("1000", StringComparison.Ordinal));
            Assert.Contains(byId, row => row.Contains("1000", StringComparison.Ordinal) && row.Contains(OldNameA, StringComparison.Ordinal));
            Assert.Equal(byName.Count + 1, byId.Count);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanAsync(cleanup, cleanupCt));
        }
    }

    private const string GhostWait = "GHOSTWAIT";

    /// <summary>A panel run scoped to a name that has rows and no registry row, through the web runner itself. Deleting the runner's
    /// lookup makes this return no row.</summary>
    [Fact]
    public async Task ThePanelRunner_ReturnsRowsForAScopeNameTheRegistryDoesNotHold()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, end) = Skip(out var cs);
        await using var _ = connection;
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, end, ct);
            await InsertAsync(connection, end.AddHours(-1), IdGhost, GhostName, GhostWait, 33, ct);

            await using var source = NpgsqlDataSource.Create(cs!);
            var body = new JsonObject { ["panel"] = JsonNode.Parse(Panel), ["hours"] = 6, ["server"] = GhostName };
            var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(source, body, ct);
            Assert.True(outcome.Error is null, outcome.Error);
            Assert.Contains(GhostWait, outcome.Payload!.ToJsonString(), StringComparison.Ordinal);

            /* A registered name still reads by id, and does not pick up the ghost's rows. */
            body["server"] = NameA;
            var registered = await DarlingWebEndpoints.RunComposedPanelAsync(source, body, ct);
            Assert.True(registered.Error is null, registered.Error);
            var text = registered.Payload!.ToJsonString();
            Assert.Contains("WRITELOG", text, StringComparison.Ordinal);
            Assert.DoesNotContain(GhostWait, text, StringComparison.Ordinal);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The same through the custom-alert scalar seam: a storage name with rows and no registry row still reads its value, and
    /// a sweep that already resolved the lookup (an empty answer) passes it through without a second read.</summary>
    [Fact]
    public async Task TheAlertScalarSeam_ReadsAStorageNameTheRegistryDoesNotHold()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, end) = Skip(out var cs);
        await using var _ = connection;
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, end, ct);
            await InsertAsync(connection, end.AddHours(-1), IdGhost, GhostName, GhostWait, 33, ct);

            var (definition, defError) = CustomAlertRuleDefinition.TryParse(
                "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":3}," +
                "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1}}");
            Assert.True(defError is null, defError);

            await using var source = NpgsqlDataSource.Create(cs!);
            var value = await CustomAlertEvaluator.EvaluateScalarNowAsync(
                source, definition!, GhostName, end, RollupAvailability.None, RollupCoverage.Unknown, 30, logger: null, ct);
            Assert.Equal(33d, value);

            /* A sweep passes its once-per-server answer: an empty list means nothing is unregistered, so the ghost matches no id. */
            var passedThrough = await CustomAlertEvaluator.EvaluateScalarNowAsync(
                source, definition!, GhostName, end, RollupAvailability.None, RollupCoverage.Unknown, 30, logger: null, ct,
                resolvedUnregistered: Array.Empty<string>());
            Assert.Null(passedThrough);

            var registered = await CustomAlertEvaluator.EvaluateScalarNowAsync(
                source, definition!, NameA, end, RollupAvailability.None, RollupCoverage.Unknown, 30, logger: null, ct);
            Assert.Equal(107d, registered);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>A scoped name that no registry row carries (rows exist under it, as they do for a server renamed since): the runner finds
    /// it, the compiler keeps matching it by stored name, and the read returns what the old scope returned. Without that, the by-id
    /// scope alone returns nothing for it.</summary>
    [Fact]
    public async Task AnUnregisteredName_StillReturnsItsRows_WhenTheRunnerFindsItAndTheCompilerKeepsTheNameBranch()
    {
        var ct = TestContext.Current.CancellationToken;
        var (connection, end) = Skip(out var cs);
        await using var _ = connection;
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, end, ct);
            await InsertAsync(connection, end.AddHours(-1), IdGhost, GhostName, "CXPACKET", 33, ct);
            string[] scope = [NameA, GhostName];

            await using var source = NpgsqlDataSource.Create(cs!);
            var unregistered = await ComposeServerScope.FindUnregisteredAsync(source, scope, ct);
            Assert.Equal([GhostName], unregistered);
            Assert.Null(await ComposeServerScope.FindUnregisteredAsync(source, [NameA, NameB], ct));
            Assert.Null(await ComposeServerScope.FindUnregisteredAsync(source, null, ct));

            var withBranch = CompileFor(scope, end, unregistered);
            var oldSql = ByNameSql(CompileFor(scope, end).Sql);
            var expected = await RowsAsync(connection, oldSql, CompileFor(scope, end), ct);
            var actual = await RowsAsync(connection, withBranch.Sql, withBranch, ct);

            Assert.Contains(expected, row => row.Contains(GhostName, StringComparison.Ordinal));
            Assert.Equal(expected, actual);

            var withoutBranch = CompileFor(scope, end);
            var dropped = await RowsAsync(connection, withoutBranch.Sql, withoutBranch, ct);
            Assert.DoesNotContain(dropped, row => row.Contains(GhostName, StringComparison.Ordinal));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await CleanAsync(cleanup, cleanupCt));
        }
    }
}
