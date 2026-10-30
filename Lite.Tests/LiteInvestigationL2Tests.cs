/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6011, Lite's twin of three Darling slices on a throwaway DuckDB: the server names a request, an opened row, a brief and the
/// webhook token carry (#5999), how often each linked metric fired at the first alert's time of day (#6000), and the report fields
/// (#6004: family_of, commit and release links, basis on evidence, queries_run), plus the v74 schema step. The names come from
/// the server list (<see cref="ServerManager"/>): Lite's DuckDB servers table is never written.
/// </summary>
public sealed class LiteInvestigationL2Tests : IDisposable
{
    private const string Evidence = "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"head blocker 412 s\",\"basis\":\"verified\"}]";

    private static readonly DateTime Anchor = new(2026, 10, 14, 2, 10, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _db;
    private readonly ServerManager _servers;
    private readonly DuckDbInvestigationStore _store;
    private DateTime _now = Anchor;

    public LiteInvestigationL2Tests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteInvestigationL2Tests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _db = new DuckDbInitializer(Path.Combine(_tempDir, "lite.duckdb"));
        _db.InitializeAsync().GetAwaiter().GetResult();
        /* Windows auth, so AddServer never touches the credential store. */
        _servers = new ServerManager(_tempDir);
        _store = new DuckDbInvestigationStore(_db, () => _now, DuckDbInvestigationStoreTests.TestLockBudget,
            InvestigationServerNameLookup.FromServerManager(_servers));
    }

    public void Dispose()
    {
        _db.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best effort */ }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private ServerConnection AddServer(string serverName, string displayName)
    {
        var server = new ServerConnection { ServerName = serverName, DisplayName = displayName };
        _servers.AddServer(server);
        return server;
    }

    private static InvestigationAlert Alert(int serverId, string openedName, string metric = "Blocking", DateTime? at = null) =>
        new(1, serverId, openedName, "blocking", metric, at ?? Anchor, new[] { "key-" + serverId + "-" + metric }, null, 20, 100,
            LeaseMinutes: 15, ExpiryMinutes: 240);

    private void Exec(string sql)
    {
        using var connection = _db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private string? Scalar(string sql, DuckDbInitializer? db = null)
    {
        using var connection = (db ?? _db).CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private InvestigationBriefComposer Composer(DuckDbInvestigationBriefSourceReader reader) =>
        new(reader, new McpDataDetailSetting(McpDataDetail.Full), LiteInvestigationBriefTools.Tools);

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    /* ---- #5999: the names ---- */

    [Fact]
    public async Task ARenamedDisplayName_ShowsOnTheNextRead_OfTheList_TheOpenedRows_TheBriefAndTheWebhookToken()
    {
        var server = AddServer("LiteSrvA", "Prod OLTP");
        var serverId = RemoteCollectorService.GetServerId(server);
        var id = (await _store.OpenOrJoinAsync(Alert(serverId, "LiteSrvA"), Ct)).RequestId!.Value;
        var briefReader = new DuckDbInvestigationBriefSourceReader(_db, () => _now, InvestigationServerNameLookup.FromServerManager(_servers));

        var first = Assert.Single(await _store.ListAsync(null, null, null, 10, Ct));
        Assert.Equal(("Prod OLTP", "LiteSrvA", "LiteSrvA"), (first.DisplayName, first.InstanceName, first.ServerName));
        Assert.Equal("Prod OLTP", (await _store.ReadRequestAsync(id, Ct))!.Value.Row.DisplayName);

        /* The operator renames the server; nothing is rewritten, so the next read of each surface shows the new name. */
        server.DisplayName = "Prod OLTP renamed";
        _servers.UpdateServer(server);

        var renamed = Assert.Single(await _store.ListAsync(null, null, null, 10, Ct));
        Assert.Equal("Prod OLTP renamed", renamed.DisplayName);
        Assert.Equal("LiteSrvA", renamed.ServerName);
        var opened = Assert.Single(await _store.OpenedSinceAsync(Anchor.AddHours(-1), null, 10, Ct));
        Assert.Equal(("Prod OLTP renamed", "LiteSrvA"), (opened.DisplayName, opened.InstanceName));
        Assert.Equal("Prod OLTP renamed", Assert.Single((await _store.ReadReportsAsync(id, null, DateTime.MinValue, 10, 10, Ct)).Requests).DisplayName);

        var brief = Root((await Composer(briefReader).ComposeAsync(id, Ct))!).GetProperty("request");
        Assert.Equal("Prod OLTP renamed", brief.GetProperty("display_name").GetString());
        Assert.Equal("LiteSrvA", brief.GetProperty("instance_name").GetString());

        var token = Root(InvestigationWebhookToken.Render(id, true, (await _store.ReadRequestAsync(id, Ct))!.Value.Row, null, McpDataDetail.Full));
        Assert.Equal("Prod OLTP renamed", token.GetProperty("display_name").GetString());
        Assert.Equal("LiteSrvA", token.GetProperty("instance_name").GetString());
    }

    [Fact]
    public async Task AServerRemovedFromTheList_LeavesInstanceNameAbsent_AndTheDisplayNameFallsBackToTheOpenedName()
    {
        var server = AddServer("LiteSrvB", "");
        var serverId = RemoteCollectorService.GetServerId(server);
        var id = (await _store.OpenOrJoinAsync(Alert(serverId, "OpenedAsName"), Ct)).RequestId!.Value;
        var briefReader = new DuckDbInvestigationBriefSourceReader(_db, () => _now, InvestigationServerNameLookup.FromServerManager(_servers));

        /* An empty display name is absent: the display name falls back to the list's server name. */
        var listed = Assert.Single(await _store.ListAsync(null, null, null, 10, Ct));
        Assert.Equal(("LiteSrvB", "LiteSrvB"), (listed.DisplayName, listed.InstanceName));

        _servers.DeleteServer(server.Id);

        var gone = Assert.Single(await _store.ListAsync(null, null, null, 10, Ct));
        Assert.Equal(("OpenedAsName", null), (gone.DisplayName, gone.InstanceName));
        var brief = Root((await Composer(briefReader).ComposeAsync(id, Ct))!).GetProperty("request");
        Assert.Equal("OpenedAsName", brief.GetProperty("display_name").GetString());
        Assert.False(brief.TryGetProperty("instance_name", out _));
    }

    /* ---- #6000: recurrence ---- */

    private async Task<(Guid Id, DuckDbInvestigationBriefSourceReader Reader)> OpenAtAsync(DateTime at, params string[] metrics)
    {
        _now = at;
        Guid id = default;
        foreach (var metric in metrics)
            id = (await _store.OpenOrJoinAsync(Alert(5, "srv-5", metric, at), Ct)).RequestId!.Value;
        return (id, new DuckDbInvestigationBriefSourceReader(_db, () => at));
    }

    private void Fire(string metric, DateTime at, bool muted = false) =>
        Exec($"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, muted) " +
             $"VALUES (TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', 5, 'srv-5', '{metric}', 1, 1, {(muted ? "true" : "false")})");

    [Fact]
    public async Task Recurrence_CountsAnEarlierDaysFireWithinThirtyMinutes_NotOneOutsideIt_NorThisIncidentsOwnRise()
    {
        var (id, reader) = await OpenAtAsync(Anchor, "Blocking");
        Fire("Blocking", new DateTime(2026, 10, 13, 1, 45, 0));  /* 25 minutes before the clock time, an earlier day: counts. */
        Fire("Blocking", new DateTime(2026, 10, 12, 2, 41, 0));  /* 31 minutes after: does not. */
        Fire("Blocking", new DateTime(2026, 10, 14, 1, 45, 0));  /* the same day, 25 minutes before the first alert: this incident's own rise. */
        Fire("Blocking", Anchor);                                /* the first alert's own row. */
        Fire("Blocking", new DateTime(2026, 10, 6, 2, 10, 0));   /* more than 7 days before the first alert. */
        Fire("Blocking Resolved", new DateTime(2026, 10, 13, 2, 10, 0)); /* a #6057 resolution row: its metric is its title, never a firing metric's. */

        var recurrence = Assert.Single((await reader.ReadAsync(id, Ct))!.Recurrence!);

        Assert.Equal("Blocking", recurrence.MetricName);
        Assert.Equal((1, 0), (recurrence.Fired, recurrence.Muted));
        Assert.Equal(new[] { new DateOnly(2026, 10, 13) }, recurrence.FiredDates);
        Assert.Equal(Anchor, recurrence.AroundUtc);
    }

    [Fact]
    public async Task Recurrence_IsCircular_AMidnightAlertCountsAFireJustAfterMidnight_AndMutedFiresAreCountedApart()
    {
        var late = new DateTime(2026, 10, 14, 23, 50, 0, DateTimeKind.Utc);
        var (id, reader) = await OpenAtAsync(late, "Blocking", "Deadlocks Detected");
        Fire("Blocking", new DateTime(2026, 10, 14, 0, 10, 0));            /* 23h40 before: a 00:10 fire is 20 minutes after 23:50's clock time. */
        Fire("Blocking", new DateTime(2026, 10, 12, 23, 40, 0), muted: true);

        var recurrence = (await reader.ReadAsync(id, Ct))!.Recurrence!;

        var blocking = Assert.Single(recurrence, r => r.MetricName == "Blocking");
        Assert.Equal((1, 1), (blocking.Fired, blocking.Muted));
        Assert.Equal(new[] { new DateOnly(2026, 10, 14) }, blocking.FiredDates);
        Assert.Equal(new[] { new DateOnly(2026, 10, 12) }, blocking.MutedDates);
        /* A linked metric with no history still comes back, with zero. */
        var quiet = Assert.Single(recurrence, r => r.MetricName == "Deadlocks Detected");
        Assert.Equal((0, 0, 0, 0), (quiet.Fired, quiet.Muted, quiet.FiredDates.Count, quiet.MutedDates.Count));
    }

    [Fact]
    public async Task TheComposedBrief_CarriesTheRecurrenceSection()
    {
        var (id, reader) = await OpenAtAsync(Anchor, "Blocking");
        Fire("Blocking", new DateTime(2026, 10, 13, 2, 0, 0));

        var recurrence = Root((await Composer(reader).ComposeAsync(id, Ct))!).GetProperty("recurrence");

        var item = Assert.Single(recurrence.EnumerateArray());
        Assert.Equal(("Blocking", "02:10", 1, 0), (item.GetProperty("metric").GetString(), item.GetProperty("around_utc").GetString(),
            item.GetProperty("fired").GetInt32(), item.GetProperty("muted").GetInt32()));
    }

    /* ---- #6004: the report fields ---- */

    private async Task<(Guid Id, Guid Claim)> ClaimedAsync(int serverId, string name)
    {
        var id = (await _store.OpenOrJoinAsync(Alert(serverId, name), Ct)).RequestId!.Value;
        var claim = await _store.ClaimAsync(id, "agent-a", Ct);
        return (id, claim.ClaimId!.Value);
    }

    [Fact]
    public async Task FamilyOf_NamingNoRequestIsRefused_AndOneNamingARealRequestIsStoredAndReadBack()
    {
        var (first, firstClaim) = await ClaimedAsync(101, "srv-101");
        var (second, secondClaim) = await ClaimedAsync(102, "srv-102");

        /* The 8-character short form of a real request id is not an id: refused, and nothing is stored. */
        var shortForm = Root(await McpInvestigationTools.SubmitAsync(_store, second.ToString(), secondClaim.ToString(), 1, "real", "cause",
            Evidence, "s", null, null, Ct, family_of: first.ToString()[..8]));
        Assert.Equal("invalid", shortForm.GetProperty("status").GetString());
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_reports"));

        var refused = Root(await McpInvestigationTools.SubmitAsync(_store, second.ToString(), secondClaim.ToString(), 1, "real", "cause",
            Evidence, "s", null, null, Ct, family_of: Guid.NewGuid().ToString()));
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        Assert.Contains("family_of", refused.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("0", Scalar("SELECT count(*) FROM investigation_reports"));

        Assert.Equal("reported", Root(await McpInvestigationTools.SubmitAsync(_store, first.ToString(), firstClaim.ToString(), 1, "real", "cause",
            Evidence, "s", null, null, Ct)).GetProperty("status").GetString());
        Assert.Equal("reported", Root(await McpInvestigationTools.SubmitAsync(_store, second.ToString(), secondClaim.ToString(), 1, "real", "cause",
            Evidence, "s", null, null, Ct, family_of: first.ToString(), queries_run: "[\"SELECT 1\"]")).GetProperty("status").GetString());

        var read = await _store.ReadReportsAsync(second, null, DateTime.MinValue, 10, 10, Ct);
        var report = Assert.Single(read.Reports);
        Assert.Equal(first, report.FamilyOf);
        Assert.Equal("[\"SELECT 1\"]", report.QueriesRunJson);
        var answer = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, second.ToString(), 24, Ct));
        var served = answer.GetProperty("reports")[0];
        Assert.Equal(first.ToString(), served.GetProperty("family_of").GetString());
        Assert.Equal("SELECT 1", served.GetProperty("queries_run")[0].GetString());
    }

    [Fact]
    public async Task Evidence_WithoutABasisIsRefused_AndWithOneTheAnswerServesTwoLists()
    {
        var (id, claim) = await ClaimedAsync(103, "srv-103");

        var refused = Root(await McpInvestigationTools.SubmitAsync(_store, id.ToString(), claim.ToString(), 1, "real", "cause",
            "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"412 s\"}]", "s", null, null, Ct));
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        Assert.Equal("evidence", refused.GetProperty("hints").GetProperty("parameter").GetString());

        const string Both = "[{\"tool\":\"a\",\"arguments\":{},\"key_numbers\":\"1\",\"basis\":\"verified\"}," +
                            "{\"tool\":\"b\",\"arguments\":{},\"key_numbers\":\"2\",\"basis\":\"inferred\"}]";
        Assert.Equal("reported", Root(await McpInvestigationTools.SubmitAsync(_store, id.ToString(), claim.ToString(), 1, "real", "cause",
            Both, "s", null, null, Ct)).GetProperty("status").GetString());

        var report = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, id.ToString(), 24, Ct))
            .GetProperty("reports")[0];
        Assert.Equal("a", Assert.Single(report.GetProperty("evidence_verified").EnumerateArray()).GetProperty("tool").GetString());
        Assert.Equal("b", Assert.Single(report.GetProperty("evidence_inferred").EnumerateArray()).GetProperty("tool").GetString());
        Assert.False(report.TryGetProperty("evidence", out _));
        /* Lite keeps no feedback: serving the evidence by basis does not claim it does. */
        Assert.False(report.TryGetProperty("feedback", out _));
    }

    [Fact]
    public async Task ACommitLinkAndAReleaseLink_AreAccepted_AndJournaled()
    {
        var (id, claim) = await ClaimedAsync(104, "srv-104");

        var answer = Root(await McpInvestigationTools.SubmitAsync(_store, id.ToString(), claim.ToString(), 1, "real", "cause", Evidence, "s", null, null, Ct,
            links: "[{\"kind\":\"commit\",\"url\":\"https://example.test/repo/commit/abc123\",\"key\":\"abc123\"}," +
                   "{\"kind\":\"release\",\"url\":\"https://example.test/repo/releases/v1\",\"key\":\"v1\"}]"));

        Assert.Equal("reported", answer.GetProperty("status").GetString());
        Assert.Equal(2, answer.GetProperty("links_recorded").GetInt32());
        Assert.Equal("2", Scalar($"SELECT count(*) FROM investigation_journal WHERE request_id = '{id}' AND event = 'link_recorded'"));
        var links = Root(await McpInvestigationTools.ReportsAsync(_servers, _store, McpDataDetail.Full, null, id.ToString(), 24, Ct))
            .GetProperty("reports")[0].GetProperty("links");
        Assert.Equal(new[] { "commit", "release" }, links.EnumerateArray().Select(l => l.GetProperty("kind").GetString()!).OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task APurge_ClearsTheFamilyOf_OfAReportWhoseFamilyRequestItRemoves()
    {
        var (first, firstClaim) = await ClaimedAsync(105, "srv-105");
        await _store.SubmitAsync(new InvestigationReportSubmission(first, firstClaim, "agent-a", 1, "real", "c", true, "s", Evidence), Ct);
        _now = Anchor.AddDays(200);
        var later = (await _store.OpenOrJoinAsync(Alert(106, "srv-106", at: _now), Ct)).RequestId!.Value;
        var laterClaim = (await _store.ClaimAsync(later, "agent-a", Ct)).ClaimId!.Value;
        await _store.SubmitAsync(new InvestigationReportSubmission(later, laterClaim, "agent-a", 1, "real", "c", true, "s", Evidence, FamilyOf: first), Ct);

        _now = Anchor.AddDays(400);
        await _store.PurgeAsync(Ct);

        Assert.False(await _store.RequestExistsAsync(first, Ct));
        Assert.True(await _store.RequestExistsAsync(later, Ct));
        Assert.Null(Assert.Single((await _store.ReadReportsAsync(later, null, DateTime.MinValue, 10, 10, Ct)).Reports).FamilyOf);
    }

    /* ---- v74 ---- */

    [Fact]
    public async Task SchemaStepV74_AppliesToANewAndAnExistingDatabase_AndASecondRunChangesNothing()
    {
        const string ColumnsSql = "SELECT string_agg(column_name, ',' ORDER BY column_name) FROM information_schema.columns " +
                                  "WHERE table_name = 'investigation_reports' AND column_name IN ('family_of', 'queries_run')";
        Assert.Equal("family_of,queries_run", Scalar(ColumnsSql));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version"));

        var path = Path.Combine(_tempDir, "old.duckdb");
        using (var old = new DuckDbInitializer(path))
        {
            await old.InitializeAsync();
            Scalar("ALTER TABLE investigation_reports DROP COLUMN family_of", old);
            Scalar("ALTER TABLE investigation_reports DROP COLUMN queries_run", old);
            Scalar("UPDATE schema_version SET version = 73", old);
        }

        using var upgraded = new DuckDbInitializer(path);
        await upgraded.InitializeAsync();
        Assert.Equal("family_of,queries_run", Scalar(ColumnsSql, upgraded));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version", upgraded));
        const string ShapeSql = "SELECT string_agg(column_name || ':' || data_type, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'investigation_reports'";
        var shape = Scalar(ShapeSql, upgraded);
        Assert.Contains("family_of:UUID", shape, StringComparison.Ordinal);

        using var again = new DuckDbInitializer(path);
        await again.InitializeAsync();
        Assert.Equal(shape, Scalar(ShapeSql, again));
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion.ToString(CultureInfo.InvariantCulture), Scalar("SELECT max(version) FROM schema_version", again));
    }
}
