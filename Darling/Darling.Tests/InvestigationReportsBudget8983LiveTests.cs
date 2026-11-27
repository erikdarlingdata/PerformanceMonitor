/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #8983: <c>get_investigation_reports</c> bounded its window by rows (100 requests, 200 journal rows) and not by bytes, so a
/// week of busy investigations answered 157 to 324 KB, over the 32,768-byte response budget the other reads keep. Each fact
/// seeds a store of requests that carry reports, calls the tool as an agent would, and holds the reply to the budget, to whole
/// requests (a kept request carries all its journal rows and its report), and to saying what it left out.
/// </summary>
public sealed class InvestigationReportsBudget8983LiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static string EvidenceOf(int items, int chars) =>
        JsonSerializer.Serialize(Enumerable.Range(0, items).Select(i => new
        {
            tool = "get_blocking",
            arguments = new { },
            key_numbers = new string((char)('a' + (i % 26)), chars),
            basis = "verified",
        }));

    private static string Case(int i) => "case-" + i.ToString("D3", CultureInfo.InvariantCulture);

    /// <summary>The store and a clock the facts move: a minute per seeded request, so "newest" is never a tie.</summary>
    private sealed class Rig
    {
        public InvestigationRequestStore Store { get; set; } = null!;
        public required NpgsqlDataSource DataSource { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public DateTime Now { get; set; }
        public int Seeded { get; set; }

        private InvestigationBriefComposer Composer() =>
            new(new PgInvestigationBriefSourceReader(DataSource), new McpDataDetailSetting(McpDataDetail.Full), DarlingInvestigationBriefTools.Tools);

        /// <summary>Opens, claims and reports <paramref name="count"/> requests, one per server. Returns the request ids, oldest first.</summary>
        public async Task<List<Guid>> SeedAsync(int count, int rootCauseChars, string evidence, CancellationToken ct)
        {
            var composer = Composer();
            var ids = new List<Guid>();
            for (var i = 0; i < count; i++)
            {
                var n = Seeded++;
                Now = Now.AddMinutes(1);
                var alert = new InvestigationAlert(1, 100 + n, "srv-" + n.ToString(CultureInfo.InvariantCulture), "blocking", "Blocking", Now,
                    new[] { "key-" + n.ToString(CultureInfo.InvariantCulture) }, null, 1000, 10000, LeaseMinutes: 15, ExpiryMinutes: 240);
                var id = (await Store.OpenOrJoinAsync(alert, ct)).RequestId!.Value;
                var claim = Root(await DarlingMcpInvestigationTools.ClaimAsync(Store, composer, id.ToString(), "agent-a", null, null, ct));
                var root = Case(n) + " " + new string('x', rootCauseChars);
                var answer = Root(await DarlingMcpInvestigationTools.SubmitAsync(Store, id.ToString(), claim.GetProperty("claim_id").GetString()!, 1,
                    "real", root, evidence, "summary of " + Case(n), null, null, ct));
                Assert.Equal("reported", answer.GetProperty("status").GetString());
                ids.Add(id);
            }

            return ids;
        }

        /// <summary>The MCP tool, as an agent calls it.</summary>
        public Task<string> ToolAsync(CancellationToken ct, string? requestId = null) =>
            DarlingMcpInvestigationTools.GetInvestigationReports(DataSource, Composer(), new InvestigationReadPolicy(false), null, requestId, 24, ct);

        public async Task<int> JournalRowsAsync(Guid id, CancellationToken ct)
        {
            await using var command = new NpgsqlCommand($"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{id}'", Connection);
            return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }
    }

    private static async Task RunAsync(Func<Rig, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live #8983 response-budget tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rig = new Rig { DataSource = dataSource, Connection = connection, Now = DateTime.UtcNow.AddHours(-2) };
        rig.Store = new InvestigationRequestStore(dataSource, 30, () => rig.Now);
        await body(rig, ct);
    }

    [Fact]
    public async Task AWindowOverTheBudget_KeepsTheNewestRequestsWhole_AndSaysWhatItLeftOut()
    {
        await RunAsync(async (rig, ct) =>
        {
            const int total = 45;
            var ids = await rig.SeedAsync(total, rootCauseChars: 1500, EvidenceOf(3, 200), ct);

            var json = await rig.ToolAsync(ct);
            var bytes = Encoding.UTF8.GetByteCount(json);
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"the reply was {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
            var root = Root(json);
            Assert.Equal("ok", root.GetProperty("status").GetString());

            /* The window is more than the budget holds: some, not all, of the newest requests are kept, newest first. */
            var kept = root.GetProperty("requests_returned").GetInt32();
            Assert.InRange(kept, 1, total - 1);
            var requests = root.GetProperty("requests").EnumerateArray().ToList();
            Assert.Equal(kept, requests.Count);
            var newestFirst = ids.AsEnumerable().Reverse().Take(kept).ToList();
            Assert.Equal(newestFirst, requests.Select(r => r.GetProperty("request_id").GetGuid()).ToList());

            /* A kept request is whole: its report is there, and every journal row the store holds for it. */
            var reports = root.GetProperty("reports").EnumerateArray().ToList();
            Assert.Equal(newestFirst.OrderBy(g => g).ToList(), reports.Select(r => r.GetProperty("request_id").GetGuid()).OrderBy(g => g).ToList());
            var journal = root.GetProperty("journal").EnumerateArray().ToList();
            foreach (var id in newestFirst)
            {
                var stored = await rig.JournalRowsAsync(id, ct);
                Assert.True(stored >= 3);
                Assert.Equal(stored, journal.Count(j => j.GetProperty("request_id").GetString() == id.ToString()));
            }

            /* Nothing of a request left out is in the reply: no report and no journal row. */
            var leftOut = ids.Except(newestFirst).ToHashSet();
            Assert.DoesNotContain(reports, r => leftOut.Contains(r.GetProperty("request_id").GetGuid()));
            Assert.DoesNotContain(journal, j => j.GetProperty("request_id").GetString() is { } s && leftOut.Contains(Guid.Parse(s)));
            Assert.Contains(Case(total - 1), reports.Single(r => r.GetProperty("request_id").GetGuid() == ids[total - 1]).GetProperty("root_cause").GetString(), StringComparison.Ordinal);

            /* It says it was cut, how many requests it left out, and the narrower call that gets them. */
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            var note = root.GetProperty("budget_note").GetString()!;
            Assert.Contains($"{total - kept:#,0} were left out", note, StringComparison.Ordinal);
            Assert.Contains($"only {kept:#,0} fit", note, StringComparison.Ordinal);
            Assert.Contains("server_name or request_id, or a smaller hours_back", note, StringComparison.Ordinal);

            /* The narrower call gets a request the first reply left out, whole. */
            var oldest = Root(await rig.ToolAsync(ct, ids[0].ToString()));
            Assert.Equal("ok", oldest.GetProperty("status").GetString());
            Assert.Contains(Case(0), Assert.Single(oldest.GetProperty("reports").EnumerateArray().ToList()).GetProperty("root_cause").GetString(), StringComparison.Ordinal);
            Assert.False(oldest.TryGetProperty("budget_note", out _));

            /* The web panel's read passes no budget and still gets every request in the window. */
            var web = Root(await DarlingMcpInvestigationTools.ReportsAsync(rig.DataSource, McpDataDetail.Full, null, null, 24, ct));
            Assert.Equal(total, web.GetProperty("requests_returned").GetInt32());
            Assert.False(web.TryGetProperty("budget_note", out _));
        });
    }

    [Fact]
    public async Task AWindowThatFits_IsTheSameReplyAsTheUnbudgetedRead_WithNoBudgetFields()
    {
        await RunAsync(async (rig, ct) =>
        {
            await rig.SeedAsync(4, rootCauseChars: 200, EvidenceOf(1, 50), ct);

            var json = await rig.ToolAsync(ct);
            Assert.True(Encoding.UTF8.GetByteCount(json) < McpResponseBudget.DefaultBytes);
            var unbudgeted = await DarlingMcpInvestigationTools.ReportsAsync(rig.DataSource, McpDataDetail.Full, null, null, 24, ct);
            Assert.Equal(unbudgeted, json);
            var root = Root(json);
            Assert.Equal(4, root.GetProperty("requests_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
            Assert.False(root.TryGetProperty("budget_truncated", out _));
            Assert.False(root.TryGetProperty("budget_note", out _));
        });
    }

    [Fact]
    public async Task OneRequestOverTheBudgetAlone_IsKept_WithItsReport_AndTheNoteSaysSo()
    {
        await RunAsync(async (rig, ct) =>
        {
            /* 50 evidence items of 500 characters and a 7,900-character root cause pass 32 KB in one report. */
            var id = (await rig.SeedAsync(1, rootCauseChars: 7900, EvidenceOf(50, 500), ct)).Single();

            var json = await rig.ToolAsync(ct);
            Assert.True(Encoding.UTF8.GetByteCount(json) > McpResponseBudget.DefaultBytes, "the one request is not over the budget.");
            var root = Root(json);
            Assert.Equal(1, root.GetProperty("requests_returned").GetInt32());
            var report = Assert.Single(root.GetProperty("reports").EnumerateArray().ToList());
            Assert.Equal(id, report.GetProperty("request_id").GetGuid());
            Assert.Equal(50, report.GetProperty("evidence_verified").GetArrayLength());
            Assert.Equal(await rig.JournalRowsAsync(id, ct), root.GetProperty("journal").GetArrayLength());
            Assert.False(root.GetProperty("budget_truncated").GetBoolean());
            Assert.Contains("kept whole", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task TheNewestRequestOverTheBudgetAlone_StaysWhenAnOlderOneIsLeftOut()
    {
        await RunAsync(async (rig, ct) =>
        {
            await rig.SeedAsync(1, rootCauseChars: 100, EvidenceOf(1, 50), ct);
            var big = (await rig.SeedAsync(1, rootCauseChars: 7900, EvidenceOf(50, 500), ct)).Single();

            var root = Root(await rig.ToolAsync(ct));
            Assert.Equal(1, root.GetProperty("requests_returned").GetInt32());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            var note = root.GetProperty("budget_note").GetString()!;
            Assert.Contains("1 were left out", note, StringComparison.Ordinal);
            Assert.Contains("over the budget on its own", note, StringComparison.Ordinal);
            var report = Assert.Single(root.GetProperty("reports").EnumerateArray().ToList());
            Assert.Equal(big, report.GetProperty("request_id").GetGuid());
            Assert.Equal(big, root.GetProperty("requests")[0].GetProperty("request_id").GetGuid());
        });
    }
}
