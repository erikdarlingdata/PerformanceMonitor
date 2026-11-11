/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8568: Lite's <c>get_alert_history</c> and <c>get_top_queries_by_cpu</c> answer the same shape as the service's,
/// so a default call over long <c>detail_text</c> or long <c>query_text</c> was over the shared 32,768-byte response
/// budget here too. Each test seeds a store whose default answer is over the budget, calls the tool as a caller
/// would, and holds the answer to the budget, to whole rows, and to saying what it cut. <c>get_table_index_sizes</c>
/// (#8655, its 100-table page) is held to the same rule here.
/// </summary>
public sealed class LiteMcpResponseBudget8568Tests : IDisposable
{
    private const string ServerName = "BudgetToolServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly DateTime _now = new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

    public LiteMcpResponseBudget8568Tests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "BudgetMcpTool_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "tool.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_tempDir);
        var connection = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(connection);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(connection));
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static JsonElement AssertWithinBudget(string tool, string json)
    {
        Assert.False(McpHelpers.IsErrorEnvelope(json), $"{tool} answered an error: {json[..Math.Min(json.Length, 400)]}");
        var bytes = Encoding.UTF8.GetByteCount(json);
        Assert.True(bytes <= McpResponseBudget.DefaultBytes,
            $"default {tool} was {bytes:N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget.");
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Fact]
    public async Task AlertHistory_Default_FiftyAlertsWithLongDetail_FitsTheBudget_PreviewsTheText_AndSaysWhatItLeftOut()
    {
        const int detailLength = 3000;
        var helper = new TestAlertDataHelper(Path.Combine(_tempDir, "tool.duckdb"));
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            for (var i = 0; i < 60; i++)
            {
                await helper.InsertLiveAlertAsync(connection, _now.AddMinutes(-2 - i), _serverId, ServerName, $"Collector Cost Digest {i:D2}", detailText: DetailText(i, detailLength));
            }
        }

        var root = AssertWithinBudget("get_alert_history", await McpAlertTools.GetAlertHistory(_service));
        var alerts = root.GetProperty("alerts").EnumerateArray().ToList();

        /* The limit's page is 50 of the 60; fewer than 50 fit, newest first, each one whole. */
        Assert.InRange(alerts.Count, 1, 49);
        Assert.Equal(alerts.Count, root.GetProperty("alerts_returned").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        var note = root.GetProperty("budget_note").GetString();
        Assert.Contains("32,768-byte response budget", note, StringComparison.Ordinal);
        Assert.Contains($"50 alerts were read; only {alerts.Count} fit", note, StringComparison.Ordinal);
        Assert.Contains($"{50 - alerts.Count} were left out", note, StringComparison.Ordinal);
        for (var i = 0; i < alerts.Count; i++)
        {
            /* The fields that say which alert this is are whole; only the long text is a preview, and it says so. */
            Assert.Equal($"Collector Cost Digest {i:D2}", alerts[i].GetProperty("metric_name").GetString());
            Assert.True(alerts[i].GetProperty("detail_text_truncated").GetBoolean());
            Assert.Equal(detailLength, alerts[i].GetProperty("detail_text_length").GetInt32());
            var shown = alerts[i].GetProperty("detail_text").GetString()!;
            Assert.InRange(shown.Length, 1, 400);
            Assert.StartsWith(shown, DetailText(i, detailLength), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task TopQueriesByCpu_Default_TwentyLongStatements_FitsTheBudget_AndALongerPageSaysWhatItLeftOut()
    {
        const int queries = 120;
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            for (var i = 0; i < queries; i++)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, query_text) "
                    + "VALUES ($1, $2, $3, 'x', 'Sales', $4, $2, 1, $5, $5, 10, 1, $6)";
                foreach (var value in new object[] { (long)(i + 1), _now.AddMinutes(-60), _serverId, QueryHash(i), (queries - i) * 1_000_000L, QueryText(i) })
                {
                    cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = value });
                }

                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        /* The default page: 20 rows, all of them, because 400 characters of text each fit where 2,000 did not. */
        var root = AssertWithinBudget("get_top_queries_by_cpu", await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName));
        var rows = root.GetProperty("queries").EnumerateArray().ToList();
        Assert.Equal(20, rows.Count);
        Assert.False(root.GetProperty("truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
        for (var i = 0; i < rows.Count; i++)
        {
            Assert.Equal(QueryHash(i), rows[i].GetProperty("query_hash").GetString());
            Assert.True(rows[i].GetProperty("query_text_truncated").GetBoolean());
            var shown = rows[i].GetProperty("query_text").GetString()!;
            Assert.EndsWith("... (truncated)", shown, StringComparison.Ordinal);
            Assert.StartsWith(shown[..^"... (truncated)".Length], QueryText(i), StringComparison.Ordinal);
            Assert.InRange(shown.Length, 1, 400 + "... (truncated)".Length);
        }

        /* A caller who asks for more rows than the budget holds gets the heaviest that fit, and is told. */
        var wide = AssertWithinBudget("get_top_queries_by_cpu top=120", await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, top: queries));
        var kept = wide.GetProperty("queries").EnumerateArray().ToList();
        Assert.InRange(kept.Count, 21, queries - 1);
        Assert.True(wide.GetProperty("truncated").GetBoolean());
        Assert.Contains("32,768-byte response budget", wide.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Contains($"{queries} queries were read; only {kept.Count} fit", wide.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(Enumerable.Range(0, kept.Count).Select(QueryHash), kept.Select(r => r.GetProperty("query_hash").GetString()));
    }

    /// <summary>A digest's long text, distinct per alert from its first characters on.</summary>
    /// <summary>
    /// The alert page is newest first, so the alerts left out are older: <c>as_of</c> at the oldest one returned reads
    /// on to every alert, and fewer rows asked for is the same newest alerts again, which the note does not offer.
    /// </summary>
    [Fact]
    public async Task AlertHistory_CutForTheBudget_FollowingAsOfReadsEveryAlert_AndTheNoteDoesNotOfferFewerRows()
    {
        const int seeded = 60;
        var helper = new TestAlertDataHelper(Path.Combine(_tempDir, "tool.duckdb"));
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            for (var i = 0; i < seeded; i++)
            {
                await helper.InsertLiveAlertAsync(connection, _now.AddMinutes(-2 - i), _serverId, ServerName, $"Collector Cost Digest {i:D2}", detailText: DetailText(i, 3000));
            }
        }

        var first = AssertWithinBudget("get_alert_history", await McpAlertTools.GetAlertHistory(_service));
        var held = first.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).ToList();
        Assert.True(first.GetProperty("budget_truncated").GetBoolean());

        /* Fewer rows asked for returns alerts the cut page already held, and none it left out. */
        var fewer = AssertWithinBudget("get_alert_history limit", await McpAlertTools.GetAlertHistory(_service, limit: held.Count - 1));
        Assert.All(fewer.GetProperty("alerts").EnumerateArray(), a => Assert.Contains(a.GetProperty("metric_name").GetString()!, held));
        var note = first.GetProperty("budget_note").GetString();
        Assert.Contains("as_of at oldest_returned_alert_time", note, StringComparison.Ordinal);
        Assert.DoesNotContain("limit", note, StringComparison.Ordinal);

        var seen = new List<string>(held);
        var asOf = first.GetProperty("oldest_returned_alert_time").GetString();
        for (var calls = 0; calls < seeded; calls++)
        {
            var page = AssertWithinBudget("get_alert_history as_of", await McpAlertTools.GetAlertHistory(_service, as_of: asOf));
            var fresh = page.GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).Except(seen).ToList();
            Assert.True(fresh.Count > 0, $"the page read at as_of {asOf} held no alert the pages before it had not.");
            seen.AddRange(fresh);
            if (!page.GetProperty("truncated").GetBoolean())
            {
                break;
            }

            asOf = page.GetProperty("oldest_returned_alert_time").GetString();
        }

        Assert.Equal(Enumerable.Range(0, seeded).Select(i => $"Collector Cost Digest {i:D2}"), seen);
    }

    /// <summary>
    /// The query page is heaviest first, so the queries left out are lighter ones. Fewer rows asked for is the same
    /// heaviest queries again, which the note does not offer; one database's page reads queries the wide page left out.
    /// </summary>
    [Fact]
    public async Task TopQueriesByCpu_CutForTheBudget_TheNotesFilterReadsQueriesLeftOut_AndItDoesNotOfferFewerRows()
    {
        const int queries = 120;
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            for (var i = 0; i < queries; i++)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, query_text) "
                    + "VALUES ($1, $2, $3, 'x', $7, $4, $2, 1, $5, $5, 10, 1, $6)";
                foreach (var value in new object[] { (long)(i + 1), _now.AddMinutes(-60), _serverId, QueryHash(i), (queries - i) * 1_000_000L, QueryText(i), i % 2 == 0 ? "Sales" : "Billing" })
                {
                    cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = value });
                }

                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var wide = AssertWithinBudget("get_top_queries_by_cpu top=120", await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, top: queries));
        var held = wide.GetProperty("queries").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!).ToList();
        Assert.True(wide.GetProperty("truncated").GetBoolean());
        Assert.InRange(held.Count, 2, queries - 1);

        /* Fewer rows asked for returns queries the cut page already held, and none it left out. */
        var fewer = AssertWithinBudget("get_top_queries_by_cpu fewer", await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, top: held.Count - 1));
        Assert.All(fewer.GetProperty("queries").EnumerateArray(), r => Assert.Contains(r.GetProperty("query_hash").GetString()!, held));
        var note = wide.GetProperty("budget_note").GetString();
        Assert.DoesNotContain("row count", note, StringComparison.Ordinal);
        Assert.Contains("database_name", note, StringComparison.Ordinal);

        /* One database's page holds queries the wide page left out. */
        var billing = AssertWithinBudget("get_top_queries_by_cpu Billing", await McpQueryTools.GetTopQueriesByCpu(_service, _serverManager, ServerName, top: queries, database_name: "Billing"));
        Assert.Contains(billing.GetProperty("queries").EnumerateArray().Select(r => r.GetProperty("query_hash").GetString()!), hash => !held.Contains(hash));
    }

    /// <summary>
    /// #8655: Lite's <c>get_table_index_sizes</c> answers the same 100-table page as the service's, which is over the
    /// budget on a server whose rows carry a growth note. A default call over 130 tables fits the budget and says what
    /// it left out, and following <c>next_offset</c> reads every table exactly once, largest first.
    /// </summary>
    [Fact]
    public async Task TableIndexSizes_Default_OverOneHundredTables_FitsTheBudget_AndPagesToEveryTable()
    {
        /* One daily snapshot, so every row carries its growth note. Table_001 is the largest. */
        const int tables = 130;
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            for (var i = 1; i <= tables; i++)
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText =
                    "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count) "
                    + "VALUES ($1, $2, $3, $4, $5, 'BigDb', 7, 'dbo', $6, $7, 1, $8, 'CLUSTERED', $9, $9, 1000, 1, 1, 1, 1, 0, 0)";
                foreach (var value in new object[] { (long)i, _now.AddHours(-2), _serverId, ServerName, _now.AddDays(-12), i, $"Table_{i:D3}", $"PK_Table_{i:D3}", 5000m - i })
                {
                    cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = value });
                }

                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        var root = AssertWithinBudget("get_table_index_sizes", await ReadTableIndexSizesAsync(offset: 0));
        var listed = root.GetProperty("tables").GetArrayLength();
        Assert.InRange(listed, 1, 99);
        Assert.Equal(listed, root.GetProperty("tables_returned").GetInt32());
        Assert.True(root.GetProperty("truncated").GetBoolean());
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(listed, root.GetProperty("next_offset").GetInt32());
        var note = root.GetProperty("budget_note").GetString();
        Assert.Contains("32,768-byte response budget", note, StringComparison.Ordinal);
        Assert.Contains($"100 tables were read; only {listed} fit", note, StringComparison.Ordinal);
        Assert.Contains($"{100 - listed} were left out", note, StringComparison.Ordinal);
        Assert.Contains($"offset {listed}", note, StringComparison.Ordinal);

        /* Nothing is dropped: following next_offset reads every table exactly once, largest first, each page within the budget. */
        var seen = new List<string>();
        var page = root;
        for (var calls = 0; calls < 50; calls++)
        {
            seen.AddRange(page.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()!));
            if (!page.GetProperty("truncated").GetBoolean())
            {
                Assert.False(page.GetProperty("budget_truncated").GetBoolean());
                Assert.Equal(JsonValueKind.Null, page.GetProperty("next_offset").ValueKind);
                break;
            }

            page = AssertWithinBudget("get_table_index_sizes", await ReadTableIndexSizesAsync(page.GetProperty("next_offset").GetInt32()));
        }

        Assert.Equal(Enumerable.Range(1, tables).Select(i => $"Table_{i:D3}"), seen);

        /* An offset past the last table is an empty answer, and a negative one is refused. */
        using (var past = JsonDocument.Parse(await ReadTableIndexSizesAsync(tables)))
        {
            Assert.Equal("empty", past.RootElement.GetProperty("status").GetString());
        }

        using (var negative = JsonDocument.Parse(await ReadTableIndexSizesAsync(-1)))
        {
            Assert.Equal("invalid", negative.RootElement.GetProperty("status").GetString());
        }
    }

    /// <summary>Calls get_table_index_sizes by its parameter names, so the call reads the same whether or not the tool
    /// takes <c>offset</c>: a tool without the parameter answers from its first table.</summary>
    private Task<string> ReadTableIndexSizesAsync(int offset)
    {
        var method = typeof(McpObjectStatsTools).GetMethod(nameof(McpObjectStatsTools.GetTableIndexSizes))!;
        var supplied = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["dataService"] = _service,
            ["serverManager"] = _serverManager,
            ["server_name"] = ServerName,
            ["offset"] = offset,
        };
        var args = method.GetParameters()
            .Select(p => supplied.TryGetValue(p.Name!, out var value) ? value : p.HasDefaultValue ? p.DefaultValue : null)
            .ToArray();
        return (Task<string>)method.Invoke(null, args)!;
    }

    private static string DetailText(int i, int length)
    {
        var text = new StringBuilder($"Digest {i:D2}: ");
        for (var line = 0; text.Length < length; line++)
        {
            text.Append(System.Globalization.CultureInfo.InvariantCulture, $"collector_{line:D3} ran 288 times, p95 {1200 + line} ms, peak {4800 + line} ms, 0 failures; ");
        }

        return text.ToString(0, length);
    }

    private static string QueryHash(int i) => $"0x{0x8568_0000_0000L + i:X16}";

    /// <summary>About 2,500 characters of statement, distinct per query from its first characters on.</summary>
    private static string QueryText(int i)
    {
        var text = new StringBuilder($"SELECT /* report {i:D3} */ o.order_id");
        for (var column = 0; text.Length < 2400; column++)
        {
            text.Append(System.Globalization.CultureInfo.InvariantCulture, $", o.measure_{column:D3} + l.adjustment_{column:D3} AS adjusted_measure_{column:D3}");
        }

        return text.Append(" FROM dbo.orders AS o JOIN dbo.order_lines AS l ON l.order_id = o.order_id WHERE o.region_id = @region").ToString();
    }
}
