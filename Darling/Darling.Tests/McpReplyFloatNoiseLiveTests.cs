/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. */

/// <summary>
/// #8572 row 1: no default MCP reply carries float noise. The reads come from the web read catalog
/// (<c>DarlingWebEndpoints.BuildReadDispatch</c>), the same dispatch table every read is registered in, so a read added tomorrow is scanned
/// without this test being edited. The store is the one the web UI smoke check seeds (<c>tools/ui-smoke/seed.sql</c>: SQL Server and
/// PostgreSQL targets with six hours of waits, queries, files, sizes and statements). Each read is called with its defaults and only a
/// server name, once for the SQL Server target and once for the PostgreSQL one, and every JSON number in the reply is scanned for more than
/// six decimals (48.06999999999999, 1185.150596030991). A read that needs an identity parameter (a query hash, a plan id) answers with a
/// refusal here, which carries no figure, so it is not driven by this test: <see cref="TheScan_IsDrivenThroughAtLeastTheReadsTheSeedFeeds"/>
/// holds a floor under how many reads answered with figures, so the scan cannot go quiet by every read refusing.
/// </summary>
public sealed class McpReplyFloatNoiseLiveTests
{
    /// <summary>The most decimals a figure in a reply may carry. Rates and averages are written to two to four places, and the rate rule
    /// keeps two significant digits for a tiny rate, which is six places for one count in a day-wide bucket.</summary>
    internal const int MaxDecimals = 6;

    /// <summary>The JSON numbers in <paramref name="reply"/> that carry more than <see cref="MaxDecimals"/> decimals, as the path and the raw
    /// text. A reply that is not JSON has no numbers to scan and answers empty.</summary>
    internal static List<string> NoisyNumbers(string reply)
    {
        var found = new List<string>();
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(reply);
        }
        catch (JsonException)
        {
            return found;
        }

        using (document)
        {
            Walk(document.RootElement, "$", found);
        }

        return found;
    }

    private static void Walk(JsonElement element, string path, List<string> found)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Number:
            {
                var raw = element.GetRawText();
                var dot = raw.IndexOf('.');
                if (dot >= 0)
                {
                    var end = raw.IndexOfAny(['e', 'E'], dot);
                    var decimals = (end < 0 ? raw.Length : end) - dot - 1;
                    if (decimals > MaxDecimals)
                    {
                        found.Add(path + " = " + raw);
                    }
                }

                break;
            }
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    Walk(property.Value, path + "." + property.Name, found);
                }

                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, path + "[" + index.ToString(CultureInfo.InvariantCulture) + "]", found);
                    index++;
                }

                break;
        }
    }

    [Fact]
    public void TheScan_FlagsNoise_AndPassesRoundedFiguresAndTimestamps()
    {
        Assert.Equal(["$.avg_cpu_ms = 1185.150596030991", "$.rows[0].growth_30d_mb = 48.06999999999999"],
            NoisyNumbers("{\"avg_cpu_ms\":1185.150596030991,\"rows\":[{\"growth_30d_mb\":48.06999999999999}]}"));
        /* Six places is the most a figure may carry; a timestamp is a string, whatever its precision; a reply that is not JSON has no figures. */
        Assert.Empty(NoisyNumbers("{\"a\":0.123456,\"b\":257.704,\"t\":\"2026-11-11T06:03:49.1234567Z\"}"));
        Assert.Empty(NoisyNumbers("Plan XML: 0.123456789"));
        Assert.Single(NoisyNumbers("[0.1234567]"));
    }

    [Fact]
    public async Task EveryReadsDefaultReply_CarriesNoNumberWithMoreThanSixDecimals()
    {
        var replies = await DriveEveryReadAsync();
        var noisy = replies
            .SelectMany(r => NoisyNumbers(r.Reply).Select(n => $"{r.Read} ({r.Server}): {n}"))
            .ToList();
        Assert.True(noisy.Count == 0, "Replies carry a number with more than " + MaxDecimals + " decimals. Round it where the reply is built:\n" + string.Join("\n", noisy.Take(40)));
    }

    [Fact]
    public async Task TheScan_IsDrivenThroughAtLeastTheReadsTheSeedFeeds()
    {
        var replies = await DriveEveryReadAsync();
        var driven = replies
            .Where(r => r.Reply.StartsWith('{') && !r.Reply.Contains("\"status\":\"error\"", StringComparison.Ordinal)
                && !r.Reply.Contains("\"status\":\"refused\"", StringComparison.Ordinal))
            .Select(r => r.Read)
            .Distinct(StringComparer.Ordinal)
            .Count();
        var catalog = replies.Select(r => r.Read).Distinct(StringComparer.Ordinal).Count();
        Assert.True(driven >= 40, $"Only {driven} of the {catalog} reads answered a default call with a reply: the scan is no longer reaching the catalog.");
    }

    /// <summary>#8572 rows 2, 3 and 11: no default reply says an em dash, a tracker reference or a count of four or more digits without its
    /// thousands separators in its own prose. The reads and the store are the ones the float scan drives (one shared run).</summary>
    [Fact]
    public async Task EveryReadsDefaultReply_CarriesNoEmDashTrackerNumberOrUngroupedCount()
    {
        var replies = await DriveEveryReadAsync();
        var hits = replies
            .SelectMany(r => ReplyTextScan.Hits(r.Reply).Select(h => $"{r.Read} ({r.Server}): {h}"))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        Assert.True(hits.Count == 0, "Replies break the reply-text rules (no U+2014, no tracker number, grouped counts):\n" + string.Join("\n", hits.Take(200)));
    }

    [Fact]
    public void TheTextScan_FlagsEachRule_AndPassesDataThatIsNotACount()
    {
        Assert.Equal(3, ReplyTextScan.Hits("{\"a\":\"one \u2014 two\",\"b\":\"retired by #3593\",\"c\":\"saw 1438 captures\"}").Count);
        /* Grouped counts, ids in a token, dates, times, decimals, hashes and the text of a captured statement are not defects. */
        Assert.Empty(ReplyTextScan.Hits("{\"a\":\"saw 1,438 captures, 12.5 ms, 2026-11-11T06:03:49Z, 0x1234ABCD, port:1433, v3.12.0\",\"query_text\":\"SELECT 12345 -- #12 \u2014\"}"));
        Assert.Single(ReplyTextScan.Hits("plain text reply with 443230195 block accesses"));
    }

    private static List<(string Read, string Server, string Reply)>? _cache;

    private static async Task<List<(string Read, string Server, string Reply)>> DriveEveryReadAsync()
    {
        if (_cache is not null)
        {
            return _cache;
        }

        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the reply float-noise scan (it mints its own scratch database).");
        var runtime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        var psql = string.IsNullOrEmpty(runtime) ? null : Path.Combine(runtime, "pgsql", "bin", "psql.exe");
        Assert.SkipWhen(psql is null || !File.Exists(psql),
            "Set DARLING_TEST_PGRUNTIME to the extracted pg-runtime folder: the seed is applied with its psql.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using (var connection = new NpgsqlConnection(csb.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using (var command = source.CreateCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, created_at, engine)
VALUES (101, 'ui-sql', '127.0.0.1,1', 'integrated', TRUE, now() AT TIME ZONE 'UTC', 'sqlserver'),
       (102, 'ui-pg',  '127.0.0.1',   'sql',        TRUE, now() AT TIME ZONE 'UTC', 'postgres'),
       (103, 'ui-long-name-sql-server-production-east-region-cluster-demo-01-primary', '127.0.0.2,1', 'integrated', TRUE, now() AT TIME ZONE 'UTC', 'sqlserver'),
       (104, 'ui-down-sql', '127.0.0.3,1', 'integrated', TRUE, now() AT TIME ZONE 'UTC', 'sqlserver')"))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        var seed = Path.Combine(RepoRoot(), "tools", "ui-smoke", "seed.sql");
        Assert.True(File.Exists(seed), "tools/ui-smoke/seed.sql not found at " + seed);
        var info = new ProcessStartInfo(psql!) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var a in new[] { "-h", csb.Host!, "-p", csb.Port.ToString(CultureInfo.InvariantCulture), "-U", csb.Username!,
                     "-d", scratch.DatabaseName, "-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A", "-f", seed })
        {
            info.ArgumentList.Add(a);
        }

        if (!string.IsNullOrEmpty(csb.Password))
        {
            info.Environment["PGPASSWORD"] = csb.Password;
        }

        using (var process = Process.Start(info)!)
        {
            var stderrTask = process.StandardError.ReadToEndAsync(ct);
            var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
            await process.WaitForExitAsync(ct);
            var stderr = await stderrTask;
            _ = await stdoutTask;
            Assert.True(process.ExitCode == 0, "seed.sql failed against a freshly migrated store: " + stderr.Trim());
        }

        var replies = new List<(string, string, string)>();
        foreach (var (read, handler) in DarlingWebEndpoints.BuildReadDispatch().OrderBy(kv => kv.Key, StringComparer.Ordinal))
        {
            foreach (var server in new[] { "ui-sql", "ui-pg" })
            {
                var context = new DefaultHttpContext();
                context.Request.QueryString = QueryString.Create("server_name", server);
                string reply;
                try
                {
                    using var deadline = System.Threading.CancellationTokenSource.CreateLinkedTokenSource(ct);
                    deadline.CancelAfter(TimeSpan.FromSeconds(30));
                    reply = await handler(context, source, null!).WaitAsync(deadline.Token);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
                {
                    /* A read that needs the analysis service or a request service this bare call does not carry is not driven here. */
                    reply = string.Empty;
                }

                replies.Add((read, server, reply ?? string.Empty));
            }
        }

        _cache = replies;
        return replies;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "tools", "ui-smoke")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repo root not found from " + thisFile);
    }
}
