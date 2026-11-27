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
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres, because the label assertions
   read every server the fleet holds, and a shared store's other servers would be in the body. */

/// <summary>
/// #9013: the metrics port end to end, through the real host on a free port with a scratch store and a collector that is
/// collecting. The body parses line by line, its only label names are the catalog's, the fixed labels carry only their fixed
/// values, no text from a monitored server reaches it, shared display names get id suffixes, and a failing refresh serves the
/// last snapshot until it is five minutes old.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMetricsScrapeLiveTests
{
    private const string Token = DarlingMetricsHostTests.GoodToken;
    private const string SecretDb = "SECRET_DB_9013";
    private const string TopApp = "TOP_APP_9013";
    private const string TopHost = "TOP_HOST_9013";

    private static readonly HashSet<string> AllowedLabelNames =
        new(StringComparer.Ordinal) { DarlingMetricCatalog.ServerLabel, "category", "file_type", "scope", "kind", "state" };

    private static readonly Regex SampleLine = new(
        @"^(?<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?<labels>[^}]*)\})? (?<value>\S+)$", RegexOptions.CultureInvariant);

    private static readonly Regex LabelPair = new(
        @"\G(?<key>[a-zA-Z_][a-zA-Z0-9_]*)=""(?<value>(?:[^""\\]|\\.)*)""(?:,|$)", RegexOptions.CultureInvariant);

    private static readonly Regex CommentLine = new(
        @"^# (?<kind>HELP|TYPE) (?<name>[a-zA-Z_:][a-zA-Z0-9_:]*) (?<text>.+)$", RegexOptions.CultureInvariant);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>[Erik] The label set is server plus the five fixed labels, each fixed label carries only its fixed values, and no
    /// text from a monitored server (a database name, an application, a host) reaches the body.</summary>
    [Fact]
    public async Task LabelSet_IsServerAndTheFixedLabels_AndNoDataTextLeaks()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics scrape tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var names = new Dictionary<int, string> { [1] = "metrics-scrape-io", [2] = "metrics-scrape-cpu" };
        var at = LiveClock.Now().AddMinutes(-1);
        await using (var c = await postgres.OpenConnectionAsync(ct))
        {
            await InsertServerAsync(c, 1, names[1], names[1], ct);
            await InsertServerAsync(c, 2, names[2], names[2], ct);
            await InsertCollectionLogAsync(c, 1, names[1], at, ct);

            await InsertFileIoAsync(c, 1, names[1], at, SecretDb, "ROWS", ct);
            await InsertFileIoAsync(c, 1, names[1], at, SecretDb, "LOG", ct);
            await InsertFileIoAsync(c, 1, names[1], at, "tempdb", "ROWS", ct);
            await InsertWaitAsync(c, 1, names[1], at, "LCK_M_X", ct);
            await InsertWaitAsync(c, 1, names[1], at, "SOS_SCHEDULER_YIELD", ct);
            await InsertSessionsAsync(c, 1, names[1], at, ct);
            await InsertTempdbAsync(c, 1, names[1], at, ct);

            await InsertCpuAsync(c, 2, names[2], at, ct);
        }

        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var host = await StartHostAsync(_ => Task.FromResult<NpgsqlDataSource?>(postgres), logger, time: null, ct);
        using var client = new HttpClient();

        var body = await ScrapeUntilSnapshotAsync(client, host.Port, ct);

        var samples = ParseStrictly(body);
        var seenLabelNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (metric, labels, _) in samples)
        {
            foreach (var (key, value) in labels)
            {
                Assert.Contains(key, AllowedLabelNames);
                seenLabelNames.Add(key);
                if (key == DarlingMetricCatalog.ServerLabel)
                {
                    Assert.Contains(value, names.Values);
                }
                else
                {
                    Assert.True(DarlingMetricCatalog.FixedLabelValues[key].Contains(value), $"{metric}: {key}=\"{value}\" is not a fixed value");
                }
            }
        }

        /* The seed reaches every label, so the checks above ran on each one rather than on an empty set. */
        Assert.Equal(AllowedLabelNames.OrderBy(n => n, StringComparer.Ordinal), seenLabelNames.OrderBy(n => n, StringComparer.Ordinal));
        Assert.Contains(samples, s => s.Labels.Any(l => l.Key == DarlingMetricCatalog.ServerLabel && l.Value == names[2]));
        Assert.DoesNotContain(SecretDb, body, StringComparison.Ordinal);
        Assert.DoesNotContain(TopApp, body, StringComparison.Ordinal);
        Assert.DoesNotContain(TopHost, body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DuplicateDisplayNames_ScrapeParses_WithIdSuffixes()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics scrape tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string shared = "metrics-scrape-prod";
        await using (var c = await postgres.OpenConnectionAsync(ct))
        {
            await InsertServerAsync(c, 7, "sql-a.example", shared, ct);
            await InsertServerAsync(c, 8, "sql-b.example", shared, ct);
            await InsertServerAsync(c, 9, "sql-c.example", "metrics-scrape-other", ct);
        }

        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var host = await StartHostAsync(_ => Task.FromResult<NpgsqlDataSource?>(postgres), logger, time: null, ct);
        using var client = new HttpClient();

        var body = await ScrapeUntilSnapshotAsync(client, host.Port, ct);

        var bands = ParseStrictly(body)
            .Where(s => s.Metric == "darling_server_health_band")
            .Select(s => Assert.Single(s.Labels, l => l.Key == DarlingMetricCatalog.ServerLabel).Value)
            .OrderBy(v => v, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(new[] { "metrics-scrape-other", shared + " #7", shared + " #8" }, bands);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Information && l.Text.Contains("share a display name", StringComparison.Ordinal));
    }

    [Fact]
    public async Task FailedRefresh_ServesOldSnapshot_ThenDropsAfter5Min()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics scrape tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var c = await postgres.OpenConnectionAsync(ct))
        {
            await InsertServerAsync(c, 1, "metrics-scrape-failing", "metrics-scrape-failing", ct);
        }

        /* After the first refresh the data source is swapped for one that throws, as a store that went away would. */
        var failing = false;
        var time = new TimerTimeProvider();
        var logger = new DarlingMetricsHostTests.CapturingLogger();
        await using var host = await StartHostAsync(
            _ => Volatile.Read(ref failing)
                ? throw new InvalidOperationException("the store went away (#9013 test)")
                : Task.FromResult<NpgsqlDataSource?>(postgres),
            logger, time, ct);
        using var client = new HttpClient();

        var fresh = await ScrapeUntilSnapshotAsync(client, host.Port, ct);
        Assert.Contains("darling_metrics_last_refresh_success 1\n", fresh, StringComparison.Ordinal);
        Assert.Contains("darling_server_health_band{server=\"metrics-scrape-failing\"}", fresh, StringComparison.Ordinal);

        Volatile.Write(ref failing, true);
        time.Advance(DarlingMetricsSnapshotCache.RefreshAge);
        var failed = "";
        TestWait.WaitUntil(
            () => (failed = Scrape(client, host.Port, ct)).Contains("darling_metrics_last_refresh_success 0\n", StringComparison.Ordinal),
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(100));
        Assert.Contains("darling_server_health_band{server=\"metrics-scrape-failing\"}", failed, StringComparison.Ordinal);
        Assert.Single(logger.Lines, l => l.Level == LogLevel.Warning && l.Text.Contains("Metrics refresh failed", StringComparison.Ordinal));

        time.Advance(DarlingMetricsSnapshotCache.DropAge - DarlingMetricsSnapshotCache.RefreshAge);
        var dropped = Scrape(client, host.Port, ct);
        Assert.Contains("darling_metrics_last_refresh_success 0\n", dropped, StringComparison.Ordinal);
        Assert.Contains("darling_metrics_snapshot_age_seconds 300\n", dropped, StringComparison.Ordinal);
        Assert.DoesNotContain("darling_server_", dropped, StringComparison.Ordinal);
        ParseStrictly(dropped);
    }

    private sealed class RunningHost(DarlingMetricsHostService host, int port) : IAsyncDisposable
    {
        public int Port { get; } = port;

        public async ValueTask DisposeAsync()
        {
            await host.StopAsync(CancellationToken.None);
            await host.DisposeAsync();
        }
    }

    private static async Task<RunningHost> StartHostAsync(
        Func<CancellationToken, Task<NpgsqlDataSource?>> dataSource, DarlingMetricsHostTests.CapturingLogger logger, TimeProvider? time, CancellationToken ct)
    {
        var port = DarlingHealthHostTests.FreePort();
        var config = new DarlingConfig
        {
            Metrics = new MetricsConfig { Enabled = true, Port = port, Listen = "127.0.0.1", Token = Token },
        };
        var collector = new CollectorRuntimeState();
        collector.PublishCollecting();
        var host = new DarlingMetricsHostService(
            collector, new ReadAdmission(), new MonitoredServerRegistryState(), logger, () => config,
            inContainer: false, dataSourceOverride: dataSource, time: time);
        await host.StartAsync(ct);
        Assert.True(host.IsListening, string.Join(" | ", logger.Lines.Select(l => l.Text)));
        return new RunningHost(host, port);
    }

    private static async Task<string> ScrapeAsync(HttpClient client, int port, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, $"http://127.0.0.1:{port}/metrics");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Token);
        using var response = await client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        return await response.Content.ReadAsStringAsync(ct);
    }

    private static string Scrape(HttpClient client, int port, CancellationToken ct) => ScrapeAsync(client, port, ct).GetAwaiter().GetResult();

    /// <summary>The first scrape starts the refresh and answers the meta series only; later scrapes answer the snapshot once it is
    /// built.</summary>
    private static async Task<string> ScrapeUntilSnapshotAsync(HttpClient client, int port, CancellationToken ct)
    {
        var first = await ScrapeAsync(client, port, ct);
        Assert.DoesNotContain("darling_server_", first, StringComparison.Ordinal);
        Assert.DoesNotContain("darling_metrics_snapshot_age_seconds", first, StringComparison.Ordinal);

        var body = "";
        TestWait.WaitUntil(
            () => (body = Scrape(client, port, ct)).Contains("darling_metrics_snapshot_age_seconds ", StringComparison.Ordinal),
            TimeSpan.FromSeconds(30),
            TimeSpan.FromMilliseconds(100));
        return body;
    }

    /// <summary>
    /// Every line is a HELP or TYPE comment for a catalog metric or a sample of one, and nothing else. A sample's label names are
    /// exactly the catalog's for its metric, in order, and its value is a finite number.
    /// </summary>
    private static List<(string Metric, List<KeyValuePair<string, string>> Labels, double Value)> ParseStrictly(string body)
    {
        Assert.EndsWith("\n", body, StringComparison.Ordinal);
        var samples = new List<(string, List<KeyValuePair<string, string>>, double)>();
        foreach (var line in body[..^1].Split('\n'))
        {
            if (line.StartsWith('#'))
            {
                var comment = CommentLine.Match(line);
                Assert.True(comment.Success, $"an unparsable comment line: {line}");
                Assert.True(DarlingMetricCatalog.Metrics.ContainsKey(comment.Groups["name"].Value), line);
                if (comment.Groups["kind"].Value == "TYPE")
                {
                    Assert.Equal("gauge", comment.Groups["text"].Value);
                }

                continue;
            }

            var sample = SampleLine.Match(line);
            Assert.True(sample.Success, $"an unparsable sample line: {line}");
            var metric = sample.Groups["name"].Value;
            Assert.True(DarlingMetricCatalog.Metrics.TryGetValue(metric, out var entry), $"not a catalog metric: {line}");
            Assert.True(
                double.TryParse(sample.Groups["value"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) && double.IsFinite(value),
                $"not a finite value: {line}");

            var labels = new List<KeyValuePair<string, string>>();
            var labelText = sample.Groups["labels"].Value;
            var position = 0;
            while (position < labelText.Length)
            {
                var pair = LabelPair.Match(labelText, position);
                Assert.True(pair.Success, $"unparsable labels: {line}");
                labels.Add(new(pair.Groups["key"].Value, Unescape(pair.Groups["value"].Value)));
                position += pair.Length;
            }

            Assert.Equal(entry.Labels, labels.Select(l => l.Key));
            samples.Add((metric, labels, value));
        }

        Assert.NotEmpty(samples);
        return samples;
    }

    private static string Unescape(string value) =>
        value.Replace("\\n", "\n", StringComparison.Ordinal).Replace("\\\"", "\"", StringComparison.Ordinal).Replace("\\\\", "\\", StringComparison.Ordinal);

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string serverName, string displayName, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $3, TRUE, 0, $4)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(displayName);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One collect row: the four key columns, then the payload columns given (DarlingMetricsServerSetReaderLiveTests' shape).</summary>
    private static async Task InsertRowAsync(
        NpgsqlConnection connection, string table, int serverId, string name, DateTime collectionTime, CancellationToken ct,
        params (string Column, object? Value)[] payload)
    {
        var columns = string.Join(", ", payload.Select(p => p.Column));
        var placeholders = string.Join(", ", payload.Select((_, i) => "$" + (i + 5).ToString(CultureInfo.InvariantCulture)));
        using var command = new NpgsqlCommand(
            $"INSERT INTO {table} (collection_id, collection_time, server_id, server_name, {columns}) VALUES ($1, $2, $3, $4, {placeholders})",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        foreach (var (_, value) in payload)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertCpuAsync(NpgsqlConnection c, int serverId, string name, DateTime at, CancellationToken ct) =>
        InsertRowAsync(c, "cpu_utilization_stats", serverId, name, at, ct,
            ("sample_time", DateTime.SpecifyKind(at, DateTimeKind.Unspecified)),
            ("sqlserver_cpu_utilization", 20),
            ("other_process_cpu_utilization", 5));

    private static Task InsertFileIoAsync(NpgsqlConnection c, int serverId, string name, DateTime at, string database, string fileType, CancellationToken ct) =>
        InsertRowAsync(c, "file_io_stats", serverId, name, at, ct,
            ("database_name", database), ("file_name", database + "_file"), ("file_type", fileType),
            ("physical_name", "C:\\" + database + ".mdf"),
            ("delta_reads", 10L), ("delta_writes", 5L), ("delta_read_bytes", 1_048_576L), ("delta_write_bytes", 2_097_152L),
            ("delta_stall_read_ms", 50L), ("delta_stall_write_ms", 10L), ("sample_interval_seconds", 60));

    private static Task InsertWaitAsync(NpgsqlConnection c, int serverId, string name, DateTime at, string waitType, CancellationToken ct) =>
        InsertRowAsync(c, "wait_stats", serverId, name, at, ct,
            ("wait_type", waitType), ("wait_time_ms", 601L), ("delta_wait_time_ms", 600L),
            ("delta_signal_wait_time_ms", 60L), ("sample_interval_seconds", 60));

    private static Task InsertSessionsAsync(NpgsqlConnection c, int serverId, string name, DateTime at, CancellationToken ct) =>
        InsertRowAsync(c, "session_summary_stats", serverId, name, at, ct,
            ("running_sessions", 1), ("sleeping_sessions", 2), ("background_sessions", 3), ("dormant_sessions", 4),
            ("top_application_name", TopApp), ("top_host_name", TopHost));

    private static Task InsertTempdbAsync(NpgsqlConnection c, int serverId, string name, DateTime at, CancellationToken ct) =>
        InsertRowAsync(c, "tempdb_stats", serverId, name, at, ct,
            ("user_object_reserved_mb", 1m), ("internal_object_reserved_mb", 2m),
            ("version_store_reserved_mb", 3m), ("unallocated_mb", 4m),
            ("log_used_percent", 5m),
            ("holder_program_name", TopApp), ("top_session_id", 99));
}
