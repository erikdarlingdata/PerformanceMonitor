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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Golden-fixture pin for the FinOps Application Connections read (<c>GetApplicationConnectionsAsync</c>). A
/// deterministic seeded store is read and the rows are serialized (declaration-order public properties; the UTC times
/// as minutes from the seed instant) and compared byte for byte with
/// <c>Fixtures/FinOpsApplicationConnections/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c> to regenerate it.
///
/// <para>The display times (<c>FirstSeenLocal</c>, <c>LastSeenLocal</c>) and the stamped clock are left out of the
/// fixture: they depend on the viewer's display mode, a process-wide static that other classes change, and on the
/// machine's zone. They are checked against the UTC instants in a class that owns that static.</para>
///
/// <para>Server A holds: two programs tied on their peak connection count; a NULL program name; programs whose
/// resource columns are NULL on some rows and on every row; averages that do not divide evenly, so the
/// <c>CAST(AVG(...) AS INTEGER)</c> rounding shows; and a row just outside the 24-hour window (a program seen only
/// there is absent, and an outside row of an included program does not move its figures). Server B has no rows.</para>
/// </summary>
public sealed class FinOpsApplicationConnectionsGoldenLiveTests
{
    internal const string ServerNameA = "darling-finops-appconn-golden-a";
    internal const string ServerNameB = "darling-finops-appconn-golden-b";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    internal static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);

    /// <summary>Names the row properties that depend on the display mode or the machine zone (left out of the fixture).</summary>
    internal static readonly string[] DisplayOnly = ["FirstSeenLocal", "LastSeenLocal", "Clock"];

    [Fact]
    public Task ApplicationConnections_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            return Serialize(anchor, new Dictionary<string, object?>
            {
                ["a"] = await viewer.GetApplicationConnectionsAsync(ServerIdA, ct),
                ["b"] = await viewer.GetApplicationConnectionsAsync(ServerIdB, ct),
            });
        });

    internal static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps application connections golden test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        DateTime anchor;
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            anchor = DarlingMcpTestData.Naive(DateTime.UtcNow);
            await SeedAsync(connection, anchor, ct);
        }

        AssertGolden(await read(scratch.ConnectionString, anchor, ct));
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);

        /* Orders: connections, running, sleeping, dormant, cpu ms, reads, writes, logical reads (NULL = not collected). */
        /* Alpha: peak 12 connections. Connections 12, 5, 6 average 7.67 -> 8; running 1, 2, 2 average 1.67 -> 2; the
           cpu column averages 1000, 1001, NULL (AVG skips the NULL) -> 1000.5 -> 1001 (BIGINT cast of a numeric rounds). */
        await Row(connection, ct, now.AddHours(-1), "Alpha", 12, 1, 4, 2, 1000, 500, 50, 9000);
        await Row(connection, ct, now.AddHours(-5), "Alpha", 5, 2, 3, 0, 1001, 501, 51, 9001);
        await Row(connection, ct, now.AddHours(-9), "Alpha", 6, 2, 4, 1, null, null, null, null);
        /* Just outside the window: it must not move Alpha's figures. */
        await Row(connection, ct, now.AddHours(-25), "Alpha", 900, 90, 90, 90, 9_999_999, 9_999_999, 9_999_999, 9_999_999);
        /* Bravo ties Alpha on peak connections (12); its resource columns are NULL on every row. */
        await Row(connection, ct, now.AddHours(-2), "Bravo", 12, 3, 5, 4, null, null, null, null);
        await Row(connection, ct, now.AddHours(-3), "Bravo", 1, 0, 1, 0, null, null, null, null);
        /* Charlie: a single row. */
        await Row(connection, ct, now.AddHours(-4), "Charlie", 3, 1, 1, 1, 7, 8, 9, 10);
        /* An unattributed program name. */
        await Row(connection, ct, now.AddHours(-6), null, 20, 0, 0, 0, 100, 200, 300, 400);
        await Row(connection, ct, now.AddHours(-7), null, 1, 0, 1, 0, 101, 201, 301, 401);
        /* Seen only outside the window: absent. */
        await Row(connection, ct, now.AddHours(-26), "Outside", 50, 5, 5, 5, 1, 1, 1, 1);
    }

    private static Task Row(NpgsqlConnection c, CancellationToken ct, DateTime at, string? program, long connections,
        int running, int sleeping, int dormant, long? cpu, long? reads, long? writes, long? logicalReads) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count,
                running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_reads, total_writes, total_logical_reads)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, program, connections, running, sleeping, dormant,
            cpu, reads, writes, logicalReads);

    /// <summary>Serializes the rows as indented JSON: public settable properties in declaration order, the display-only
    /// ones left out, and each <c>DateTime</c> as whole minutes from the seed instant.</summary>
    internal static string Serialize(DateTime anchor, object rows)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            WriteValue(writer, anchor, rows);
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    private static void WriteValue(Utf8JsonWriter writer, DateTime anchor, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case DateTime d: writer.WriteNumberValue((long)Math.Round((d - anchor).TotalMinutes)); break;
            case System.Collections.IDictionary map:
                writer.WriteStartObject();
                foreach (System.Collections.DictionaryEntry entry in map)
                {
                    writer.WritePropertyName((string)entry.Key);
                    WriteValue(writer, anchor, entry.Value);
                }
                writer.WriteEndObject();
                break;
            case System.Collections.IEnumerable list:
                writer.WriteStartArray();
                foreach (var item in list) WriteValue(writer, anchor, item);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStartObject();
                foreach (var p in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.SetMethod is { IsPublic: true } && !DisplayOnly.Contains(p.Name))
                             .OrderBy(p => p.MetadataToken))
                {
                    writer.WritePropertyName(p.Name);
                    WriteValue(writer, anchor, p.GetValue(value));
                }
                writer.WriteEndObject();
                break;
        }
    }

    private static string GoldenSourcePath([CallerFilePath] string testFile = "") =>
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsApplicationConnections", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(GoldenSourcePath())!);
            File.WriteAllText(GoldenSourcePath(), actual);
        }

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsApplicationConnections", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
