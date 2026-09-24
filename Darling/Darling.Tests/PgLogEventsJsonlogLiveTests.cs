/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The jsonlog route's own reasoning for existing (#4053 part a2): under jsonlog every field PostgreSQL
/// writes is JSON-string-escaped, so a newline planted in a failed login's user name lands in the log as a
/// literal two-character escape sequence, never a real line break — it can never start a second record. This
/// is the LIVE proof, gated on <c>DARLING_TEST_PG_JSONLOG</c>: a rig with <c>log_destination = jsonlog</c>, a
/// forged login attempt sent as a raw <c>StartupMessage</c> whose user name carries a newline and a
/// stderr-format look-alike line, read back through <see cref="PgLogFormatCapability"/> and
/// <see cref="PgLogEventsCollector"/>'s OWN query and <see cref="PgLogEventsCollector.ReadAsync"/> — never a
/// hand-copied restatement of either.
///
/// <para>Serialized against every other live class in the shared collection because it writes the shared
/// <c>DARLING_TEST_PG</c> store; teardown goes through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogEventsJsonlogLiveTests
{
    private const string ServerName = "darling-pg-jsonlog-a2";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private static string? TargetConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_JSONLOG");

    /// <summary>
    /// A forged login whose user name is a fake STDERR-format line: an unescaped newline here would start
    /// what reads as a second, independent log line at pid 1, severity LOG, message "forged". Under jsonlog,
    /// JSON string escaping keeps the newline as a literal escape inside this one field's value. Trimmed to
    /// fit PostgreSQL's 63-byte role-name limit (NAMEDATALEN - 1): the role name is truncated at the wire
    /// before it ever reaches the log, so the marker text has to survive that cut, not just the parser's.
    /// </summary>
    private const string ForgedStderrUserName =
        "y\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged";

    /// <summary>The one probe, live: on a jsonlog-only target it answers jsonlog on and csvlog off, from a
    /// single cached round trip.</summary>
    [Fact]
    public async Task TheProbe_OnAJsonlogOnlyTarget_SaysJsonlogOnAndCsvlogOff()
    {
        var target = TargetConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG_JSONLOG (a PostgreSQL started with log_destination=jsonlog) to run the probe live test.");

        var ct = TestContext.Current.CancellationToken;
        /* No try/finally: nothing here writes the store, and the cache is process-static state every
           capability test resets on entry, so a failed assertion leaves nothing another test depends on. */
        PgLogFormatCapability.Reset();
        await using var connection = new NpgsqlConnection(target);
        await connection.OpenAsync(ct);

        Assert.True(await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "jsonlog-probe-live", ct));
        Assert.False(await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "jsonlog-probe-live", ct));
        PgLogFormatCapability.Reset();
    }

    [Fact]
    public async Task TheJsonlogRoute_KeepsAPlantedNewlineInsideItsOwnField_AndNeverForgesALine()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_JSONLOG (a PostgreSQL started with logging_collector=on "
            + "and log_destination=jsonlog whose log the login can read) to run the #4053 part a2 jsonlog live test.");

        var ct = TestContext.Current.CancellationToken;
        using var storeConnection = new NpgsqlConnection(store);
        await storeConnection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(storeConnection, ct);
        await DarlingMcpTestData.ExecAsync(storeConnection, ct, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId);

        PgLogFormatCapability.Reset();

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(storeConnection, ServerId, ServerName, ct);

            await SendForgedLoginAsync(target!, ForgedStderrUserName, ct);

            await using var targetConnection = new NpgsqlConnection(target);
            await targetConnection.OpenAsync(ct);

            await WaitForLogGrowthAsync(targetConnection, ct);

            var usesJsonlog = await PgLogFormatCapability.IsJsonlogEnabledAsync(targetConnection, target!, ct);
            Assert.True(usesJsonlog, "the rig's log_destination must include jsonlog for this route to be exercised.");

            var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(targetConnection, target!, ct);
            Assert.False(usesCsvlog, "the rig's log_destination must not include csvlog for this to prove the jsonlog route alone.");

            foreach (var binaryGranted in new[] { false, true })
            {
                var context = new CollectorContext
                {
                    LogHashKey = TestLogHashKeys.Fixed,
                    ServerId = ServerId, ServerName = ServerName,
                    CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                    Deltas = new CollectorDeltaCalculator(),
                    Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                    PgLogUsesJsonlog = true,
                    PgReadBinaryFileGranted = binaryGranted,
                };

                var definition = PgLogEventsCollector.Instance;
                await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, targetConnection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var rows = await definition.ReadAsync(reader, context, ct);

                /* The forged text lands INSIDE the real FATAL auth-failure event's own message text — the
                   record it was planted into — never as a separate line of its own. */
                var fatalCount = rows.Count(r =>
                    r.Severity == "FATAL"
                    && r.Message != null
                    && r.Message.Contains("forged", StringComparison.Ordinal));
                Assert.True(fatalCount >= 1,
                    $"expected at least the one forged FATAL event this run planted; got {fatalCount}.");

                /* No event was forged OUT of the planted newline: no pid 1, no severity LOG with message
                   "forged" standing on its own. */
                Assert.DoesNotContain(rows, r => r.Pid == 1);
                Assert.DoesNotContain(rows, r => r.Severity == "LOG" && r.Message == "forged");

                Assert.NotEmpty(rows);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM pg_log_events WHERE server_id = $1", ServerId));
        }
    }

    /// <summary>
    /// Sends a raw <c>StartupMessage</c> carrying <paramref name="userName"/> — which PostgreSQL will refuse
    /// (no such role exists), producing a FATAL auth-failure log entry whose user field is exactly what was
    /// sent — then discards the server's reply and closes. No client library involved: Npgsql would refuse to
    /// put a newline into a startup parameter itself, and the point of this test is what the SERVER's jsonlog
    /// writer does with one, not what a client permits.
    /// </summary>
    private static async Task SendForgedLoginAsync(string connectionString, string userName, CancellationToken ct)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var host = builder.Host ?? "localhost";
        var port = builder.Port == 0 ? 5432 : builder.Port;

        using var client = new TcpClient();
        await client.ConnectAsync(host, port, ct);
        using var stream = client.GetStream();

        var parameters = Encoding.UTF8.GetBytes("user\0" + userName + "\0database\0postgres\0\0");
        var length = 4 + 4 + parameters.Length;

        var message = new byte[length];
        WriteBigEndianInt32(message, 0, length);
        WriteBigEndianInt32(message, 4, 196608);
        Buffer.BlockCopy(parameters, 0, message, 8, parameters.Length);

        await stream.WriteAsync(message, ct);

        /* Read and discard whatever the server sends back (an ErrorResponse, here) — this test cares only
           about what the server's log writer did with the forged name, not about the wire reply. */
        var discard = new byte[4096];
        try
        {
            using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            readCts.CancelAfter(TimeSpan.FromSeconds(5));
            _ = await stream.ReadAsync(discard, readCts.Token);
        }
        catch (OperationCanceledException)
        {
            /* The server may close the connection without a reply once it decides to reject — either way,
               the StartupMessage already reached its log by the time this returns. */
        }
        catch (IOException)
        {
        }
    }

    private static void WriteBigEndianInt32(byte[] buffer, int offset, int value)
    {
        buffer[offset] = (byte)(value >> 24);
        buffer[offset + 1] = (byte)(value >> 16);
        buffer[offset + 2] = (byte)(value >> 8);
        buffer[offset + 3] = (byte)value;
    }

    /// <summary>
    /// Polls <c>pg_ls_logdir()</c> for a <c>.json</c> file whose size has grown since the call started, up to
    /// 5 seconds — the syslogger flushes asynchronously, so the forged line is not guaranteed to be on disk
    /// the instant the TCP round trip above returns.
    /// </summary>
    private static async Task WaitForLogGrowthAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        long before = 0;
        await using (var command = new NpgsqlCommand(
            "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.json$'", connection))
        {
            before = (long)(await command.ExecuteScalarAsync(ct))!;
        }

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            await using (var command = new NpgsqlCommand(
                "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.json$'", connection))
            {
                var after = (long)(await command.ExecuteScalarAsync(ct))!;
                if (after > before)
                {
                    return;
                }
            }

            await Task.Delay(200, ct);
        }
    }
}
