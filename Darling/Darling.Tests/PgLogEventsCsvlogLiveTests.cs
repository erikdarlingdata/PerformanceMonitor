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
/// The csvlog route's own reasoning for existing (#4053 part a1): a failed login's role or database name lands
/// in the STDERR log unescaped, so a newline planted in either forges a whole extra line that a regex prefix
/// reader takes for the server's own. Under csvlog every field PostgreSQL writes is RFC 4180 quoted, so the
/// same planted newline stays inside its own field. This is the LIVE proof, gated on
/// <c>DARLING_TEST_PG_CSVLOG</c>: a rig with <c>log_destination = csvlog</c>, two forged login attempts sent as
/// raw <c>StartupMessage</c>s (a fake csv record embedded in a user name, and a fake stderr line embedded in
/// another), read back through <see cref="PgLogFormatCapability"/> and <see cref="PgLogEventsCollector"/>'s
/// OWN query and <see cref="PgLogEventsCollector.ReadAsync"/> — never a hand-copied restatement of either.
///
/// <para>Serialized against every other live class in the shared collection because it writes the shared
/// <c>DARLING_TEST_PG</c> store; teardown goes through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgLogEventsCsvlogLiveTests
{
    private const string ServerName = "darling-pg-csvlog-a1b";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private static string? TargetConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG_CSVLOG");

    /// <summary>
    /// A forged login whose user name is itself a fake CSV record: PostgreSQL rejects the login (there is no
    /// such role), and csvlog quotes the whole name — including the embedded comma-separated fields and the
    /// literal newline — inside ONE field of the real FATAL record, so it can never start a second record.
    /// Trimmed to fit PostgreSQL's 63-byte role-name limit (NAMEDATALEN - 1): the role name is truncated at
    /// the wire before it ever reaches the log, so the marker text has to survive that cut, not just the
    /// parser's.
    /// </summary>
    private const string ForgedCsvUserName =
        "x\n2026-09-24 00:00:00.0 UTC,\"forged\",\"z\",1,\"a\",1.1,0,\"x\",...";

    /// <summary>
    /// A forged login whose user name is a fake STDERR-format line: under the stderr route, an unescaped
    /// newline here would start what reads as a second, independent log line at pid 1, severity LOG, message
    /// "forged". Under csvlog, RFC 4180 quoting keeps it inside this one field too.
    /// </summary>
    private const string ForgedStderrUserName =
        "y\n2026-09-24 00:00:00.000 UTC [1] LOG:  forged";

    [Fact]
    public async Task TheCsvlogRoute_KeepsAPlantedNewlineInsideItsOwnField_AndNeverForgesALine()
    {
        var store = StoreConnectionString;
        var target = TargetConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(store) || string.IsNullOrEmpty(target),
            "Set DARLING_TEST_PG (store) and DARLING_TEST_PG_CSVLOG (a PostgreSQL started with logging_collector=on "
            + "and log_destination=csvlog whose log the login can read) to run the #4053 part a1b csvlog live test.");

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

            await SendForgedLoginAsync(target!, ForgedCsvUserName, ct);
            await SendForgedLoginAsync(target!, ForgedStderrUserName, ct);

            await using var targetConnection = new NpgsqlConnection(target);
            await targetConnection.OpenAsync(ct);

            await WaitForLogGrowthAsync(targetConnection, ct);

            var usesCsvlog = await PgLogFormatCapability.IsCsvlogEnabledAsync(targetConnection, target!, ct);
            Assert.True(usesCsvlog, "the rig's log_destination must include csvlog for this route to be exercised.");

            foreach (var binaryGranted in new[] { false, true })
            {
                var context = new CollectorContext
                {
                    LogHashKey = TestLogHashKeys.Fixed,
                    ServerId = ServerId, ServerName = ServerName,
                    CollectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
                    Deltas = new CollectorDeltaCalculator(),
                    Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
                    PgLogUsesCsvlog = true,
                    PgReadBinaryFileGranted = binaryGranted,
                };

                var definition = PgLogEventsCollector.Instance;
                await using var command = new NpgsqlCommand(definition.BuildQuery(context).Text, targetConnection);
                await using var reader = await command.ExecuteReaderAsync(ct);
                var rows = await definition.ReadAsync(reader, context, ct);

                /* The forged text lands INSIDE the two real FATAL auth-failure events' own message text —
                   the record each was planted into — never as a separate line of its own. */
                var fatalCount = rows.Count(r =>
                    r.Severity == "FATAL"
                    && r.Message != null
                    && r.Message.Contains("forged", StringComparison.Ordinal));
                Assert.True(fatalCount >= 2,
                    $"expected at least the two forged FATAL events this run planted; got {fatalCount}.");

                /* No event was forged OUT of either planted newline: no pid 1, no severity LOG with message
                   "forged" standing on its own, no user "fake" from the csvlog record's own field. */
                Assert.DoesNotContain(rows, r => r.Pid == 1);
                Assert.DoesNotContain(rows, r => r.Severity == "LOG" && r.Message == "forged");
                Assert.DoesNotContain(rows, r => r.UserName == "fake" || r.UserName == "z");

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
    /// put a newline into a startup parameter itself, and the point of this test is what the SERVER's csvlog
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
    /// Polls <c>pg_ls_logdir()</c> for a <c>.csv</c> file whose size has grown since the call started, up to
    /// 5 seconds — the syslogger flushes asynchronously, so the forged lines are not guaranteed to be on disk
    /// the instant the TCP round trip above returns.
    /// </summary>
    private static async Task WaitForLogGrowthAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        long before = 0;
        await using (var command = new NpgsqlCommand(
            "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.csv$'", connection))
        {
            before = (long)(await command.ExecuteScalarAsync(ct))!;
        }

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            await using (var command = new NpgsqlCommand(
                "SELECT coalesce(max(size), 0) FROM pg_catalog.pg_ls_logdir() WHERE name ~* '\\.csv$'", connection))
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
