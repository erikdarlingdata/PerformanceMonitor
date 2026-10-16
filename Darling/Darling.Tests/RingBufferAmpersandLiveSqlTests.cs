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
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5948: SQL Server writes the <c>objectname</c> and <c>indexname</c> of a <c>keylock</c> in an <c>xml_deadlock_report</c> graph
/// without escaping <c>&amp;</c>, so one deadlock on a table named <c>a&amp;x</c> made the session's whole ring buffer
/// unparseable. The collectors' plain <c>TRY_CAST(target_data AS xml)</c> gave NULL, the insert into a <c>ring_buffer xml NOT
/// NULL</c> table variable failed, and every later run failed the same way until the session restarted.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. The deadlock test CREATEs
/// and DROPs its own scratch database and its own server-scoped session (the own-name form, so it never touches the shared
/// <c>PerformanceMonitor_Deadlock</c> session a live install collects from). <c>DARLING_TEST_SQL_READONLY=1</c> skips the
/// whole class: the deadlock test creates objects, and a server that holds real monitoring data must not get them.</para>
/// </summary>
public sealed class RingBufferAmpersandLiveSqlTests
{
    private static string? ConnectionString(string applicationName, string database = "master")
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
            InitialCatalog = database,
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

    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live ring-buffer ampersand tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs the shared parse fragment over a literal buffer text and returns the parsed xml, or null when the text is NULL.</summary>
    private static async Task<string?> ParseAsync(string connectionString, string? text, CancellationToken ct)
    {
        var sql = RingBufferXml.DeclareSql + @"
SET @ring_text = @text;
SET @ring_session = N'PerformanceMonitor_Deadlock';
" + RingBufferXml.ParseSql + @"
SELECT CONVERT(nvarchar(max), @ring_xml);";

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        command.Parameters.Add("@text", System.Data.SqlDbType.NVarChar, -1).Value = (object?)text ?? DBNull.Value;
        var result = await command.ExecuteScalarAsync(ct);
        return result is string s ? s : null;
    }

    [Theory]
    /* The reported shape: a bare ampersand in an attribute value. */
    [InlineData("<r><e a=\"a&x\"/></r>", "a&x")]
    /* The five named entities survive the pass as themselves; only the stray ampersand is escaped. */
    [InlineData("<r><e a=\"1&lt;2 &gt;0 &amp; &quot;q&quot; &apos;s&apos; b&x\"/></r>", "1<2 >0 & \"q\" 's' b&x")]
    /* A numeric character reference becomes literal text once the plain cast has failed (documented, accepted). */
    [InlineData("<r><e a=\"&#65;&x\"/></r>", "&#65;&x")]
    /* A buffer that already parses is untouched: the numeric reference is still a character. */
    [InlineData("<r><e a=\"&#65;&amp;x\"/></r>", "A&x")]
    public async Task ParseSql_EscapesStrayAmpersandsAndKeepsTheFiveNamedEntities(string buffer, string expectedAttribute)
    {
        var connectionString = ConnectionString("pm-5948-parse-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var xml = await ParseAsync(connectionString!, buffer, ct);

        Assert.NotNull(xml);
        Assert.Equal(expectedAttribute, (string?)XElement.Parse(xml!).Element("e")!.Attribute("a"));
    }

    [Fact]
    public async Task ParseSql_NullText_ReadsAsNoBufferAndDoesNotThrow()
    {
        var connectionString = ConnectionString("pm-5948-parse-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);

        Assert.Null(await ParseAsync(connectionString!, null, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ParseSql_BufferThatStillWillNotParse_RaisesAClearErrorInsteadOfInsertingNull()
    {
        var connectionString = ConnectionString("pm-5948-parse-live-test");
        Assert.SkipWhen(connectionString is null, SkipMessage);

        const string broken = "<r><a></r>";
        var ex = await Assert.ThrowsAsync<SqlException>(
            () => ParseAsync(connectionString!, broken, TestContext.Current.CancellationToken));

        Assert.Contains("PerformanceMonitor_Deadlock ring buffer (" + broken.Length + " characters) is not XML SQL Server can parse:", ex.Message, StringComparison.Ordinal);
        Assert.Contains("restarting the session clears it", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The other three collectors that read a ring buffer (system health, blocked process, long-query completions) run their
    /// real batch, fragment included, against a live server. system_health exists on every instance; the other two name an own
    /// session that does not exist, which must read as no rows rather than an error.
    /// </summary>
    [Fact]
    public async Task TheOtherRingBufferCollectorsBatches_RunAgainstALiveServer()
    {
        var master = ConnectionString("pm-5948-batches-live-test");
        Assert.SkipWhen(master is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];

        CollectorContext Context(string? alwaysOnSession = null, string? longQuerySession = null) => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = CollectorContext.NoState,
            AlwaysOnSessionName = alwaysOnSession,
            LongQuerySessionName = longQuerySession,
            Watermark = DateTime.UtcNow.AddMinutes(-10),
        };

        var queries = new[]
        {
            SystemHealthEventsCollector.Instance.BuildQuery(Context()),
            BlockedProcessReportCollector.Instance.BuildQuery(Context(
                AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, suffix, AlwaysOnXeSessionKind.BlockedProcess))),
            LongQueryCompletionsCollector.Instance.BuildQuery(Context(
                null, LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.DarlingProduct, suffix))),
        };

        foreach (var query in queries)
        {
            await using var connection = new SqlConnection(master);
            await connection.OpenAsync(ct);
            await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
            await using var reader = await command.ExecuteReaderAsync(ct);
            do
            {
                while (await reader.ReadAsync(ct))
                {
                }
            }
            while (await reader.NextResultAsync(ct));
        }
    }

    /// <summary>
    /// The reproduction: two tables (and their primary keys) named with an ampersand, a real deadlock between them, and the
    /// deadlocks collector's REAL batch and <c>ReadAsync</c> over the session that captured it.
    /// </summary>
    [Fact]
    public async Task DeadlockOnTablesNamedWithAnAmpersand_IsCollectedAndTheObjectNameReadsWithTheAmpersand()
    {
        var master = ConnectionString("pm-5948-deadlock-live-test");
        Assert.SkipWhen(master is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var database = "pm5948_" + suffix;
        var sessionName = AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, suffix, AlwaysOnXeSessionKind.Deadlock);
        Assert.True(AlwaysOnXeSessions.IsOwnName(sessionName, AlwaysOnXeSessionKind.Deadlock));

        var databaseCreated = false;
        var sessionCreated = false;
        try
        {
            await ExecuteAsync(master!, "CREATE DATABASE [" + database + "];", ct);
            databaseCreated = true;

            var scratch = ConnectionString("pm-5948-deadlock-live-test", database)!;
            await ExecuteAsync(scratch, @"
CREATE TABLE dbo.[a&x] (id integer NOT NULL, v integer NOT NULL, CONSTRAINT [pk_a&x] PRIMARY KEY CLUSTERED (id));
CREATE TABLE dbo.[b&x] (id integer NOT NULL, v integer NOT NULL, CONSTRAINT [pk_b&x] PRIMARY KEY CLUSTERED (id));
INSERT dbo.[a&x] (id, v) VALUES (1, 0);
INSERT dbo.[b&x] (id, v) VALUES (1, 0);", ct);

            /* A server-scoped session on the deadlock graph with a ring buffer, the shape the install makes, under an own name. */
            await ExecuteAsync(master!, "CREATE EVENT SESSION [" + sessionName + @"] ON SERVER
ADD EVENT sqlserver.xml_deadlock_report
ADD TARGET package0.ring_buffer
WITH (MAX_MEMORY = 4096 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, STARTUP_STATE = OFF);
ALTER EVENT SESSION [" + sessionName + "] ON SERVER STATE = START;", ct);
            sessionCreated = true;

            await ForceDeadlockAsync(scratch, ct);

            /* The ring buffer gets the event up to MAX_DISPATCH_LATENCY later. */
            List<DeadlocksCollector.Row> rows = new();
            var deadline = DateTime.UtcNow.AddSeconds(45);
            while (true)
            {
                rows = await RunCollectorAsync(master!, sessionName, ct);
                if (rows.Exists(r => r.GraphXml is not null && r.GraphXml.Contains(database, StringComparison.Ordinal)) || DateTime.UtcNow > deadline)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }

            var row = Assert.Single(rows, r => r.GraphXml is not null && r.GraphXml.Contains(database, StringComparison.Ordinal));
            Assert.NotNull(row.DeadlockTime);
            Assert.False(string.IsNullOrEmpty(row.VictimProcessId));

            var graph = XElement.Parse(row.GraphXml!);
            var objectNames = graph.Descendants("keylock").Select(k => (string?)k.Attribute("objectname")).ToList();
            Assert.Contains(database + ".dbo.a&x", objectNames);
            Assert.Contains(database + ".dbo.b&x", objectNames);
            var indexNames = graph.Descendants("keylock").Select(k => (string?)k.Attribute("indexname")).ToList();
            Assert.Contains("pk_a&x", indexNames);
        }
        finally
        {
            if (sessionCreated)
            {
                await ExecuteAsync(master!, "DROP EVENT SESSION [" + sessionName + "] ON SERVER;", CancellationToken.None);
            }

            if (databaseCreated)
            {
                await ExecuteAsync(master!, "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + database + "];", CancellationToken.None);
            }
        }
    }

    /// <summary>Two sessions each hold one table and ask for the other's, so SQL Server's deadlock monitor kills one with 1205.</summary>
    private static async Task ForceDeadlockAsync(string scratch, CancellationToken ct)
    {
        await using var first = new SqlConnection(scratch);
        await using var second = new SqlConnection(scratch);
        await first.OpenAsync(ct);
        await second.OpenAsync(ct);

        await RunAsync(first, "BEGIN TRANSACTION; UPDATE dbo.[a&x] SET v = 1 WHERE id = 1;", ct);
        await RunAsync(second, "BEGIN TRANSACTION; UPDATE dbo.[b&x] SET v = 1 WHERE id = 1;", ct);

        var t1 = TryAsync(first, "UPDATE dbo.[b&x] SET v = 2 WHERE id = 1;", ct);
        var t2 = TryAsync(second, "UPDATE dbo.[a&x] SET v = 2 WHERE id = 1;", ct);
        var errors = await Task.WhenAll(t1, t2);

        Assert.Single(errors, e => e == 1205);

        foreach (var connection in new[] { first, second })
        {
            try
            {
                await RunAsync(connection, "IF @@TRANCOUNT > 0 ROLLBACK;", ct);
            }
            catch (SqlException)
            {
                /* The victim's transaction is already gone. */
            }
        }
    }

    private static async Task RunAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs the statement; returns the SQL error number it died with (1205 for the deadlock victim), or 0.</summary>
    private static async Task<int> TryAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        try
        {
            await RunAsync(connection, sql, ct);
            return 0;
        }
        catch (SqlException ex)
        {
            return ex.Number;
        }
    }

    /// <summary>Runs the collector's real batch and its real <c>ReadAsync</c>, the way the host does.</summary>
    private static async Task<List<DeadlocksCollector.Row>> RunCollectorAsync(string connectionString, string sessionName, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            State = CollectorContext.NoState,
            AlwaysOnSessionName = sessionName,
            Watermark = DateTime.UtcNow.AddMinutes(-10),
        };
        var query = DeadlocksCollector.Instance.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return (await DeadlocksCollector.Instance.ReadAsync(reader, context, ct)).ToList();
    }
}
