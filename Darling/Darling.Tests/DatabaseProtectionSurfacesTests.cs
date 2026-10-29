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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6873 part 3b: the Viewer's Database Protection words as functions of the read. They are the words the web page's Health Checks panels
/// show, so the assertions are on the values: the age text, the state words, the platform label, the order and the sentences of a read
/// with no capture. The judgement under them is <see cref="DatabaseProtectionBands"/>.
/// </summary>
public sealed class ViewerDatabaseProtectionTextTests
{
    private static readonly DateTime Captured = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Day = TimeSpan.FromDays(1);

    private static DatabaseProtectionFact Fact(
        string name, string state = "ONLINE", string recovery = "FULL", string platform = "native", string? role = null,
        TimeSpan? checkDbAgo = null, string checkDbState = "checked", TimeSpan? fullAgo = null, string fullState = "recorded",
        TimeSpan? logAgo = null, string logState = "recorded")
    {
        DateTime? Ago(TimeSpan? span) => span is { } v ? Captured - v : null;
        return new DatabaseProtectionFact(
            name, state, recovery, Captured - (400 * Day), role, platform,
            Ago(checkDbAgo), checkDbState, Ago(fullAgo), fullState, Ago(logAgo), logState);
    }

    private static ViewerDatabaseProtectionRead Read(params DatabaseProtectionFact[] facts)
    {
        var banded = facts.Select(f => (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Captured, DatabaseProtectionThresholds.Default))).ToList();
        var summary = DatabaseProtectionBands.Summarize(banded);
        return new ViewerDatabaseProtectionRead(
            summary.OverdueDatabases > 0 ? ViewerDatabaseProtectionRead.Overdue : ViewerDatabaseProtectionRead.Ok, null, Captured, banded, summary);
    }

    [Fact]
    public void AnOverdueDatabase_ReadsItsAgeAndOverdue_AndComesBeforeTheOnesThatAreFine()
    {
        var rows = ViewerDatabaseProtectionText.Rows(Read(
            Fact("alpha", checkDbAgo: 3 * Day, fullAgo: Day, logAgo: TimeSpan.FromMinutes(10)),
            Fact("zeta", checkDbAgo: 20 * Day, fullAgo: 9 * Day + TimeSpan.FromHours(2), logAgo: TimeSpan.FromMinutes(90)),
            Fact("beta", checkDbAgo: 3 * Day, fullAgo: 2 * Day, logAgo: TimeSpan.FromMinutes(42))));

        Assert.Equal(["zeta", "alpha", "beta"], rows.Select(r => r.Database).ToArray());
        Assert.Equal(
            new ViewerDatabaseProtectionRow("zeta", "ONLINE", "9 days 2 hours - overdue", "1 hour 30 minutes - overdue", "20 days - overdue", "", "", true),
            rows[0]);
        Assert.Equal(new ViewerDatabaseProtectionRow("alpha", "ONLINE", "1 day", "10 minutes", "3 days", "", "", false), rows[1]);
    }

    [Fact]
    public void NeverAndNoneInWindow_ReadTheirWord_AndOverdue()
    {
        var row = ViewerDatabaseProtectionText.Rows(Read(Fact("lost", checkDbState: "never", fullState: "none_in_window", logState: "none_in_window")))[0];

        Assert.Equal(("none in window - overdue", "none in window - overdue", "never - overdue"), (row.FullBackup, row.LogBackup, row.CheckDb));
    }

    [Fact]
    public void APlatformManagedTarget_ShowsTheAgeAndTheLabel_AndIsNotOverdueWhenRdsCheckDbIsRecent()
    {
        var read = Read(
            Fact("azure_db", platform: "platform_managed", checkDbAgo: 40 * Day, fullAgo: 30 * Day, logAgo: 5 * Day),
            Fact("rds_db", platform: "native_only", checkDbAgo: Day, fullAgo: 30 * Day, logState: "unavailable"));

        var rows = ViewerDatabaseProtectionText.Rows(read);

        Assert.Equal("azure_db", rows[0].Database);
        Assert.Equal(("30 days (platform managed)", "5 days (platform managed)", "40 days (platform managed)", "platform managed"),
            (rows[0].FullBackup, rows[0].LogBackup, rows[0].CheckDb, rows[0].Platform));
        Assert.Equal(("30 days (native only)", "native only"), (rows[1].FullBackup, rows[1].Platform));
        Assert.Equal("ok", read.Status);
        Assert.DoesNotContain(rows, r => r.Overdue);
        // #6902: the RDS row is not overdue because its CHECKDB passed a day ago, not because it is skipped.
        var rds = read.Databases.Single(d => d.Fact.DatabaseName == "rds_db").Band;
        Assert.Equal(("native_only", "ok"), (rds.FullBackup.State, rds.CheckDb.State));
    }

    [Fact]
    public void UnavailableAndUnknown_PassThroughAsWords_AndNeverOverdue()
    {
        var row = ViewerDatabaseProtectionText.Rows(Read(Fact("hyper", fullState: "unavailable", logState: "unavailable", checkDbState: "unknown")))[0];

        Assert.Equal(("unavailable", "unavailable", "unknown", false), (row.FullBackup, row.LogBackup, row.CheckDb, row.Overdue));
    }

    [Fact]
    public void ASimpleDatabase_ExpectsNoLogBackup_AndAnOfflineOneIsNotJudged()
    {
        var rows = ViewerDatabaseProtectionText.Rows(Read(
            Fact("simple_db", recovery: "SIMPLE", checkDbAgo: Day, fullAgo: Day, logState: "not_expected"),
            Fact("off_db", state: "OFFLINE", checkDbState: "never", fullState: "none_in_window", logState: "none_in_window")));

        var simple = rows.Single(r => r.Database == "simple_db");
        var offline = rows.Single(r => r.Database == "off_db");
        Assert.Equal("not expected", simple.LogBackup);
        Assert.Equal(("excluded", "offline", false), (offline.FullBackup, offline.NotJudgedBecause, offline.Overdue));
    }

    [Fact]
    public void TheStatusLine_NamesTheOverdueDatabases_AndTheCaptureTime()
    {
        var read = Read(Fact("zeta", fullAgo: 9 * Day, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: Day));
        var local = Captured.ToLocalTime().ToString("g", System.Globalization.CultureInfo.CurrentCulture);

        Assert.Equal($"1 of 1 judged databases overdue. full backup: zeta.   Captured: {local}", ViewerDatabaseProtectionText.StatusLine(read));
        Assert.StartsWith("No database is overdue (1 judged).", ViewerDatabaseProtectionText.StatusLine(Read(Fact("a", fullAgo: Day, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: Day))), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("not_collected", "Azure SQL Database does not have this data.")]
    [InlineData("precondition", "The monitoring login was refused SELECT on msdb.dbo.backupset.")]
    [InlineData("unavailable", "No database protection capture in the 24-hour window.")]
    public void ARead_WithoutACapture_ShowsTheReadsOwnSentence_InPlaceOfTheList(string status, string sentence)
    {
        var read = new ViewerDatabaseProtectionRead(status, sentence, null, [], null);

        Assert.Equal(sentence, ViewerDatabaseProtectionText.StatusLine(read));
        Assert.Equal(sentence, ViewerDatabaseProtectionText.EmptyText(read));
        Assert.Empty(ViewerDatabaseProtectionText.Rows(read));
    }

    [Fact]
    public void ACaptureWithNoDatabase_SaysSo()
    {
        var read = Read();

        Assert.Equal("No databases in the newest capture.", ViewerDatabaseProtectionText.EmptyText(read));
        Assert.Empty(ViewerDatabaseProtectionText.Rows(read));
    }
}

/// <summary>
/// #6873 part 3b: the web read (<c>get_database_protection</c> through the web dispatch) and the Viewer's data method over one seeded store
/// give the same databases, ages and overdue words, and a server with no capture answers <c>unavailable</c> on both.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseProtectionSurfacesLiveTests
{
    private const string ServerName = "darling-database-protection-surfaces";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlConnection, CancellationToken, Task> seed, Func<NpgsqlDataSource, string, CancellationToken, Task<T>> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live database protection surface tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await seed(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, scratch.ConnectionString, ct);
    }

    private static Task InsertAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string name, string platform = "native", string recovery = "FULL",
        TimeSpan? checkDbAgo = null, string checkDbState = "checked", TimeSpan? fullAgo = null, string fullState = "recorded",
        TimeSpan? logAgo = null, string logState = "recorded")
    {
        DateTime? Ago(TimeSpan? span) => span is { } v ? DarlingMcpTestData.Naive(at - v) : null;
        return DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.database_protection
(collection_id, collection_time, server_id, server_name, database_name, state_desc, recovery_model_desc, create_date, replica_role, platform,
 last_good_checkdb_time, checkdb_state, last_full_backup_time, full_backup_state, last_log_backup_time, log_backup_state)
VALUES ($1,$2,$3,$4,$5,'ONLINE',$6,$7,NULL,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, name, recovery,
            DarlingMcpTestData.Naive(at - TimeSpan.FromDays(400)), platform, Ago(checkDbAgo), checkDbState, Ago(fullAgo), fullState, Ago(logAgo), logState);
    }

    private static async Task<JsonElement> WebReadAsync(NpgsqlDataSource postgres)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(new List<KeyValuePair<string, string?>> { new("server_name", ServerName) });
        return JsonDocument.Parse(await DarlingWebEndpoints.BuildReadDispatch()["get_database_protection"](context, postgres, null!)).RootElement;
    }

    private static DarlingServer Server() =>
        new(ServerId, ServerName, ServerName, true, 16, engineKind: MonitoredEngineKind.SqlServer, engineEdition: CollectorEngineCapability.UnknownEngineEdition);

    [Fact]
    public async Task ASeededStore_GivesTheSameDatabasesAndOverdueWords_OnTheWebReadAndInTheViewer()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            var at = DateTime.UtcNow.AddMinutes(-5);
            var day = TimeSpan.FromDays(1);
            await InsertAsync(connection, ct, at, "fine", checkDbAgo: 3 * day, fullAgo: day, logAgo: TimeSpan.FromMinutes(10));
            await InsertAsync(connection, ct, at, "old_full", checkDbAgo: 3 * day, fullAgo: 9 * day, logAgo: TimeSpan.FromMinutes(10));
            await InsertAsync(connection, ct, at, "azure", platform: "platform_managed", checkDbAgo: 40 * day, fullAgo: 30 * day, logAgo: 5 * day);
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadDatabaseProtectionAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal(ViewerDatabaseProtectionRead.Overdue, read.Status);
            var webOverdue = web.GetProperty("summary").GetProperty("overdue_full_backup").EnumerateArray().Select(v => v.GetString()!).ToArray();
            Assert.Equal(["old_full"], webOverdue);
            Assert.Equal(webOverdue, read.Summary!.OverdueFullBackup);
            Assert.Equal(web.GetProperty("summary").GetProperty("text").GetString(), read.Summary.Text);

            var rows = ViewerDatabaseProtectionText.Rows(read);
            Assert.Equal(["old_full", "azure", "fine"], rows.Select(r => r.Database).ToArray());
            Assert.Equal("9 days - overdue", rows[0].FullBackup);
            Assert.Equal(("30 days (platform managed)", "platform managed"), (rows[1].FullBackup, rows[1].Platform));
            var webAzure = web.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database").GetString() == "azure");
            Assert.Equal("platform_managed", webAzure.GetProperty("full_backup").GetProperty("state").GetString());
            Assert.Equal("30 days", webAzure.GetProperty("full_backup").GetProperty("age").GetString());
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_OnAPlainServerThatHasCollectedNothing_IsUnavailable_OnBothSurfaces()
    {
        await WithStoreAsync((_, _) => Task.CompletedTask, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadDatabaseProtectionAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("unavailable", web.GetProperty("status").GetString());
            Assert.Equal(ViewerDatabaseProtectionRead.Unavailable, read.Status);
            Assert.StartsWith("No database protection capture in the 24-hour window ending ", read.Message, StringComparison.Ordinal);
            Assert.Equal(read.Message, ViewerDatabaseProtectionText.StatusLine(read));
            return 0;
        });
    }

    [Fact]
    public async Task NoCapture_WhenTheCollectorWasRefusedItsGrant_IsAPreconditionNamingIt_OnBothSurfaces()
    {
        await WithStoreAsync(async (connection, ct) =>
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status, error_message)
VALUES ($1, $2, $3, 'database_protection', $4, 'PERMISSIONS', 'The SELECT permission was denied on the object backupset, database msdb, schema dbo.')",
                CollectionIdGenerator.Next(), ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-20)));
        }, async (postgres, cs, ct) =>
        {
            var web = await WebReadAsync(postgres);
            await using var viewer = new ViewerDataService(cs);
            var read = await viewer.ReadDatabaseProtectionAsync(Server(), DateTime.UtcNow, cancellationToken: ct);

            Assert.Equal("precondition", web.GetProperty("status").GetString());
            Assert.Equal(ViewerDatabaseProtectionRead.Precondition, read.Status);
            Assert.Contains("database_protection", read.Message, StringComparison.Ordinal);
            Assert.Contains("SELECT permission was denied on the object backupset", read.Message, StringComparison.Ordinal);
            Assert.Contains("SELECT permission was denied on the object backupset", web.GetProperty("message").GetString(), StringComparison.Ordinal);
            return 0;
        });
    }
}
