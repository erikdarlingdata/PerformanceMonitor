/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6873 part 3b: the Viewer's Database Protection words as functions of the read. They are the words the web page's Health Checks panels
/// show, so the assertions are on the values: the age text, the state words, the platform label, the order and the sentences of a read
/// with no capture. The judgement under them is <see cref="DatabaseProtectionBands"/>.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerDatabaseProtectionTextTests : IDisposable
{
    /* #7343: the capture and scan times follow the display mode; these assertions are on the machine-local words, so the class runs in Local mode. */
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public ViewerDatabaseProtectionTextTests() => ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    public static TheoryData<TimeDisplayMode> Modes => new() { TimeDisplayMode.ServerTime, TimeDisplayMode.UTC, TimeDisplayMode.LocalTime };

    /// <summary>The time of <paramref name="utc"/> as each display mode words it on the page's clock (#8576), to the second with its zone: UTC in
    /// Server and UTC modes, as the refresh stamp beside it reads, and this machine's time with this machine's zone name in Local mode.</summary>
    private static string Worded(DateTime utc, TimeDisplayMode mode) => mode switch
    {
        TimeDisplayMode.ServerTime or TimeDisplayMode.UTC => utc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture) + " UTC",
        _ => LocalWorded(utc),
    };

    private static string LocalWorded(DateTime utc) =>
        TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.CurrentCulture)
        + " " + (TimeZoneInfo.Local.IsDaylightSavingTime(utc) ? TimeZoneInfo.Local.DaylightName : TimeZoneInfo.Local.StandardName);

    [Theory]
    [MemberData(nameof(Modes))]
    public void TheStatusLineCaptureTime_FollowsTheDisplayMode_OnTheRefreshStampsClock(TimeDisplayMode mode)
    {
        ViewerTimeHelper.CurrentDisplayMode = mode;
        var read = Read(Fact("a", fullAgo: Day, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: Day));

        Assert.EndsWith("Captured: " + Worded(Captured, mode), ViewerDatabaseProtectionText.StatusLine(read), StringComparison.Ordinal);
    }

    /// <summary>The capture time is stamped to the second and names its zone, as every other grid's time does, so a reader never has to
    /// guess which clock a culture short date was in.</summary>
    [Fact]
    public void TheStatusLineCaptureTime_IsStampedToTheSecond_AndNamesItsZone()
    {
        var read = Read(Fact("a", fullAgo: Day, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: Day));

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.EndsWith("Captured: 2026-10-28 12:00:00 UTC", ViewerDatabaseProtectionText.StatusLine(read), StringComparison.Ordinal);

        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        /* #8576: Server Time has no server clock to apply to this page's stamps, so it reads UTC as the refresh stamp does. */
        Assert.EndsWith("Captured: 2026-10-28 12:00:00 UTC", ViewerDatabaseProtectionText.StatusLine(read), StringComparison.Ordinal);
    }


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
            new ViewerDatabaseProtectionRow("zeta", "ONLINE", "9 days 2 hours - overdue", "1 hour 30 minutes - overdue", "20 days - overdue", "", "", true, "Boot page"),
            rows[0]);
        Assert.Equal(new ViewerDatabaseProtectionRow("alpha", "ONLINE", "1 day", "10 minutes", "3 days", "", "", false, "Boot page"), rows[1]);
    }

    [Fact]
    public void AModelWithNoLogBackup_ReadsNotExpected_WithItsSentence_AndIsNotOverdue()
    {
        var read = Read(Fact("model", fullAgo: Day, checkDbAgo: Day, logState: "none_in_window"), Fact("Sales", fullAgo: Day, checkDbAgo: Day, logState: "none_in_window"));
        var rows = ViewerDatabaseProtectionText.Rows(read);

        var model = rows.Single(r => r.Database == "model");
        Assert.Equal(("not expected", false, DatabaseProtectionBands.DetailSystemDatabase), (model.LogBackup, model.Overdue, model.LogBackupNote));
        var sales = rows.Single(r => r.Database == "Sales");
        Assert.Equal(("none in window - overdue", true, (string?)null), (sales.LogBackup, sales.Overdue, sales.LogBackupNote));
    }

    [Fact]
    public void NeverAndNoneInWindow_ReadTheirWord_AndOverdue()
    {
        var row = ViewerDatabaseProtectionText.Rows(Read(Fact("lost", checkDbState: "never", fullState: "none_in_window", logState: "none_in_window")))[0];

        Assert.Equal(("none in window - overdue", "none in window - overdue", "never - overdue"), (row.FullBackup, row.LogBackup, row.CheckDb));
    }

    [Fact]
    public void ThePlatformColumn_ShowsOnlyWhenSomeRowNamesAPlatform()
    {
        var boxed = ViewerDatabaseProtectionText.Rows(Read(Fact("a", fullAgo: Day), Fact("b", fullAgo: Day)));
        Assert.False(ViewerDatabaseProtectionText.ShowsPlatformColumn(boxed));

        var mixed = ViewerDatabaseProtectionText.Rows(Read(Fact("a", fullAgo: Day), Fact("b", platform: "platform_managed", fullAgo: Day)));
        Assert.True(ViewerDatabaseProtectionText.ShowsPlatformColumn(mixed));
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
        var local = LocalWorded(Captured);

        Assert.Equal($"1 of 1 judged databases overdue. Full backup: zeta.   Captured: {local}", ViewerDatabaseProtectionText.StatusLine(read));
        Assert.StartsWith("No database is overdue (1 judged).", ViewerDatabaseProtectionText.StatusLine(Read(Fact("a", fullAgo: Day, logAgo: TimeSpan.FromMinutes(5), checkDbAgo: Day))), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("not_collected", "Azure SQL Database does not have this data.")]
    [InlineData("precondition", "The monitoring login was refused SELECT on msdb.dbo.backupset.")]
    [InlineData("unavailable", "No database protection capture in the 24-hour window.")]
    public void ARead_WithoutACapture_ShowsTheReadsOwnSentenceOnce_AndTheGridSaysNoData(string status, string sentence)
    {
        var read = new ViewerDatabaseProtectionRead(status, sentence, null, [], null);

        Assert.Equal(sentence, ViewerDatabaseProtectionText.StatusLine(read));
        /* #6907: the status line carries the sentence, so the grid note must not repeat it. */
        Assert.Equal(ViewerDatabaseProtectionText.NoData, ViewerDatabaseProtectionText.EmptyText(read));
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
/// #8865: the surfaces of a CHECKDB judged per replica. The MCP tool's projections (the CHECKDB fact and the server-level error_log block) are
/// serialised exactly as the tool does, the web panel's page script runs under Node on that reply, and the Viewer's words are asked for the same facts.
/// </summary>
public sealed class DatabaseProtectionPerReplicaSurfacesTests
{
    private static readonly DateTime Now = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DatabaseProtectionThresholds Limits = DatabaseProtectionThresholds.Default;

    private static DatabaseProtectionFact Base(string name, string? role = null) => new(
        name, "ONLINE", "FULL", Now.AddDays(-400), role, "native",
        Now.AddDays(-3), "checked", Now.AddDays(-1), "recorded", Now.AddMinutes(-10), "recorded");

    private static DatabaseProtectionFact Logged(string name, string? role, TimeSpan? checkAgo, int? errors = null, string? options = "NO_INFOMSGS") =>
        Base(name, role) with
        {
            ErrorLogState = "read", ErrorLogBackfillComplete = true,
            ErrorLogCheckDbTime = checkAgo is { } a ? Now - a : null, ErrorLogCheckDbOptions = options, ErrorLogCheckDbErrors = errors,
        };

    /// <summary>The tool's reply for these facts, built from the tool's own projection helpers and serialised with its options.</summary>
    private static JsonElement ToolReply(params DatabaseProtectionFact[] facts)
    {
        var banded = facts.Select(f => (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Now, Limits))).ToList();
        var reply = new
        {
            error_log = DarlingMcpDatabaseProtectionTools.ErrorLogBlock(facts.FirstOrDefault()),
            databases = banded.Select(b => DarlingMcpDatabaseProtectionTools.DatabaseEntry(b.Fact, b.Band)),
        };
        return JsonDocument.Parse(JsonSerializer.Serialize(reply, McpHelpers.JsonOptions)).RootElement.Clone();
    }

    private static JsonElement Db(JsonElement reply, string name) =>
        reply.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database").GetString() == name);

    private static string? Str(JsonElement e, string name) =>
        e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    [Fact]
    public void TheToolsLastTime_IsTheTimeTheBandJudged_AndTheBootPageStaysSeparate()
    {
        /* #8865 I7: last_time was always the boot page, so an error-log judgement showed an age and a state the time beside it did not explain. */
        static string Iso(DateTime t) => t.ToString("o", CultureInfo.InvariantCulture);
        var reply = ToolReply(
            Logged("AgDb", "SECONDARY", TimeSpan.FromDays(2)) with { LastGoodCheckDbTime = Now.AddDays(-30) },
            Logged("LogNewer", null, TimeSpan.FromDays(1)) with { LastGoodCheckDbTime = Now.AddDays(-9) },
            Logged("BootNewer", null, TimeSpan.FromDays(9)) with { LastGoodCheckDbTime = Now.AddDays(-1) });

        var ag = Db(reply, "AgDb").GetProperty("checkdb");
        Assert.Equal(Iso(Now.AddDays(-2)), Str(ag, "last_time"));
        Assert.Equal(Iso(Now.AddDays(-30)), Str(ag, "boot_page_time"));
        /* #8865 M8: the tool description promises detail secondary on a secondary's backup facts. */
        Assert.Equal(("excluded", "secondary"), (Str(Db(reply, "AgDb").GetProperty("full_backup"), "state"), Str(Db(reply, "AgDb").GetProperty("full_backup"), "detail")));

        var logNewer = Db(reply, "LogNewer").GetProperty("checkdb");
        Assert.Equal(("error_log", Iso(Now.AddDays(-1))), (Str(logNewer, "source"), Str(logNewer, "last_time")));

        var bootNewer = Db(reply, "BootNewer").GetProperty("checkdb");
        Assert.Equal(("boot_page", Iso(Now.AddDays(-1))), (Str(bootNewer, "source"), Str(bootNewer, "last_time")));
    }

    [Fact]
    public void TheTool_ShowsSourceOptionsAndErrors_AndTheErrorLogBlock()
    {
        /* An AG secondary judged on its own log; the boot page is kept for reference. */
        var secondary = Logged("Sales", "SECONDARY", TimeSpan.FromDays(2)) with { LastGoodCheckDbTime = Now.AddDays(-30), ReplicaReadable = true };
        var primary = Logged("Hr", "PRIMARY", TimeSpan.FromDays(1), errors: 3, options: "PHYSICAL_ONLY");
        var read = ToolReply(secondary, primary);

        var s = Db(read, "Sales");
        var sc = s.GetProperty("checkdb");
        Assert.Equal(("ok", "error_log", "NO_INFOMSGS"), (sc.GetProperty("state").GetString(), sc.GetProperty("source").GetString(), sc.GetProperty("options").GetString()));
        Assert.Equal(Now.AddDays(-30).ToString("o", CultureInfo.InvariantCulture), sc.GetProperty("boot_page_time").GetString());
        Assert.True(s.GetProperty("replica_readable").GetBoolean());
        Assert.Null(Str(s, "excluded_reason"));

        var p = Db(read, "Hr").GetProperty("checkdb");
        Assert.Equal(("errors_found", true, 3, "PHYSICAL_ONLY"), (p.GetProperty("state").GetString(), p.GetProperty("overdue").GetBoolean(), p.GetProperty("errors").GetInt32(), p.GetProperty("options").GetString()));

        var log = read.GetProperty("error_log");
        Assert.Equal(("read", "complete"), (log.GetProperty("status").GetString(), log.GetProperty("backfill").GetString()));

        /* R11: a capture from before the error-log read shows the boot page as its source, and no boot-page reference. */
        var old = ToolReply(Base("Old"));
        var oc = Db(old, "Old").GetProperty("checkdb");
        Assert.Equal("boot_page", oc.GetProperty("source").GetString());
        Assert.Null(Str(oc, "boot_page_time"));
        Assert.Equal("not_applicable", old.GetProperty("error_log").GetProperty("status").GetString());

        /* A refused read reports precondition and names the grant; the CHECKDB falls back to the boot page. */
        var refused = ToolReply(Base("Sales", "PRIMARY") with { ErrorLogState = "refused", ErrorLogMessage = "The VIEW SERVER STATE permission was denied.", ErrorLogGrant = "VIEW SERVER STATE" });
        var rl = refused.GetProperty("error_log");
        Assert.Equal(("precondition", "VIEW SERVER STATE"), (rl.GetProperty("status").GetString(), rl.GetProperty("grant").GetString()));
        Assert.Contains("denied", rl.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("boot_page", Db(refused, "Sales").GetProperty("checkdb").GetProperty("source").GetString());

        /* A failed read with skipped archives and unparsed lines says so, and a backfill still running is in progress. */
        var failed = ToolReply(Logged("Sales", "PRIMARY", TimeSpan.FromDays(1)) with
        {
            ErrorLogState = "failed", ErrorLogMessage = "timeout", ErrorLogBackfillComplete = false, ErrorLogSkippedArchives = "3, 4", ErrorLogUnparsedLines = 7,
        });
        var fl = failed.GetProperty("error_log");
        Assert.Equal(("failed", "in_progress", "3, 4", 7), (fl.GetProperty("status").GetString(), fl.GetProperty("backfill").GetString(), fl.GetProperty("skipped_archives").GetString(), fl.GetProperty("unparsed_lines").GetInt32()));

        /* pending: the sentence travels with the state. */
        var pending = ToolReply(Base("Sales", "SECONDARY") with { ErrorLogState = "read", ErrorLogBackfillComplete = false, ReplicaReadable = false });
        var pc = Db(pending, "Sales").GetProperty("checkdb");
        Assert.Equal(("pending", "error_log"), (pc.GetProperty("state").GetString(), pc.GetProperty("source").GetString()));
        Assert.Contains("still being read", pc.GetProperty("detail").GetString(), StringComparison.Ordinal);
        Assert.False(Db(pending, "Sales").GetProperty("replica_readable").GetBoolean());
    }

    [Fact]
    public void TheDescription_NoLongerCallsASecondaryExcludedForCheckDb_AndNamesPendingAndErrorsFound()
    {
        var text = typeof(DarlingMcpDatabaseProtectionTools).GetMethod(nameof(DarlingMcpDatabaseProtectionTools.GetDatabaseProtection))!
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.DoesNotContain("a secondary or otherwise", text, StringComparison.Ordinal);
        Assert.Contains("pending", text, StringComparison.Ordinal);
        Assert.Contains("errors_found", text, StringComparison.Ordinal);
        Assert.Contains("error_log", text, StringComparison.Ordinal);
        Assert.Contains("boot_page", text, StringComparison.Ordinal);
        Assert.Contains("backup facts also read excluded", text, StringComparison.Ordinal);
    }

    private static string[][] Grid(JsonElement web, string[] headers)
    {
        var cells = web.GetProperty("cells").EnumerateArray().Select(c => c.GetString()!).ToArray();
        return cells.Chunk(headers.Length).ToArray();
    }

    /// <summary>The same facts through the page script (under Node) and through the Viewer's text: the CHECKDB cell, the source, the options and the error log
    /// sentence read the same, for an AG secondary, an errors_found primary, a pending replica and a boot-page database.</summary>
    [Fact]
    public void TheSurfacesSayTheSameWords()
    {
        var facts = new[]
        {
            Logged("Sales", "SECONDARY", TimeSpan.FromDays(2)) with { LastGoodCheckDbTime = Now.AddDays(-30), ErrorLogState = "failed", ErrorLogMessage = "timeout", ErrorLogSkippedArchives = "3", ErrorLogUnparsedLines = 2 },
            Logged("Hr", "PRIMARY", TimeSpan.FromDays(1), errors: 3, options: "PHYSICAL_ONLY"),
            Base("Wait", "SECONDARY") with { ErrorLogState = "read", ErrorLogBackfillComplete = false, ReplicaReadable = false },
            Base("Plain"),
        };
        var reply = ToolReply(facts);
        var file = Path.Combine(Path.GetTempPath(), "darling-8865-" + Guid.NewGuid().ToString("N") + ".json");
        JsonElement web;
        try
        {
            File.WriteAllText(file, reply.GetRawText());
            web = WebTileTruthsTests.Run(file).GetProperty("protectionRead");
        }
        finally
        {
            File.Delete(file);
        }

        var headers = web.GetProperty("headers").EnumerateArray().Select(h => h.GetString()!).ToArray();
        var rows = Grid(web, headers).ToDictionary(r => r[0]);
        int At(string header) => Array.IndexOf(headers, header);

        var banded = facts.Select(f => (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Now, Limits))).ToList();
        var viewer = ViewerDatabaseProtectionText.Rows(new ViewerDatabaseProtectionRead(
            ViewerDatabaseProtectionRead.Overdue, null, Now, banded, DatabaseProtectionBands.Summarize(banded))).ToDictionary(r => r.Database);

        Assert.Equal(4, rows.Count);
        foreach (var (name, v) in viewer)
        {
            Assert.Equal(v.CheckDb, rows[name][At("Last good CHECKDB")]);
            Assert.Equal(v.CheckDbSource, rows[name][At("CHECKDB source")]);
            if (v.CheckDbOptions.Length > 0)
            {
                Assert.Equal(v.CheckDbOptions, rows[name][At("CHECKDB options")]);
            }
        }

        Assert.Equal("2 days (ok)".Replace(" (ok)", ""), viewer["Sales"].CheckDb);
        Assert.Equal("1 day (errors found, 3 errors) - overdue", viewer["Hr"].CheckDb);
        Assert.Equal("pending", viewer["Wait"].CheckDb);
        Assert.Equal(("Error log", "Error log", "Error log", "Boot page"), (viewer["Sales"].CheckDbSource, viewer["Hr"].CheckDbSource, viewer["Wait"].CheckDbSource, viewer["Plain"].CheckDbSource));
        Assert.Equal("PHYSICAL_ONLY", viewer["Hr"].CheckDbOptions);

        /* The tooltips: the pending sentence is the same string the Viewer's cell carries. */
        var tooltips = web.GetProperty("tooltips").EnumerateArray().Select(t => t.GetString()!).ToArray();
        Assert.Contains(viewer["Wait"].CheckDbNote, tooltips);
        Assert.Contains("still being read", viewer["Wait"].CheckDbNote, StringComparison.Ordinal);
        /* #8865 M2: a cell with nothing to say has no tooltip: null, because WPF draws an empty box for "". */
        Assert.Null(viewer["Plain"].CheckDbNote);

        /* The server-level sentence: failed with the message, a skipped archive and unparsed lines. */
        var viewerNote = ViewerDatabaseProtectionText.ErrorLogNote(new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.Overdue, null, Now, banded, DatabaseProtectionBands.Summarize(banded)));
        Assert.Equal("failed: timeout; skipped archives 3; 2 unparsed lines", viewerNote);
        Assert.Equal(viewerNote, web.GetProperty("errorLog").GetString());

        /* R12: a healthy read and an old capture have no tile on the web and no note on the Viewer. */
        var healthy = ToolReply(Logged("Sales", "PRIMARY", TimeSpan.FromDays(1)));
        var healthyFile = Path.Combine(Path.GetTempPath(), "darling-8865-" + Guid.NewGuid().ToString("N") + ".json");
        try
        {
            File.WriteAllText(healthyFile, healthy.GetRawText());
            Assert.Equal(JsonValueKind.Null, WebTileTruthsTests.Run(healthyFile).GetProperty("protectionRead").GetProperty("errorLog").ValueKind);
        }
        finally
        {
            File.Delete(healthyFile);
        }
    }

    [Fact]
    public void TheErrorLogNote_IsPresentForASkippedArchiveAndForUnparsedLines_EvenOnAHealthyRead()
    {
        static string? Note(DatabaseProtectionFact f)
        {
            var banded = new[] { (Fact: f, Band: DatabaseProtectionBands.Evaluate(f, Now, Limits)) }.ToList();
            return ViewerDatabaseProtectionText.ErrorLogNote(new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.Ok, null, Now, banded, DatabaseProtectionBands.Summarize(banded)));
        }

        var healthy = Logged("a", "PRIMARY", TimeSpan.FromDays(1));
        Assert.Null(Note(healthy));
        Assert.Equal("skipped archives 2", Note(healthy with { ErrorLogSkippedArchives = "2" }));
        Assert.Equal("1 unparsed line", Note(healthy with { ErrorLogUnparsedLines = 1 }));
        Assert.Equal("failed: boom", Note(healthy with { ErrorLogState = "failed", ErrorLogMessage = "boom" }));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("not_applicable", null)]
    [InlineData("read", null)]
    [InlineData("refused", "refused, grant VIEW SERVER STATE")]
    public void TheErrorLogNote_IsAbsentWhenThereIsNothingToActOn_AndNamesTheGrantWhenRefused(string? state, string? expected)
    {
        var fact = Base("a") with { ErrorLogState = state, ErrorLogGrant = state == "refused" ? "VIEW SERVER STATE" : null };
        var banded = new[] { (Fact: fact, Band: DatabaseProtectionBands.Evaluate(fact, Now, Limits)) }.ToList();
        var read = new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.Ok, null, Now, banded, DatabaseProtectionBands.Summarize(banded));

        Assert.Equal(expected, ViewerDatabaseProtectionText.ErrorLogNote(read));
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
