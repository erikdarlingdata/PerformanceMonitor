/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Security.AccessControl;
using System.Security.Principal;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #1581 field finding 1: the rolling file log must fail LOUDLY, not silently. On the field box the log
/// directory turned unwritable (an ACL artifact) and <see cref="DarlingFileLoggerProvider"/> went silent — the
/// flush catch swallowed every write, and nobody learned until they went to tail it. The provider now surfaces a
/// file-logging failure ONCE through an injectable sink (production: a best-effort Windows Event Log Warning),
/// latched so a persistently-broken log emits a single event, not one per 5s flush. These pins drive the latch
/// through the injected sink and the internal <see cref="DarlingFileLoggerProvider.Flush"/> seam — no real Event
/// Log needed.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingFileLoggerProviderTests : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "darling-filelog-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempRoot))
            {
                Directory.Delete(_tempRoot, recursive: true);
            }
            else if (File.Exists(_tempRoot))
            {
                File.Delete(_tempRoot);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    /// <summary>
    /// A healthy provider — a writable log directory, every flush succeeds — NEVER surfaces the failure
    /// fallback. The sink must stay untouched across construction and repeated flushes so a working box is never
    /// spuriously paged about its own logging.
    /// </summary>
    [Fact]
    public void HealthyProvider_NeverSurfacesFailure()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        for (var i = 0; i < 5; i++)
        {
            logger.LogInformation("healthy line {Index}", i);
            provider.Flush();
        }

        Assert.Empty(reports);
        /* Sanity: the flushes actually wrote — otherwise "never surfaced" would be a vacuous pass. */
        Assert.True(File.Exists(provider.CurrentLogFile()), "a healthy provider must have written its log file");
    }

    /// <summary>
    /// The log folder is limited to SYSTEM, Administrators and the service account wherever it is: a folder that
    /// already exists with an ordinary users' entry (one created beforehand, or one that inherits ProgramData's
    /// users' read) comes out with a protected DACL carrying none of the broad groups.
    /// </summary>
    [Fact]
    public void LogDirectory_IsRestrictedToTheServiceIdentities_WhateverItHeldBefore()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var logDir = Path.Combine(_tempRoot, "logs");
        var directory = Directory.CreateDirectory(logDir);
        var before = directory.GetAccessControl();
        before.AddAccessRule(new FileSystemAccessRule(
            new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null),
            FileSystemRights.ReadAndExecute,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        directory.SetAccessControl(before);

        var reports = new ConcurrentQueue<string>();
        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        provider.CreateLogger("Test").LogInformation("a line");
        provider.Flush();

        Assert.Empty(reports);
        var after = new DirectoryInfo(logDir).GetAccessControl();
        Assert.True(after.AreAccessRulesProtected, "the log folder must not inherit access from its parent");
        var broad = new[]
        {
            new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null),
            new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null),
            new SecurityIdentifier(WellKnownSidType.WorldSid, null),
            new SecurityIdentifier(WellKnownSidType.InteractiveSid, null),
        };
        foreach (FileSystemAccessRule rule in after.GetAccessRules(true, true, typeof(SecurityIdentifier)))
        {
            Assert.DoesNotContain((SecurityIdentifier)rule.IdentityReference, broad);
        }

        Assert.True(File.Exists(provider.CurrentLogFile()), "the provider still writes its log");
        Assert.Null(provider.StartupFailure);
    }

    private static void MakeJunction(string link, string target)
    {
        using var process = Process.Start(new ProcessStartInfo("cmd.exe", $"/c mklink /J \"{link}\" \"{target}\"")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
        });
        process!.StandardOutput.ReadToEnd();
        process.WaitForExit();
    }

    /// <summary>The log folder is a link, so locking it by path would change the folder it points to: file logging is
    /// turned off, the reason is kept for the host's console, and the folder behind the link is left as it was.</summary>
    [Fact]
    public void ALogDirectoryThatIsAJunction_TurnsFileLoggingOff_AndLeavesItsTargetAlone()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "junctions are Windows-only.");

        Directory.CreateDirectory(_tempRoot);
        var target = Path.Combine(_tempRoot, "elsewhere");
        Directory.CreateDirectory(target);
        var logDir = Path.Combine(_tempRoot, "logs");
        MakeJunction(logDir, target);

        var reports = new ConcurrentQueue<string>();
        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        provider.CreateLogger("Test").LogInformation("a line");
        provider.Flush();

        Assert.NotNull(provider.StartupFailure);
        Assert.Contains("junction or symbolic link", provider.StartupFailure, StringComparison.Ordinal);
        Assert.Single(reports);
        Assert.Empty(Directory.GetFiles(target));
        Assert.False(new DirectoryInfo(target).GetAccessControl().AreAccessRulesProtected, "the folder behind the link keeps its ACL");
    }

    /// <summary>The folder above the log folder is the link: the lock by path would follow it just the same.</summary>
    [Fact]
    public void ALogDirectoryBelowAJunction_TurnsFileLoggingOff()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "junctions are Windows-only.");

        Directory.CreateDirectory(_tempRoot);
        var target = Path.Combine(_tempRoot, "elsewhere");
        Directory.CreateDirectory(target);
        var parent = Path.Combine(_tempRoot, "parent");
        MakeJunction(parent, target);

        var reports = new ConcurrentQueue<string>();
        using var provider = new DarlingFileLoggerProvider(Path.Combine(parent, "logs"), reports.Enqueue);

        Assert.NotNull(provider.StartupFailure);
        Assert.Contains("junction or symbolic link", provider.StartupFailure, StringComparison.Ordinal);
        Assert.Single(reports);
    }

    /// <summary>A log folder some other user made, and so owns, is not locked and written to: its owner could give
    /// itself access again whenever it liked.</summary>
    [Fact]
    public void ALogDirectoryAnOrdinaryUserOwns_TurnsFileLoggingOff()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var logDir = Path.Combine(_tempRoot, "logs");
        var directory = Directory.CreateDirectory(logDir);
        var me = WindowsIdentity.GetCurrent().User!;
        try
        {
            var security = directory.GetAccessControl();
            security.SetOwner(me);
            directory.SetAccessControl(security);
        }
        catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
        {
            Assert.Skip("this account may not make itself the owner: " + ex.Message);
        }

        var reports = new ConcurrentQueue<string>();
        try
        {
            /* The service runs as another account, and this folder belongs to neither it nor the service's own. */
            DarlingFileSecurity.UseStoreTrustForTests(
                runningAccount: new SecurityIdentifier(WellKnownSidType.NetworkServiceSid, null), ownServiceSid: null);
            using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);

            Assert.NotNull(provider.StartupFailure);
            Assert.Contains("is not owned by", provider.StartupFailure, StringComparison.Ordinal);
            Assert.Single(reports);
            Assert.False(new DirectoryInfo(logDir).GetAccessControl().AreAccessRulesProtected, "the folder is not locked");
        }
        finally
        {
            DarlingFileSecurity.UseStoreTrustForTests(null, null);
        }
    }

    /// <summary>The same folder, owned by the service's own account (which still owns it after the Log On account was
    /// changed), is locked and written to.</summary>
    [Fact]
    public void ALogDirectoryTheServicesOwnAccountOwns_IsStillLogged()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var logDir = Path.Combine(_tempRoot, "logs");
        var directory = Directory.CreateDirectory(logDir);
        var me = WindowsIdentity.GetCurrent().User!;
        try
        {
            var security = directory.GetAccessControl();
            security.SetOwner(me);
            directory.SetAccessControl(security);
        }
        catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
        {
            Assert.Skip("this account may not make itself the owner: " + ex.Message);
        }

        var reports = new ConcurrentQueue<string>();
        try
        {
            DarlingFileSecurity.UseStoreTrustForTests(
                runningAccount: new SecurityIdentifier(WellKnownSidType.NetworkServiceSid, null), ownServiceSid: me);
            using (var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue))
            {
                Assert.Null(provider.StartupFailure);
                Assert.Empty(reports);
            }
        }
        finally
        {
            DarlingFileSecurity.UseStoreTrustForTests(null, null);

            /* The lock above left this account out of the folder; it owns it, so it can take its access back to clean up. */
            var restore = directory.GetAccessControl();
            restore.AddAccessRule(new FileSystemAccessRule(me, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
            directory.SetAccessControl(restore);
        }
    }

    /// <summary>The file-logging-off reason reaches the host's console logger once it exists: the Event Log copy
    /// goes nowhere on a hand install with no registered event source, and the file provider drops everything while
    /// it is off.</summary>
    [Fact]
    public void TheHost_WritesTheFileLoggingOffReasonToItsLoggers_RightAfterItIsBuilt()
    {
        var program = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        var build = program.IndexOf("var host = builder.Build();", StringComparison.Ordinal);
        var report = program.IndexOf("fileLoggerProvider.StartupFailure", StringComparison.Ordinal);
        var run = program.IndexOf("host.Run();", StringComparison.Ordinal);

        Assert.True(build >= 0 && report > build && report < run, "the reason is logged after the host is built and before it runs");
        Assert.Contains("LogWarning(\"{Warning}\", fileLogFailure)", program, StringComparison.Ordinal);
    }

    /// <summary>
    /// A constructor that cannot create/enable the log directory surfaces the failure EXACTLY once, and the
    /// disabled provider never surfaces it again on later flushes. The directory is unmakeable because its parent
    /// is a FILE — a deterministic, cross-platform <see cref="Directory.CreateDirectory(string)"/> failure.
    /// </summary>
    [Fact]
    public void ConstructorDirectoryFailure_SurfacesOnce()
    {
        var reports = new ConcurrentQueue<string>();
        Directory.CreateDirectory(_tempRoot);
        var blocker = Path.Combine(_tempRoot, "blocker");
        File.WriteAllText(blocker, "not a directory");
        var unmakeableDir = Path.Combine(blocker, "logs");   /* parent is a file → CreateDirectory throws */

        using var provider = new DarlingFileLoggerProvider(unmakeableDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        /* A disabled provider drops enqueues and short-circuits flush, so later flushes must not add a second
           report. */
        for (var i = 0; i < 3; i++)
        {
            logger.LogInformation("line {Index}", i);
            provider.Flush();
        }

        Assert.Single(reports);
        Assert.True(reports.TryPeek(out var message), "the constructor failure must have surfaced a message");
        Assert.Contains("File logging is disabled", message);
    }

    /// <summary>
    /// The core latch pin: a provider that constructed healthy but whose directory then turned unwritable (the
    /// field's mid-run ACL artifact) surfaces the failure ONCE across MANY failing flushes — not one event per 5s
    /// flush. Each iteration enqueues a fresh line so every flush genuinely attempts (and fails) a write; the
    /// latch, not an emptied buffer, is what bounds the report to one.
    /// </summary>
    [Fact]
    public void RepeatedFlushFailures_SurfaceAtMostOnce()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        /* Break the directory the provider enabled against: replace it with a FILE so every subsequent
           File.AppendAllText(CurrentLogFile()) throws (its parent is no longer a directory). */
        Directory.Delete(logDir, recursive: true);
        File.WriteAllText(logDir, "now a file");

        for (var i = 0; i < 6; i++)
        {
            logger.LogInformation("failing line {Index}", i);
            provider.Flush();
        }

        Assert.Single(reports);
    }

    /* ---------------- #4281 review, finding 1 (Medium): a lone surrogate must not cost the batch ---------------- */

    /// <summary>
    /// File.AppendAllText's default UTF-8 encoder throws EncoderFallbackException on a lone surrogate and
    /// writes ZERO bytes -- so one malformed line used to cost the WHOLE 5-second batch, including every
    /// other line queued in the same flush. The surrogate is injected directly here, bypassing
    /// DarlingHttpRefusalLog.Sanitize entirely, to isolate this fix (Flush's encoding) from that one: Flush
    /// itself must never throw or drop a batch, whatever put the surrogate there.
    /// </summary>
    [Fact]
    public void Flush_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        logger.LogInformation("bad line {Tail}", new string('a', 255) + '\uD83D');
        logger.LogInformation("the other line in the same batch");
        provider.Flush();

        Assert.Empty(reports);
        var written = File.ReadAllText(provider.CurrentLogFile());
        Assert.Contains("the other line in the same batch", written, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4281 review, finding 5 and its #4286 follow-up (Low 5): exception.Message can repeat request text (a
    /// PostgreSQL cast error echoes the bad value; KeyNotFoundException echoes the key) and reached the file
    /// log unsanitized -- CR/LF in it could forge a second entry the same way an unsanitized route could.
    /// The fix now cleans the WHOLE assembled line at the sink (not just exception.Message), and a CRLF
    /// collapses to ONE kept line feed rather than two dots -- so the forged-looking text survives readably,
    /// but INDENTED, which is what actually defeats the forgery: it can never start at column 0, the one
    /// place a reader looks for the next entry's timestamp. Same input the pre-#4286 version of this test
    /// used, new expected shape.
    /// </summary>
    [Fact]
    public void Log_ExceptionMessageCarriesCrLf_StaysOneEntry_ContinuationNeverAtColumnZero()
    {
        var logDir = Path.Combine(_tempRoot, "logs");
        using var provider = new DarlingFileLoggerProvider(logDir, _ => { });
        var logger = provider.CreateLogger("Test");

        var ex = new InvalidOperationException("bad value\r\n2026-08-21 12:00:00 WARN  Forged line");
        logger.LogError(ex, "operation failed");
        provider.Flush();

        var written = File.ReadAllText(provider.CurrentLogFile());

        /* The CR is gone (collapsed into the LF it preceded), and the fake timestamp is now indented --
           never a bare "\n2026-08-21" or "\r\n2026-08-21" at the start of a line. */
        Assert.DoesNotContain("\r\n2026-08-21", written, StringComparison.Ordinal);
        Assert.DoesNotContain("\n2026-08-21", written, StringComparison.Ordinal);
        Assert.Contains("bad value\n    2026-08-21 12:00:00 WARN  Forged line", written, StringComparison.Ordinal);

        /* "Stays one entry": only the real entry's own timestamp sits at the true start of a line. */
        var realEntryStarts = System.Text.RegularExpressions.Regex.Matches(
            written, @"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", System.Text.RegularExpressions.RegexOptions.Multiline);
        Assert.Single(realEntryStarts);
    }

    /// <summary>
    /// #4286 review, Low 4 applied at the sink: this used to check only ASCII control characters -- the C1
    /// range (here NEL/U+0085) and the Unicode line separators (here U+2028) passed through into the file
    /// unsanitized, the same gap DarlingHttpRefusalLog.Sanitize and DarlingWebOidc's subject check had.
    /// </summary>
    [Fact]
    public void Log_MessageCarriesC1ControlOrUnicodeLineSeparator_MapsToADot()
    {
        var logDir = Path.Combine(_tempRoot, "logs");
        using var provider = new DarlingFileLoggerProvider(logDir, _ => { });
        var logger = provider.CreateLogger("Test");

        logger.LogInformation("before{Sep1}middle{Sep2}end", ((char)0x0085).ToString(), ((char)0x2028).ToString());
        provider.Flush();

        var written = File.ReadAllText(provider.CurrentLogFile());
        Assert.Contains("before.middle.end", written, StringComparison.Ordinal);
        Assert.DoesNotContain('\u0085', written);
        Assert.DoesNotContain((char)0x2028, written);
    }

    /// <summary>
    /// #4286 review, Low 5: the whole-line clean must not mangle DarlingWorker's own "Store host profile"
    /// line -- <c>_logger.LogInformation("Store host profile:\n{Profile}", FormatStartupProfileText(...))</c>,
    /// logged once at every start (DarlingWorker.cs, #4214 ruling 9) -- which embeds '\n' on purpose to print
    /// a readable multi-line block. Same call shape and a real HostProfile
    /// (<see cref="StartupHostProfileLogTests"/>'s fixture, duplicated here since it is private there): every
    /// row must survive on its own line, indented, not collapsed into the timestamp line or dotted out.
    /// </summary>
    [Fact]
    public void Log_StoreHostProfileBlock_StaysReadable_EachRowOnItsOwnIndentedLine()
    {
        var profile = new HostProfile
        {
            Platform = "linux",
            IsContainerized = true,
            ProcessorCount = 4,
            Memory = new HostMemoryProfile(8_589_934_592, 4_294_967_296, 4_294_967_296, true, "cgroup v2 memory.max"),
            DataVolume = new HostDataVolumeProfile(107_374_182_400, 53_687_091_200, "ext4", true),
            IsManagedStore = true,
            Store = new HostStoreFacts("17.4", "2.99.0", 999_999_999_999, 42.0, 123, 999_999_999_999, 4_242),
            Settings =
            [
                new HostSettingProfile("shared_buffers", "2048MB", 2048, "v8 (#4214 managed block)", "2048MB", 2048, HostSettingVerdict.Matches),
            ],
            Cloud = CloudIdentity.None,
        };

        var logDir = Path.Combine(_tempRoot, "logs");
        using var provider = new DarlingFileLoggerProvider(logDir, _ => { });
        var logger = provider.CreateLogger("Test");

        logger.LogInformation("Store host profile:\n{Profile}", DarlingStoreHostProfile.FormatStartupProfileText(profile));
        provider.Flush();

        var written = File.ReadAllText(provider.CurrentLogFile());

        Assert.Contains("Store host profile:\n    Host: linux (containerized), 4 CPU(s)", written, StringComparison.Ordinal);
        Assert.Contains("\n    RAM: 4 GB", written, StringComparison.Ordinal);
        Assert.Contains("\n    Data volume: 100 GB total, 50 GB free (ext4)", written, StringComparison.Ordinal);
        Assert.Contains("\n    shared_buffers", written, StringComparison.Ordinal);
    }

    /* ---------------- #1652 gap 3: the RECURRING retention sweep ---------------- */

    /// <summary>
    /// The sweep the worker's daily maintenance tick calls. It used to run only from the constructor, so a
    /// service up for months — which is the normal case for a service — swept once at startup and never again
    /// while writing a file a day. Static and directory-taking so the tick needs no reference to the provider
    /// the host owns.
    /// </summary>
    [Fact]
    public void SweepOldFiles_DeletesPastRetention_KeepsRecentAndForeignFiles()
    {
        var logDir = Path.Combine(_tempRoot, "logs");
        Directory.CreateDirectory(logDir);

        var expired = WriteAged(logDir, "darling-service_20240101.log", ageDays: 30);
        var justOutside = WriteAged(logDir, "darling-service_20240102.log", ageDays: 15);
        var justInside = WriteAged(logDir, "darling-service_20240103.log", ageDays: 13);
        var foreign = WriteAged(logDir, "pg.log", ageDays: 30);

        DarlingFileLoggerProvider.SweepOldFiles(logDir);

        Assert.False(File.Exists(expired), "a 30-day-old service log survived the sweep");
        Assert.False(File.Exists(justOutside), "a 15-day-old log survived the 14-day window");
        Assert.True(File.Exists(justInside), "a 13-day-old log was deleted inside the 14-day window");
        Assert.True(File.Exists(foreign), "the sweep deleted a file that is not a service log");
    }

    /// <summary>
    /// Running on a maintenance tick means it runs forever, so it must never throw — a missing directory (the
    /// provider failed to create it, or an operator removed it mid-run) must not take down the tick that also
    /// carries the store's retention purge.
    /// </summary>
    [Fact]
    public void SweepOldFiles_MissingDirectory_DoesNotThrow() =>
        DarlingFileLoggerProvider.SweepOldFiles(Path.Combine(_tempRoot, "never-created"));

    /// <summary>14 days, the window an operator needs to reconstruct an incident they noticed last week.</summary>
    [Fact]
    public void RetentionWindow_IsFourteenDays() =>
        Assert.Equal(14, DarlingFileLoggerProvider.RetentionDays);

    /// <summary>
    /// Writes a file and backdates it. The sweep compares <see cref="File.GetLastWriteTime(string)"/>, so that
    /// is the stamp the fixture has to move.
    /// </summary>
    private static string WriteAged(string directory, string name, int ageDays)
    {
        var path = Path.Combine(directory, name);
        File.WriteAllText(path, "line");
        File.SetLastWriteTime(path, DateTime.Now.AddDays(-ageDays));
        return path;
    }
}
