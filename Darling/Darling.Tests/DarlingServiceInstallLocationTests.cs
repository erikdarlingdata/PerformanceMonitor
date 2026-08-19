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
using System.Text;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The SERVICE's own install-location diagnosis (#2185), as distinct from the installer's refusal, which
/// <see cref="DarlingInstallLocationTests"/> pins.
///
/// <para><b>Why the service needs its own.</b> #2187 taught <c>install-darling.ps1</c> to refuse a location
/// the service account cannot read, which covers every install that goes through the script — and none of the
/// ones that do not. The README's manual <c>sc create</c> path is documented, and registering the exe by hand
/// is exactly what someone does with a zip they just extracted. Those installs reached the reporter's
/// experience: an empty <c>Output:</c>, a bare loader status, then a missing <c>pg-admin-credential.dpapi</c>
/// and advice to start the service once, which they had. Nothing named the install directory.</para>
///
/// <para><b>What is pinned here.</b> The decision table both ways (a refused path must be named, and — the
/// half that decides whether an operator believes the first — a machine-scoped path must never be), the
/// message's three obligations, and the two silences: a console run, and a non-Windows host. The table is the
/// artifact: <see cref="Cases"/> is shared with the cross-language parity test in
/// <see cref="DarlingInstallLocationTests"/>, so the two definitions of "a location that cannot work" cannot
/// drift apart without a red test.</para>
/// </summary>
public sealed class DarlingServiceInstallLocationTests
{
    /// <summary>The machine profile root the table is written against — the ordinary one.</summary>
    private const string ProfileRoot = @"C:\Users";

    /// <summary>The installing operator's own profile, sitting inside the machine root as it normally does.</summary>
    private const string UserProfile = @"C:\Users\installer";

    /// <summary>
    /// The decision table. Every row carries WHY, because a bare "expected UserProfile, got None" over
    /// twenty-odd rows is not actionable, and half these rows exist to record a decision rather than a rule.
    /// </summary>
    internal static IReadOnlyList<(string InstallDirectory, InstallLocationVerdict Expected, string Because)> Cases =>
    [
        /* The reported shape (#2185), and the two ways the same folder gets written. */
        (@"C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0", InstallLocationVerdict.UserProfile,
            "the reported install: a zip extracted to a Desktop"),
        (@"C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0\", InstallLocationVerdict.UserProfile,
            "a trailing separator is not a way out of the profile"),
        (@"C:\Users\username\Downloads\PerformanceMonitorDarling", InstallLocationVerdict.UserProfile,
            "Downloads is the other place a zip lands"),

        /* The profile root itself, and the ways a path can be spelled. An install root sitting AT the root is
           exactly as unreadable as one below it. */
        (@"C:\Users", InstallLocationVerdict.UserProfile, "at the profile root, not under it"),
        (@"C:\Users\", InstallLocationVerdict.UserProfile, "the profile root with a trailing separator"),
        (@"c:\users\bob\x", InstallLocationVerdict.UserProfile, "Windows paths are case-insensitive"),
        (@"C:\Users\bob\..\bob\x", InstallLocationVerdict.UserProfile, "a relative segment is not a way out"),
        ("C:/Users/bob/x", InstallLocationVerdict.UserProfile, "a forward slash is not a way out"),

        /* Deliberate residual, shared with the installer and recorded rather than discovered later (#2187):
           C:\Users\Public is under the profile root and is refused, even though an install there would
           actually WORK - it grants NT AUTHORITY\SERVICE:(OI)(CI)(IO)(M,DC). Not carved out: nobody installs
           a Windows service into the shared documents profile, a carve-out would amount to documenting it as
           a reasonable place to install, and the refusal is not a dead end because it names the alternative. */
        (@"C:\Users\Public\PerformanceMonitorDarling", InstallLocationVerdict.UserProfile,
            "C:\\Users\\Public is a deliberate refusal, not an oversight - one rule, no exceptions (#2187)"),

        /* The false-refusal cases. A bare StartsWith fails every one of these, and a false refusal is worse
           than a missed one: it maligns an install that works. */
        (@"C:\UsersData\PerformanceMonitorDarling", InstallLocationVerdict.None,
            "C:\\UsersData is not under C:\\Users - the boundary this table exists for"),
        (@"C:\UsersData", InstallLocationVerdict.None, "the same boundary at the root itself"),
        (@"C:\Users2\PerformanceMonitorDarling", InstallLocationVerdict.None, "nor is C:\\Users2"),

        /* The documented location and the other machine-scoped ones #2187 asked about explicitly. These are
           the rows that decide whether the message is credible when it does appear. */
        (@"C:\PerformanceMonitorDarling", InstallLocationVerdict.None, "the documented install location"),
        (@"C:\ProgramData\PerformanceMonitorDarling", InstallLocationVerdict.None, "machine-scoped"),
        (@"C:\Program Files\PerformanceMonitorDarling", InstallLocationVerdict.None, "machine-scoped"),
        (@"D:\PerformanceMonitorDarling", InstallLocationVerdict.None, "a second local volume is fine"),

        /* The network half. The reason differs from the profile case - a virtual account reaches the network
           as the COMPUTER account, and a mapped letter belongs to one logon session - so the verdicts are
           separate values rather than one "bad location". */
        (@"\\fileserver\share\PerformanceMonitorDarling", InstallLocationVerdict.UncPath, "a UNC share"),
        (@"\\fileserver\share", InstallLocationVerdict.UncPath, "a UNC share root"),
        (@"Z:\PerformanceMonitorDarling", InstallLocationVerdict.MappedDrive, "Z: is mapped to a share"),
        (@"C:\Tools\PerformanceMonitorDarling", InstallLocationVerdict.None, "C: is a local volume, so not a mapped drive"),

        /* \\?\ is the long-path prefix on a LOCAL path, not a server name. Treating it as a share would
           strand an ordinary install root written the extended-length way. */
        (@"\\?\C:\PerformanceMonitorDarling", InstallLocationVerdict.None,
            "the extended-length prefix on a local path is not a UNC share"),

        /* Two residuals, shared with the installer and pinned so they stay known quantities rather than being
           rediscovered as bugs (#2348). Neither implementation matches the EXTENDED-LENGTH spelling of a bad
           location: \\?\C:\Users\... does not begin with C:\Users, and the \\?\ exclusion in the UNC test is
           wholesale, so \\?\UNC\server\share - a real share - is waved through as well. A fix belongs in both
           at once rather than in whichever one someone edits first, and these rows are what make the pair move
           together: change one side and the cross-language parity test goes red. */
        (@"\\?\C:\Users\bob\PerformanceMonitorDarling", InstallLocationVerdict.None,
            "known shared residual: the extended-length spelling defeats the profile prefix test"),
        (@"\\?\UNC\fileserver\share\PerformanceMonitorDarling", InstallLocationVerdict.None,
            "known shared residual: \\\\?\\UNC\\ is a real share the wholesale \\\\?\\ exclusion misses"),

        /* Nothing is not somewhere. An empty path must never become a relative one resolved against whatever
           the working directory happens to be. */
        ("", InstallLocationVerdict.None, "no path is not a bad path"),
        ("   ", InstallLocationVerdict.None, "nor is whitespace"),
    ];

    /// <summary>The drives the table's <c>isNetworkDrive</c> probe answers yes for.</summary>
    private static bool IsMappedInTheTable(string qualifier) =>
        string.Equals(qualifier, "Z:", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The whole decision, driven by the table. Runs every row and reports ALL disagreements at once: a
    /// classifier is a table, and finding out about one wrong row per CI round is how a table gets fixed
    /// wrong.
    /// </summary>
    [Fact]
    public void Classify_DecidesEveryRowOfTheTable()
    {
        var wrong = new List<string>();

        foreach (var (directory, expected, because) in Cases)
        {
            var actual = DarlingInstallLocation.Classify(directory, ProfileRoot, UserProfile, IsMappedInTheTable);
            if (actual != expected)
            {
                wrong.Add($"'{directory}' => {actual}, expected {expected} ({because})");
            }
        }

        Assert.True(wrong.Count == 0, "the service's install-location table is wrong:\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The profile root is whatever Windows says it is, not <c>C:\Users</c>. It is relocatable, and a literal
    /// would quietly stop matching on precisely the box that moved it — the one box where a missed check costs
    /// the most.
    /// </summary>
    [Fact]
    public void Classify_HonoursARelocatedProfileRoot()
    {
        Assert.Equal(
            InstallLocationVerdict.UserProfile,
            DarlingInstallLocation.Classify(@"D:\Profiles\bob\PerformanceMonitorDarling", @"D:\Profiles", UserProfile, IsMappedInTheTable));

        /* And the OLD location stops being special once the root has moved, which is the half that proves the
           value is actually being read rather than checked alongside a hardcoded literal. */
        Assert.Equal(
            InstallLocationVerdict.None,
            DarlingInstallLocation.Classify(@"C:\Users\bob\PerformanceMonitorDarling", @"D:\Profiles", @"D:\Profiles\installer", IsMappedInTheTable));
    }

    /// <summary>
    /// A profile redirected outside <c>ProfilesDirectory</c> is still a profile, so the current identity's own
    /// profile is checked as well as the machine root — the same second arm the installer applies.
    /// </summary>
    [Fact]
    public void Classify_AlsoChecksTheCurrentIdentitysOwnProfile()
    {
        Assert.Equal(
            InstallLocationVerdict.UserProfile,
            DarlingInstallLocation.Classify(@"E:\redirected\bob\PerformanceMonitorDarling", ProfileRoot, @"E:\redirected\bob", IsMappedInTheTable));
    }

    /// <summary>
    /// The mapped-drive probe is only consulted for a path that HAS a drive qualifier, and an unknown drive is
    /// not a refusal. A refusal needs evidence: the cost of missing one mapped drive is a message that does not
    /// appear, while the cost of inventing one is telling an operator their working install is broken.
    /// </summary>
    [Fact]
    public void Classify_NeverInventsAMappedDrive()
    {
        var asked = new List<string>();
        bool Probe(string qualifier)
        {
            asked.Add(qualifier);
            return false;
        }

        Assert.Equal(
            InstallLocationVerdict.None,
            DarlingInstallLocation.Classify(@"Z:\PerformanceMonitorDarling", ProfileRoot, UserProfile, Probe));
        Assert.Equal(new[] { "Z:" }, asked);

        /* A UNC path is decided lexically and must not spend a drive probe at all - there is no letter to ask
           about, and Split-Path -Qualifier's C# equivalent would have to invent one. */
        asked.Clear();
        Assert.Equal(
            InstallLocationVerdict.UncPath,
            DarlingInstallLocation.Classify(@"\\fileserver\share\x", ProfileRoot, UserProfile, Probe));
        Assert.Empty(asked);
    }

    /// <summary>
    /// The registry-unreadable fallback root must be a ROOTED path, not a drive-relative one.
    ///
    /// <para><b>The bug this pins.</b> The fallback was <c>Path.Combine(systemDrive, "Users")</c>, and
    /// <c>%SystemDrive%</c> is documented to be a bare <c>C:</c> — which <c>Path.Combine</c> treats like a
    /// trailing separator and concatenates, producing <c>C:Users</c>. <see cref="Path.GetFullPath(string)"/>
    /// then resolves that against the process's current directory on the volume instead of the volume root, so
    /// the profile check silently stopped matching on precisely the box the fallback exists for: one whose
    /// <c>ProfileList</c> cannot be read. The registry read succeeds on every box CI runs on, which is exactly
    /// why the branch had to be made reachable to be pinned at all (review catch on #2185).</para>
    ///
    /// <para>Asserted against the separator rather than a literal <c>C:\Users</c>, so the row that matters —
    /// "the drive and the folder are not merely concatenated" — is the one being checked.</para>
    /// </summary>
    [Fact]
    public void ProfileRootForSystemDrive_IsRooted_NotDriveRelative()
    {
        var separator = Path.DirectorySeparatorChar;

        foreach (var (drive, expectedDrive, because) in new[]
        {
            ("C:", "C:", "the documented bare form, and the one that produced C:Users"),
            (@"C:\", "C:", "a drive that already carries a separator must not double it"),
            ("D:", "D:", "a box booted from another volume"),
            ("", "C:", "an unset %SystemDrive% falls back to C:"),
            ("   ", "C:", "and so does a blank one"),
            (null, "C:", "and so does a missing one"),
        })
        {
            var actual = DarlingInstallLocation.ProfileRootForSystemDrive(drive);

            Assert.Equal(expectedDrive + separator + "Users", actual);
            Assert.DoesNotContain(expectedDrive + "Users", actual, StringComparison.Ordinal);
            Assert.True(actual.Length > 0, because);
        }

        /* And the shipped lookup's answer is rooted whichever branch it took - the registry read on a healthy
           box, or the fallback on a locked-down one. A drive-relative answer here is the defect above. */
        Assert.True(Path.IsPathFullyQualified(DarlingInstallLocation.MachineProfileRoot()),
            "MachineProfileRoot must return a fully-qualified path, or IsAtOrUnder resolves it against the " +
            "process's current directory and the profile check stops matching (#2185)");
    }

    /// <summary>
    /// The message's three obligations, one per verdict: name the offending PATH, say WHY this account cannot
    /// read it, and say WHAT TO DO. It also has to name the downstream messages it displaces — those are what
    /// the operator has already been chasing by the time they read this, and #2185 took four exchanges
    /// precisely because nothing connected them.
    /// </summary>
    /* A Fact over the three verdicts rather than a Theory: xUnit needs a public signature, and the verdict
       enum is internal to the service assembly. Every verdict is checked in one run either way. */
    [Fact]
    public void Describe_NamesThePathTheReasonAndTheRemedy()
    {
        const string Directory = @"C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0";
        const string Account = @"NT SERVICE\PerformanceMonitor Darling";

        foreach (var verdict in new[]
        {
            InstallLocationVerdict.UserProfile,
            InstallLocationVerdict.UncPath,
            InstallLocationVerdict.MappedDrive,
        })
        {
            var message = DarlingInstallLocation.Describe(verdict, Directory, Account);

            /* The path, exactly as the service knows it - an operator has to be able to match it against their
               own log's content-root line without interpreting anything. */
            Assert.Contains(Directory, message, StringComparison.Ordinal);

            /* The account, resolved rather than assumed: an operator who re-homed the service to a domain
               account or gMSA (#1802/#1823) must not be told to reason about a virtual account they do not
               use. */
            Assert.Contains(Account, message, StringComparison.Ordinal);

            /* The remedy, with a destination. A refusal that does not name where to go instead is where
               #2185's reporter already was. */
            Assert.Contains(DarlingInstallLocation.DocumentedInstallDirectory, message, StringComparison.Ordinal);
            Assert.Contains("install-darling.ps1", message, StringComparison.Ordinal);

            /* And the connection to what they have already seen: the loader status (#2186) and the credential
               message (#2197) are both downstream of this, and saying so is the whole point of saying it
               early. */
            Assert.Contains("0xC0000135", message, StringComparison.Ordinal);
            Assert.Contains("pg-admin-credential.dpapi", message, StringComparison.Ordinal);
            Assert.Contains("#2185", message, StringComparison.Ordinal);

            /* BOTH store modes, because the message is built before darling.json is read and cannot know which
               one is configured. The initdb narrative is the managed default's; an operator on
               bring-your-own Postgres has no initdb to fail, and sending them to look for one would spend the
               credibility this message exists to have (review catch on #2185). */
            Assert.Contains("postgres.managed = true", message, StringComparison.Ordinal);
            Assert.Contains("postgres.managed = false", message, StringComparison.Ordinal);
            Assert.Contains("Cannot load configuration", message, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The three reasons are actually different sentences. A single "bad location" message would be a
    /// half-diagnosis: a profile fails on ACL inheritance, a share fails because a virtual account reaches
    /// the network as the computer account, and a mapped letter fails because it belongs to a logon session a
    /// service never joins. An operator who is told the wrong one goes and checks the wrong thing.
    /// </summary>
    [Fact]
    public void Describe_GivesEachVerdictItsOwnReason()
    {
        var profile = DarlingInstallLocation.Describe(InstallLocationVerdict.UserProfile, @"C:\Users\bob\x", "ACCOUNT");
        var unc = DarlingInstallLocation.Describe(InstallLocationVerdict.UncPath, @"\\fs\share\x", "ACCOUNT");
        var mapped = DarlingInstallLocation.Describe(InstallLocationVerdict.MappedDrive, @"Z:\x", "ACCOUNT");

        Assert.Contains("under a user profile", profile, StringComparison.Ordinal);
        Assert.Contains("BUILTIN\\Users", profile, StringComparison.Ordinal);

        Assert.Contains("UNC network path", unc, StringComparison.Ordinal);
        Assert.Contains("COMPUTER account", unc, StringComparison.Ordinal);

        Assert.Contains("mapped network drive", mapped, StringComparison.Ordinal);
        Assert.Contains("logon session", mapped, StringComparison.Ordinal);

        Assert.NotEqual(profile, unc);
        Assert.NotEqual(unc, mapped);
    }

    /// <summary>
    /// A refused location produces exactly ONE critical line. The issue's ask was one clear actionable
    /// message, and a diagnosis split across four lines is how the useful one gets quoted without the others.
    /// </summary>
    [Fact]
    public void Report_SaysItOnce_Critically()
    {
        var log = new CountingLogger();

        DarlingInstallLocation.Report(
            Path.Combine(DarlingInstallLocation.MachineProfileRoot(), "username", "Desktop", "PerformanceMonitorDarling-3.2.0"),
            runningAsWindowsService: true,
            log);

        Assert.Equal(1, log.Count(LogLevel.Critical));
        Assert.Equal(1, log.Total);
        Assert.Contains("under a user profile", log.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// The documented install location says nothing at all. The most important property in the file: a
    /// warning every healthy install prints is a warning nobody reads, and this one has to be believed on the
    /// day it appears.
    /// </summary>
    [Fact]
    public void Report_IsSilentForAMachineScopedInstall()
    {
        var log = new CountingLogger();

        DarlingInstallLocation.Report(DarlingInstallLocation.DocumentedInstallDirectory, runningAsWindowsService: true, log);

        Assert.Equal(0, log.Total);
    }

    /// <summary>
    /// A console run says nothing either, even from the worst possible folder. Running the exe interactively
    /// is something the README suggests, an interactive run IS the profile owner, and the tree it cannot read
    /// as a service is perfectly readable as them — so this is the one false positive available to the check,
    /// aimed at someone doing exactly what the docs told them to.
    /// </summary>
    [Fact]
    public void Report_IsSilentOnAConsoleRun()
    {
        var log = new CountingLogger();

        DarlingInstallLocation.Report(
            Path.Combine(DarlingInstallLocation.MachineProfileRoot(), "username", "Desktop", "PerformanceMonitorDarling-3.2.0"),
            runningAsWindowsService: false,
            log);

        Assert.Equal(0, log.Total);
    }

    /// <summary>
    /// The worker's call site is platform-gated, so a Linux or container host (the compose deployment, which
    /// runs bring-your-own Postgres and no virtual service account at all) can never reach this check. Pinned
    /// on the SOURCE because the assertion is about the call site rather than the classifier: the test process
    /// is Windows by construction, so nothing runnable here can observe the gate.
    /// </summary>
    [Fact]
    public void TheWorker_CallsThisOnlyOnWindows_AndBeforeAnythingElse()
    {
        var worker = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var executeAsync = worker.IndexOf(
            "protected override async Task ExecuteAsync(CancellationToken stoppingToken)", StringComparison.Ordinal);
        Assert.True(executeAsync >= 0, "DarlingWorker.ExecuteAsync moved");

        var report = worker.IndexOf("DarlingInstallLocation.Report(", executeAsync, StringComparison.Ordinal);
        Assert.True(report >= 0, "DarlingWorker no longer diagnoses the install location (#2185)");

        /* The platform gate, immediately above the call. */
        var gate = worker.LastIndexOf("if (OperatingSystem.IsWindows())", report, StringComparison.Ordinal);
        Assert.True(gate > executeAsync, "the install-location report must sit inside a Windows guard (#2185)");

        /* And it runs before anything that can fail in a way this explains. Config load first: an unreadable
           tree takes darling.json out too, and "Cannot load configuration" is another message that never
           names the install directory. */
        foreach (var (marker, what) in new[]
        {
            ("config = DarlingConfig.Load();", "loading darling.json"),
            ("new DarlingManagedPostgres(config.Postgres, _logger)", "constructing the managed-Postgres bootstrap"),
            ("await managedPostgres.EnsureRunningAsync(stoppingToken)", "the managed-Postgres bootstrap"),
        })
        {
            var at = worker.IndexOf(marker, executeAsync, StringComparison.Ordinal);
            Assert.True(at >= 0, $"DarlingWorker.ExecuteAsync no longer contains {what} ('{marker}')");
            Assert.True(report < at,
                $"the install-location diagnosis must run BEFORE {what}, or the operator reads the downstream failure first (#2185)");
        }
    }

    /// <summary>Walks up from the test output directory to the repo root — the same idiom
    /// <see cref="DarlingInstallLocationTests"/> uses.</summary>
    private static string ReadRepoFile(string relativePath)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            var candidate = Path.Combine(directory.FullName, relativePath);
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")) && File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }

            directory = directory.Parent;
        }

        Assert.Fail($"could not find {relativePath} above {AppContext.BaseDirectory}");
        return string.Empty;
    }

    /// <summary>Counts lines per level as well as capturing them: "exactly one critical" is the assertion, and
    /// a text-only capture cannot make it.</summary>
    private sealed class CountingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> _lines = new();

        public int Total
        {
            get { lock (_lines) { return _lines.Count; } }
        }

        public int Count(LogLevel level)
        {
            lock (_lines)
            {
                var n = 0;
                foreach (var (l, _) in _lines)
                {
                    if (l == level) { n++; }
                }

                return n;
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add((logLevel, formatter(state, exception)));
            }
        }

        public override string ToString()
        {
            lock (_lines)
            {
                var builder = new StringBuilder();
                foreach (var (level, message) in _lines)
                {
                    builder.Append('[').Append(level).Append("] ").AppendLine(message);
                }

                return builder.ToString();
            }
        }
    }
}
