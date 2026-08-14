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
using System.IO;
using System.Text;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Turns a bundled-Postgres tool's exit code into words (#2186) — the shared vocabulary every
/// managed-store process failure is reported in.
///
/// <para><b>The bug this exists for, twice from the field:</b> a managed bootstrap failed with
/// <c>initdb failed (exit code -1073741515) for C:\ProgramData\PerformanceMonitorDarling\pg. Output:</c>
/// and nothing more. <c>-1073741515</c> is <c>0xC0000135</c>, <c>STATUS_DLL_NOT_FOUND</c>: Windows killed
/// the process in the LOADER, before a line of its own code ran. Which is also why <c>Output:</c> was
/// empty and always will be for this class of failure — the one field an operator reads is guaranteed
/// blank exactly when the failure is a load failure, so it reads as "no information available" rather
/// than "this is a loader failure". The operator's attention then went to the follow-on
/// missing-credential message and darling.json, neither of which was the fault.</para>
///
/// <para><b>Why a decoder and not one more format string:</b> the sibling case already converts "the
/// binaries could not run" into words (<c>MustRefuseUnidentifiableRuntime</c>, #1738) and every other
/// bundled-tool failure printed the raw number. One shared decoder is what keeps the next one from
/// being the third report of the same defect.</para>
/// </summary>
internal static class DarlingToolExitCode
{
    /// <summary>
    /// NTSTATUS severity ERROR — the top two bits set. A process whose exit code lands in this range did
    /// not choose it: Windows did, either in the loader or by ending a crashed process. Real programs exit
    /// with small numbers (initdb 1, pg_ctl 3/4), so the range is a clean discriminator rather than a
    /// heuristic, and treating the WHOLE range as "Windows killed it" is what keeps an unlisted status
    /// from being as opaque as -1073741515 was.
    /// </summary>
    private const uint NtStatusErrorFloor = 0xC0000000;

    /// <summary>
    /// The statuses worth naming. Loader statuses (the ones that mean the image or a dependency never
    /// loaded) carry <c>Loader = true</c> and earn the full two-causes-two-checks diagnosis; the rest are
    /// named so the number stops being a mystery, and get the shorter paragraph their kind deserves.
    /// </summary>
    private static readonly Dictionary<uint, (string Name, StatusKind Kind)> s_knownStatuses = new()
    {
        [0xC0000135] = ("STATUS_DLL_NOT_FOUND", StatusKind.Loader),
        [0xC0000139] = ("STATUS_ENTRYPOINT_NOT_FOUND", StatusKind.Loader),
        [0xC000007B] = ("STATUS_INVALID_IMAGE_FORMAT", StatusKind.Loader),
        [0xC0000142] = ("STATUS_DLL_INIT_FAILED", StatusKind.Loader),
        [0xC0000022] = ("STATUS_ACCESS_DENIED", StatusKind.Loader),
        [0xC0000005] = ("STATUS_ACCESS_VIOLATION", StatusKind.Crash),
        [0xC0000374] = ("STATUS_HEAP_CORRUPTION", StatusKind.Crash),
        [0xC0000409] = ("STATUS_STACK_BUFFER_OVERRUN", StatusKind.Crash),
        [0xC000013A] = ("STATUS_CONTROL_C_EXIT", StatusKind.Terminated),
    };

    private enum StatusKind
    {
        Loader,
        Crash,
        Terminated,
    }

    /// <summary>
    /// The exit code as an operator should read it: the bare number for a tool's own status (initdb's 1,
    /// pg_ctl status's 3), and the number PLUS its hex and NTSTATUS name when Windows set it.
    /// </summary>
    internal static string Describe(int exitCode)
    {
        var status = unchecked((uint)exitCode);
        if (status < NtStatusErrorFloor)
        {
            return exitCode.ToString(CultureInfo.InvariantCulture);
        }

        /* No parentheses of its own: every call site already sits inside one ("exit code {...}"), and
           nesting them produced "(exit code -1073741515 (0xC0000135 STATUS_DLL_NOT_FOUND))". The "=" form
           is also how the field report itself explained the number. */
        var hex = "0x" + status.ToString("X8", CultureInfo.InvariantCulture);
        return s_knownStatuses.TryGetValue(status, out var known)
            ? $"{exitCode.ToString(CultureInfo.InvariantCulture)} = {hex} {known.Name}"
            : $"{exitCode.ToString(CultureInfo.InvariantCulture)} = {hex}, an unnamed Windows status rather than the program's own exit code";
    }

    /// <summary>
    /// True when Windows killed the process in the LOADER — a dependency problem, not a tool error. The
    /// caller uses this to decide whether gathering more evidence is worth a process launch (#2185).
    /// </summary>
    internal static bool IsLoaderStatus(int exitCode)
    {
        var status = unchecked((uint)exitCode);
        return status >= NtStatusErrorFloor
               && s_knownStatuses.TryGetValue(status, out var known)
               && known.Kind == StatusKind.Loader;
    }

    /// <summary>
    /// Names WHICH binary could not load, from a <c>--version</c> probe of each (#2185).
    ///
    /// <para><b>Why this exists.</b> The field report that produced it took four exchanges and still is not
    /// diagnosed, and the decisive clue was buried in the operator's shell rather than in any log: they ran
    /// <c>initdb --version</c> by hand and it printed <c>initdb (PostgreSQL) 18.4</c>, while the service's
    /// real <c>initdb</c> run died in the loader. A process that dies loading cannot print its own version,
    /// so those two facts together rule out the two causes the loader diagnosis suggests — and neither the
    /// operator nor the log could see that.</para>
    ///
    /// <para><b>The asymmetry that makes it diagnostic.</b> <c>initdb --version</c> prints and exits, but a
    /// real <c>initdb</c> run spawns <c>postgres.exe</c> in bootstrap mode to build the template database
    /// and propagates its status. So a dependency missing only for <c>postgres.exe</c> produces exactly the
    /// reported shape: the version probe succeeds and the bootstrap dies. Probing both separates that from
    /// "the whole runtime cannot load", which is a different fix.</para>
    /// </summary>
    internal static string DescribeRuntimeProbe(int initDbExitCode, int postgresExitCode)
    {
        var initDbLoaded = !IsLoaderStatus(initDbExitCode);
        var postgresLoaded = !IsLoaderStatus(postgresExitCode);

        if (initDbLoaded && !postgresLoaded)
        {
            return "\nRuntime probe: initdb.exe loaded and reported its version, but postgres.exe did NOT — " +
                   $"it exited {Describe(postgresExitCode)}. That is the specific cause: a real initdb run spawns " +
                   "postgres.exe in bootstrap mode to build the template database and passes its status back, so a " +
                   "dependency missing only for postgres.exe fails the bootstrap while leaving `initdb --version` " +
                   "working. Compare postgres.exe's imports against the DLLs beside it; the bundle is the suspect, " +
                   "not the install location or the service account.";
        }

        if (!initDbLoaded && !postgresLoaded)
        {
            return "\nRuntime probe: NEITHER initdb.exe nor postgres.exe could load, so this is not specific to " +
                   "one binary — the whole bundled runtime is failing to start. That points at the shared MSVC " +
                   "runtime beside the binaries or the machine's Universal CRT, rather than at any one tool.";
        }

        if (initDbLoaded && postgresLoaded)
        {
            return "\nRuntime probe: BOTH initdb.exe and postgres.exe loaded and reported their versions when " +
                   "probed just now. The loader failure is therefore not a permanently missing dependency — it is " +
                   "specific to the failing invocation, so capture the Event Viewer > Windows Logs > Application " +
                   "entry at the failure time, which names the module.";
        }

        return "\nRuntime probe: postgres.exe loaded but initdb.exe did not — unusual, since they share a " +
               $"dependency set; initdb.exe exited {Describe(initDbExitCode)}. Treat initdb.exe itself as the " +
               "damaged file and re-extract the package.";
    }

    /// <summary>
    /// The paragraph that follows the failure line: what Windows did, why the captured output is empty,
    /// and the checks that separate the two causes. Empty string for a tool's own exit code — initdb
    /// exiting 1 with a real error on stderr needs no help from here, and burying that message under
    /// boilerplate would make the common failure worse to read.
    /// </summary>
    internal static string Diagnose(int exitCode, string exePath)
    {
        var status = unchecked((uint)exitCode);
        if (status < NtStatusErrorFloor)
        {
            return string.Empty;
        }

        var toolName = SafeFileName(exePath);
        var kind = s_knownStatuses.TryGetValue(status, out var known) ? known.Kind : StatusKind.Terminated;

        return kind switch
        {
            StatusKind.Loader => LoaderDiagnosis(toolName, exePath),
            StatusKind.Crash =>
                $"\n{toolName} started and then crashed — Windows ended it, so anything it had to say may be truncated or missing entirely. " +
                "A crash is not a configuration problem and restarting is unlikely to clear it: capture the matching entry from Event Viewer > Windows Logs > Application " +
                "(Application Error / Windows Error Reporting) together with the store's pg.log, and report it.",
            _ =>
                $"\nWindows ended {toolName} rather than the tool exiting on its own, so its output may be empty or truncated and this is not a PostgreSQL error. " +
                "Event Viewer > Windows Logs > Application, at the time of the failure, usually carries the matching entry.",
        };
    }

    /// <summary>
    /// The two causes and the two checks, in the order an operator should work them. Both causes are
    /// specific to how this product ships PostgreSQL: the MSVC runtime is BUNDLED beside the binaries
    /// (so a missing one means a partial extract, not a missing prerequisite), and the service runs as an
    /// unprivileged virtual account that a user-profile install tree does not grant.
    /// </summary>
    private static string LoaderDiagnosis(string toolName, string exePath)
    {
        var binDirectory = SafeDirectoryName(exePath);
        var builder = new StringBuilder();

        builder.Append('\n').Append("Windows killed ").Append(toolName)
            .Append(" in the LOADER: it never ran a line of its own code. This is a Windows failure, not a PostgreSQL one, and it is why the captured output is empty — for a loader failure an empty Output is EXPECTED, not missing information.\n");

        builder.Append("Two causes account for nearly all of these:\n");
        builder.Append("  (1) The Microsoft Visual C++ runtime is missing from ").Append(binDirectory)
            .Append(". Packaging bundles vcruntime140.dll, vcruntime140_1.dll and msvcp140.dll there so the box needs no prerequisite — ")
            .Append("if any of the three is absent the install tree is a partial or damaged extract, so redeploy the package, or install the Microsoft Visual C++ 2015-2022 x64 redistributable.\n");
        builder.Append("  (2) The service account cannot read the install tree. The service runs as the virtual account NT SERVICE\\PerformanceMonitor Darling, which is neither you nor Administrators, ")
            .Append("so an install under a user profile (Desktop, Downloads, anywhere below C:\\Users) is unreadable to it — reinstall to a machine-scoped path such as C:\\PerformanceMonitorDarling.\n");

        builder.Append("Two checks that tell them apart:\n");
        builder.Append("  (a) Run \"").Append(exePath).Append("\" --version from an elevated prompt. Failing there too means (1). Succeeding points at (2) — and note that these tools re-execute themselves ")
            .Append("under a restricted token that drops the Administrators group, so a tree readable only VIA Administrators still fails the real run even when it runs by hand.\n");
        builder.Append("  (b) Event Viewer > Windows Logs > Application, at the time of the failure: an Application Error or SideBySide entry usually names the exact module that could not be loaded.");

        return builder.ToString();
    }

    /// <summary>
    /// The <c>Output:</c> field itself. Real output passes through untouched; a blank capture renders as
    /// something readable instead of nothing at all, and says so explicitly when Windows is the reason
    /// there was nothing to capture.
    /// </summary>
    internal static string FormatOutput(string? output, int exitCode)
    {
        if (!string.IsNullOrWhiteSpace(output))
        {
            return output;
        }

        return unchecked((uint)exitCode) >= NtStatusErrorFloor
            ? "(none — Windows ended the process before it could write anything, which is expected for this failure rather than missing information)"
            : "(none)";
    }

    /// <summary>Never let a malformed path turn a diagnostic into a second exception.</summary>
    private static string SafeFileName(string exePath)
    {
        try
        {
            var name = Path.GetFileName(exePath);
            return string.IsNullOrEmpty(name) ? "the tool" : name;
        }
        catch (ArgumentException)
        {
            return "the tool";
        }
    }

    private static string SafeDirectoryName(string exePath)
    {
        try
        {
            var directory = Path.GetDirectoryName(exePath);
            return string.IsNullOrEmpty(directory) ? "the pg-runtime\\pgsql\\bin directory" : directory;
        }
        catch (ArgumentException)
        {
            return "the pg-runtime\\pgsql\\bin directory";
        }
    }
}
