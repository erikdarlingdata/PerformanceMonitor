/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.Versioning;
using System.Security;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Win32;

namespace PerformanceMonitor.Darling.Service;

/// <summary>Why an install directory is one the service's own account cannot read. <see cref="None"/> is the
/// only value that means "this location is fine".</summary>
internal enum InstallLocationVerdict
{
    /// <summary>A machine-scoped local path. Nothing to say.</summary>
    None,

    /// <summary>At or under a user profile — the reported shape (#2185): a zip extracted to a Desktop.</summary>
    UserProfile,

    /// <summary>A UNC path (<c>\\server\share\...</c>).</summary>
    UncPath,

    /// <summary>A drive letter mapped to a network share.</summary>
    MappedDrive,
}

/// <summary>
/// Says out loud, at service start, that the install directory is one the service account can never read
/// (#2185) — before the bundled PostgreSQL bootstrap turns that into an exit code nobody can read.
///
/// <para><b>The reported experience.</b> A zip extracted to
/// <c>C:\Users\username\Desktop\PerformanceMonitorDarling-3.2.0\</c> — a completely reasonable thing to do
/// with a download — produced <c>initdb failed (exit code -1073741515)</c> with an empty <c>Output:</c>, and
/// then a message about a missing <c>pg-admin-credential.dpapi</c> telling the operator to "start the service
/// once", which they had. Every visible message was downstream of the install location, and none of them
/// named it. #2186 decoded the exit code, #2197 fixed the credential message's advice, and #2187 taught
/// <c>install-darling.ps1</c> to refuse the location — but the installer can only guard the installs that go
/// through it. The README's manual <c>sc create</c> path, and anyone who registers the exe by hand, bypass it
/// entirely, which is why the SERVICE has to be able to reach this conclusion by itself.</para>
///
/// <para><b>The mechanism, measured rather than assumed</b> (#2187, on a clean Windows 11 box). The service
/// runs as the virtual account <c>NT SERVICE\PerformanceMonitor Darling</c> and never as LocalSystem, because
/// the bundled PostgreSQL refuses to run with administrative privileges. A directory created under a profile
/// inherits exactly <c>SYSTEM</c> / <c>Administrators</c> / the profile owner — no <c>BUILTIN\Users</c>, no
/// Authenticated Users, no CREATOR OWNER — so that account cannot read the program files it was pointed at,
/// and cannot read back what it writes there itself. A directory created under <c>C:\</c> inherits
/// <c>BUILTIN\Users:(RX)</c> from the volume root instead, which every service account is a member of, which
/// is why the documented location works and a profile does not.</para>
///
/// <para><b>Why this diagnoses and does not refuse to start.</b> The installer's asymmetry, for the same
/// reason (#2187): a fresh install is refused outright, but an existing service already living there is asked
/// rather than blocked, because an operator may have granted the tree read + execute by hand — #2187's
/// rejected option 2 — and stranding a deployment that runs today would be worse than the disease. A service
/// has nobody to ask, so it takes the same side: it states the cause, unmissably and BEFORE the failure it
/// predicts, and then gets on with the start it was asked for. An operator who hand-granted the tree keeps a
/// working service and a standing note that it is one ACL reset away from breaking, which is true.</para>
///
/// <para><b>The decision table is deliberately the same one <c>install-darling.ps1</c> applies</b> — profile
/// root from <c>ProfileList\ProfilesDirectory</c> plus <c>%USERPROFILE%</c>, UNC excluding the <c>\\?\</c>
/// long-path prefix, and a drive letter whose type is network. Two definitions of "a location that cannot
/// work" would drift, and the one that drifted would be the one nobody was reading. <c>DarlingInstallLocationTests</c>
/// runs BOTH over one shared table and fails if they ever disagree. Making the C# the single source and having
/// the script call it is the better end state and is NOT done here: it would change the installer's behavior,
/// which is not this change's to make.</para>
/// </summary>
[SupportedOSPlatform("windows")]
internal static class DarlingInstallLocation
{
    /// <summary>The documented machine-scoped location, named in every remedy this class prints so the
    /// operator is never left with a refusal and no destination.</summary>
    internal const string DocumentedInstallDirectory = @"C:\PerformanceMonitorDarling";

    /// <summary>
    /// The one message, logged critical, when <paramref name="installDirectory"/> is somewhere the service
    /// account cannot read. Never throws: a diagnostic that costs the service its start is worse than the
    /// diagnostic being absent.
    /// </summary>
    /// <param name="installDirectory">Normally <c>AppContext.BaseDirectory</c>.</param>
    /// <param name="runningAsWindowsService">
    /// <see langword="false"/> silences this entirely, and that is the point of the parameter rather than a
    /// convenience. A console run from a Desktop folder is a SUPPORTED thing to do — the README suggests
    /// test-driving the service interactively, and an interactive run is the profile owner, so the tree it
    /// cannot read as a service is perfectly readable as them. Warning there would be the one false positive
    /// available to this check, aimed at someone doing exactly what the docs told them to.
    /// </param>
    internal static void Report(string installDirectory, bool runningAsWindowsService, ILogger logger)
    {
        if (!runningAsWindowsService)
        {
            return;
        }

        try
        {
            var verdict = Classify(
                installDirectory,
                MachineProfileRoot(),
                Environment.GetEnvironmentVariable("USERPROFILE"),
                IsNetworkDrive);

            if (verdict == InstallLocationVerdict.None)
            {
                return;
            }

            logger.LogCritical("{Diagnosis}", Describe(verdict, installDirectory, DarlingFileSecurity.ServiceAccountDisplayName));
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                "Could not check whether the install directory {Root} is one this service's account can read ({Message}).",
                installDirectory, ex.Message);
        }
    }

    /// <summary>
    /// The decision, with every environment lookup passed in so the table can be driven by a test rather than
    /// by the box the test runs on.
    ///
    /// <para>The profile answer WINS over the network answer when a path is somehow both, matching the
    /// installer: the profile case is the one that gets reported, so it is the one whose explanation an
    /// operator most needs to read.</para>
    /// </summary>
    /// <param name="profilesDirectory">The machine's profile root, e.g. <c>C:\Users</c>.</param>
    /// <param name="userProfileDirectory">
    /// The current identity's own profile. Checked as well as the machine root because a profile redirected
    /// outside <c>ProfilesDirectory</c> is still a profile. Under a service this is the service account's
    /// profile rather than an operator's, so it is the arm that almost never fires here — kept anyway,
    /// because a definition that is the same as the installer's except for one arm is exactly the drift this
    /// class exists to avoid.
    /// </param>
    /// <param name="isNetworkDrive">Given a qualifier such as <c>Z:</c>, whether it maps to a share.</param>
    internal static InstallLocationVerdict Classify(
        string installDirectory,
        string? profilesDirectory,
        string? userProfileDirectory,
        Func<string, bool> isNetworkDrive)
    {
        if (string.IsNullOrWhiteSpace(installDirectory))
        {
            /* Nothing is not somewhere. An empty path must never become a relative one resolved against
               whatever the working directory happens to be. */
            return InstallLocationVerdict.None;
        }

        if (IsAtOrUnder(installDirectory, profilesDirectory) || IsAtOrUnder(installDirectory, userProfileDirectory))
        {
            return InstallLocationVerdict.UserProfile;
        }

        /* \\?\ is the long-path prefix on a LOCAL path, not a server name. Refusing it would strand an
           ordinary install root written the extended-length way. */
        if (installDirectory.StartsWith(@"\\", StringComparison.Ordinal)
            && !installDirectory.StartsWith(@"\\?\", StringComparison.Ordinal))
        {
            return InstallLocationVerdict.UncPath;
        }

        var qualifier = DriveQualifier(installDirectory);
        if (qualifier is not null && isNetworkDrive(qualifier))
        {
            return InstallLocationVerdict.MappedDrive;
        }

        return InstallLocationVerdict.None;
    }

    /// <summary>
    /// True when <paramref name="candidate"/> IS <paramref name="parent"/> or sits underneath it.
    ///
    /// <para>The separator is appended before the prefix test on purpose, and it is the one thing here worth
    /// being exact about: a bare <c>StartsWith</c> reads <c>C:\UsersData</c> as living under <c>C:\Users</c>,
    /// and a false refusal is a worse failure than the one this check exists to catch — it maligns an install
    /// that would have worked. Equality counts as under: a root sitting AT the profile root is exactly as
    /// unreadable as one below it.</para>
    /// </summary>
    internal static bool IsAtOrUnder(string? candidate, string? parent)
    {
        if (string.IsNullOrWhiteSpace(candidate) || string.IsNullOrWhiteSpace(parent))
        {
            return false;
        }

        string child;
        string root;
        try
        {
            /* Normalizes separators, resolves . and .., and makes the answer independent of how the path was
               typed. Anything GetFullPath rejects is not a path we can reason about, so it is not matched. */
            child = Path.TrimEndingDirectorySeparator(Path.GetFullPath(candidate));
            root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(parent));
        }
        catch (Exception ex) when (ex is ArgumentException or NotSupportedException or PathTooLongException or IOException)
        {
            return false;
        }

        return child.Equals(root, StringComparison.OrdinalIgnoreCase)
            || child.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// <c>Z:</c> for <c>Z:\Darling</c>, null for anything without a drive letter — the C# equivalent of the
    /// installer's <c>Split-Path -Qualifier</c>.
    ///
    /// <para>Read off the string rather than through <see cref="Path.GetPathRoot(string)"/> so it answers the
    /// same way on the machine running the tests as on Windows: a drive qualifier is a Windows concept, and
    /// <c>GetPathRoot</c> returns nothing for one on any other platform, which would make the mapped-drive
    /// arm of the table unrunnable anywhere but Windows.</para>
    /// </summary>
    internal static string? DriveQualifier(string path)
    {
        if (path.Length < 2 || path[1] != ':')
        {
            return null;
        }

        return char.IsLetter(path[0]) ? path.Substring(0, 2) : null;
    }

    /// <summary>
    /// The message. One log line, three jobs: name the path, say why the account cannot read it, and say what
    /// to do about it — with the downstream messages named explicitly, because those are the ones the operator
    /// has already been chasing by the time they read this (#2185).
    /// </summary>
    internal static string Describe(InstallLocationVerdict verdict, string installDirectory, string serviceAccount)
    {
        var message = new StringBuilder();

        message.Append("This service is installed somewhere its own account cannot read: ")
            .Append(installDirectory).Append('.');

        switch (verdict)
        {
            case InstallLocationVerdict.UserProfile:
                message.Append(" That path is under a user profile, and this service is running as ")
                    .Append(serviceAccount)
                    .Append(" — which is not you, not SYSTEM, and not Administrators. A directory created under a profile grants access to about those three and nobody else (no BUILTIN\\Users, no Authenticated Users, no CREATOR OWNER), so the service cannot read its own program files there, and cannot even read back what it writes there itself.");
                break;

            case InstallLocationVerdict.UncPath:
                message.Append(" That path is a UNC network path, and this service is running as ")
                    .Append(serviceAccount)
                    .Append(" — a virtual service account reaches the network as the COMPUTER account rather than as the operator who installed it, so a share that opens for you is not open for it.");
                break;

            case InstallLocationVerdict.MappedDrive:
                message.Append(" That path is on a mapped network drive, and this service is running as ")
                    .Append(serviceAccount)
                    .Append(" — a drive letter belongs to the logon session that mapped it, which a service does not share and cannot see at all, and a virtual service account reaches the network as the COMPUTER account rather than as you.");
                break;

            default:
                /* Describe is only reached with a real verdict; Report returns on None. Kept explicit rather
                   than throwing, because a diagnostic must never be the thing that fails. */
                break;
        }

        message.Append(" The bundled PostgreSQL bootstrap is what fails first: initdb.exe dies at exit code -1073741515 (0xC0000135, STATUS_DLL_NOT_FOUND) in the Windows loader, before it can write a word of output. Everything after that is downstream of THIS and is not the fault — a missing pg-admin-credential.dpapi, and advice to start the service once, which starting the service again will not satisfy (#2185).")
            .Append(" FIX: stop the service, move the install to a machine-scoped local path — ")
            .Append(DocumentedInstallDirectory)
            .Append(" is the documented one, and a folder created there inherits read + execute for BUILTIN\\Users, which the service account is a member of — then re-run install-darling.ps1 from the new location. It updates the service in place; darling.json can move with the folder, and the store under %ProgramData%\\PerformanceMonitorDarling and its credentials stay exactly where they are.");

        return message.ToString();
    }

    /// <summary>
    /// The machine's profile root, read from where Windows actually keeps it rather than assumed to be
    /// <c>C:\Users</c>: <c>ProfilesDirectory</c> is relocatable, and a hardcoded literal would quietly stop
    /// matching on precisely the box that moved it — the one box where a missed check costs the most.
    ///
    /// <para><b>Read from the same registry value the installer reads</b>, and this is the second attempt: the
    /// first derived it from <c>%PUBLIC%</c>'s parent, on the belief that Windows keeps <c>PUBLIC</c> in step
    /// with <c>ProfilesDirectory</c>. It does not. <c>ProfilesDirectory</c> and <c>Public</c> are two
    /// INDEPENDENT <c>REG_EXPAND_SZ</c> values under the same key that merely default to the same tree, so an
    /// administrator who relocates profiles to <c>D:\Profiles</c> — a documented, supported move — without also
    /// moving Public leaves <c>%PUBLIC%</c> at <c>C:\Users\Public</c>. The service would then have answered
    /// <c>C:\Users</c> while the installer answered <c>D:\Profiles</c>, and an install under the box's real
    /// profile root would have passed silently: a false negative on exactly the case this exists to catch, on
    /// exactly the box where a missed check costs the most. Reading the value itself removes the divergence
    /// instead of documenting it (review catch on #2185).</para>
    ///
    /// <para>Falls back to <c>%SystemDrive%\Users</c>, which is what the installer falls back to. An
    /// unreadable <c>ProfileList</c> is not a reason to skip the check.</para>
    /// </summary>
    internal static string MachineProfileRoot()
    {
        try
        {
            using var profileList = Registry.LocalMachine.OpenSubKey(
                @"SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList");

            /* GetValue expands a REG_EXPAND_SZ by default; expanding again is harmless and keeps this
               correct if the value is ever stored as a plain string containing %SystemDrive%. */
            if (profileList?.GetValue("ProfilesDirectory") is string configured
                && !string.IsNullOrWhiteSpace(configured))
            {
                var expanded = Environment.ExpandEnvironmentVariables(configured);
                if (!string.IsNullOrWhiteSpace(expanded))
                {
                    return expanded;
                }
            }
        }
        catch (Exception ex) when (ex is SecurityException or UnauthorizedAccessException or IOException or ObjectDisposedException or PlatformNotSupportedException)
        {
            /* An unreadable ProfileList is not a reason to skip the check — fall through to the default. */
        }

        var systemDrive = Environment.GetEnvironmentVariable("SystemDrive");
        return Path.Combine(string.IsNullOrWhiteSpace(systemDrive) ? "C:" : systemDrive, "Users");
    }

    /// <summary>
    /// Whether a drive qualifier maps to a share. Unknown answers "no": a refusal needs evidence, and the
    /// cost of missing one mapped drive is a message that does not appear, while the cost of inventing one is
    /// telling an operator their working install is broken.
    /// </summary>
    private static bool IsNetworkDrive(string qualifier)
    {
        try
        {
            return new DriveInfo(qualifier).DriveType == DriveType.Network;
        }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }
}
