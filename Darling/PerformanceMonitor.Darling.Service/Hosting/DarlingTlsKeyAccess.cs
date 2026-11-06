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
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// <c>--grant-cert-key-access &lt;thumbprint&gt;</c> (#7718): gives the service account Read on the private key of a
/// certificate in a Windows certificate store, so the <c>store</c> form of <c>web.network.tls</c> and
/// <c>mcp.network.tls</c> can open it. It is the command form of certlm.msc &gt; All Tasks &gt; Manage Private Keys.
///
/// <para>The rules, all of them about doing no more than that:</para>
/// <list type="bullet">
/// <item>It grants Read, and only Read (<see cref="ReadMask"/>, the same bits <c>icacls (R)</c> grants), to ONE account:
/// the service's registered account, or the one <c>--account</c> names. It refuses every group, whatever the operator
/// typed: the broad principals by SID (<see cref="IsBroadPrincipal"/>) and every other group by what Windows says the SID
/// names (<see cref="RefuseByKind"/>).</item>
/// <item>It ADDS one allow entry and keeps every existing one. The new entry is placed after the explicit entries and
/// before the inherited ones, so the list stays in canonical order. The DACL is never replaced.</item>
/// <item>It changes nothing without elevation for <c>LocalMachine</c>, and nothing when the account already has Read
/// ("already granted"), and nothing when an explicit Deny on the account covers Read (a Deny wins over any Allow, so a
/// grant would be a lie).</item>
/// <item>It re-reads the key's access list after the write and reports success only when the account then has Read.</item>
/// <item>It touches no key material. It reads and writes the key's security descriptor, never its bytes.</item>
/// </list>
///
/// <para>The pure parts (argument parsing, the broad-principal rule, building the one new entry, the orchestration over
/// its seams) take no machine state, so the tests run them over in-memory descriptors and self-signed certificates.
/// <see cref="OpenKeyTarget"/>, <see cref="ResolveAccount"/> and <see cref="RunForReal"/> are the Windows half.</para>
/// </summary>
internal static class DarlingTlsKeyAccess
{
    /// <summary>The verb, as an operator types it.</summary>
    internal const string VerbName = "--grant-cert-key-access";

    internal const string Usage =
        "Usage: --grant-cert-key-access <thumbprint> [--account <name>] [--store-location LocalMachine|CurrentUser] [--store-name <name>]";

    /// <summary>FILE_GENERIC_READ (FileSystemRights.Read | Synchronize): the Read entry certlm.msc and <c>icacls (R)</c>
    /// grant. Never more. A literal, because the enum is Windows-only to the analyzer and this is also the text of a test.</summary>
    internal const int ReadMask = 0x120089;

    private const int GenericAll = 0x10000000;
    private const int GenericRead = unchecked((int)0x80000000);

    /// <summary>What the operator asked for.</summary>
    internal sealed record GrantArgs(string Thumbprint, string? Account, StoreLocation Location, string StoreName);

    /// <summary>The account a grant is for. <see cref="Error"/> is set when it could not be resolved.</summary>
    internal readonly record struct AccountResolution(SecurityIdentifier? Sid, string Display, string? Error);

    /// <summary>A key's security descriptor, readable and writable, whatever kind of key it is.</summary>
    internal interface IKeyAclTarget : IDisposable
    {
        /// <summary>What kind of key this is, for the output ("CNG key", "CAPI key file").</summary>
        string Kind { get; }

        /// <summary>The key's DACL as a self-relative security descriptor.</summary>
        byte[] ReadDescriptor();

        /// <summary>Replaces the key's DACL with the DACL of <paramref name="descriptor"/>.</summary>
        void WriteDescriptor(byte[] descriptor);
    }

    /// <summary>Everything <see cref="Run"/> needs from the machine, so a test supplies it.</summary>
    internal sealed record GrantSeams(
        Func<bool> IsElevated,
        Func<StoreLocation, string, IReadOnlyList<X509Certificate2>> ReadStore,
        Func<string?, AccountResolution> ResolveAccount,
        Func<X509Certificate2, IKeyAclTarget> OpenTarget);

    internal enum GrantOutcome
    {
        Added,
        AlreadyGranted,
        BlockedByDeny,
    }

    /// <summary>What <see cref="PlanGrant"/> decided. <see cref="NewDescriptor"/> is set only for <see cref="GrantOutcome.Added"/>.</summary>
    internal sealed record GrantPlan(GrantOutcome Outcome, byte[]? NewDescriptor, string? Note);

    /* ---- PURE: the arguments ---- */

    /// <summary>
    /// The arguments after the verb: one thumbprint, then optionally <c>--account</c>, <c>--store-location</c> and
    /// <c>--store-name</c>, each once. The location and name default as the config's do (LocalMachine, My). Returns false
    /// with a ready-to-print <paramref name="errorMessage"/> on anything else.
    /// </summary>
    internal static bool TryParseArgs(string[] rest, out GrantArgs? args, out string? errorMessage)
    {
        ArgumentNullException.ThrowIfNull(rest);

        args = null;
        errorMessage = null;
        string? thumbprint = null;
        string? account = null;
        string? locationText = null;
        string? storeName = null;

        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            if (string.Equals(arg, "--account", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryTakeValue(rest, ref i, "--account", ref account, out errorMessage))
                {
                    return false;
                }
            }
            else if (string.Equals(arg, "--store-location", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryTakeValue(rest, ref i, "--store-location", ref locationText, out errorMessage))
                {
                    return false;
                }
            }
            else if (string.Equals(arg, "--store-name", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryTakeValue(rest, ref i, "--store-name", ref storeName, out errorMessage))
                {
                    return false;
                }
            }
            else if (arg.StartsWith('-') || thumbprint is not null)
            {
                errorMessage = $"Unknown or unexpected argument for {VerbName}: {arg}" + Environment.NewLine + Usage;
                return false;
            }
            else
            {
                thumbprint = arg;
            }
        }

        if (string.IsNullOrWhiteSpace(thumbprint))
        {
            errorMessage = "A certificate thumbprint is required (certlm.msc > Personal > Certificates > the certificate > Details > Thumbprint)."
                + Environment.NewLine + Usage;
            return false;
        }

        var location = StoreLocation.LocalMachine;
        if (locationText is not null && !DarlingWebTls.TryParseStoreLocation(locationText, out location))
        {
            errorMessage = $"--store-location '{locationText.Trim()}' is not LocalMachine or CurrentUser." + Environment.NewLine + Usage;
            return false;
        }

        if (account is not null && string.IsNullOrWhiteSpace(account))
        {
            /* An empty value (a script's unset variable) must not fall through to "the registered account". */
            errorMessage = "--account is empty. Name the account, or leave --account out to use the service's registered account." + Environment.NewLine + Usage;
            return false;
        }

        if (storeName is not null && string.IsNullOrWhiteSpace(storeName))
        {
            errorMessage = "--store-name is empty." + Environment.NewLine + Usage;
            return false;
        }

        if (storeName is not null && DarlingTlsStore.HasPathSeparator(storeName))
        {
            errorMessage = "--store-name is a store name, not a path: it cannot contain a backslash or a slash." + Environment.NewLine + Usage;
            return false;
        }

        args = new GrantArgs(
            DarlingTlsStore.NormalizeThumbprint(thumbprint),
            account?.Trim(),
            location,
            storeName?.Trim() ?? DarlingTlsStore.DefaultStoreName);
        return true;
    }

    /// <summary>Takes the value after option <paramref name="option"/> at <c>rest[i]</c>. An option given twice, or
    /// with no value (or with another option where its value belongs), is an error.</summary>
    private static bool TryTakeValue(string[] rest, ref int i, string option, ref string? target, out string? errorMessage)
    {
        errorMessage = null;
        if (target is not null)
        {
            errorMessage = $"{option} was given twice." + Environment.NewLine + Usage;
            return false;
        }

        if (i + 1 >= rest.Length || rest[i + 1].StartsWith('-'))
        {
            errorMessage = $"{option} needs a value." + Environment.NewLine + Usage;
            return false;
        }

        target = rest[++i];
        return true;
    }

    /// <summary>The command that fixes a key the service cannot open, for the Critical line the loader logs. Names the
    /// account explicitly, because the line knows exactly which account failed.</summary>
    internal static string GrantCommandFor(string thumbprint, StoreLocation location, string storeName, string account)
    {
        var command = $"PerformanceMonitor.Darling.Service.exe {VerbName} {DarlingTlsStore.NormalizeThumbprint(thumbprint)}";
        if (location != StoreLocation.LocalMachine)
        {
            command += $" --store-location {location}";
        }

        if (!string.Equals(storeName, DarlingTlsStore.DefaultStoreName, StringComparison.OrdinalIgnoreCase))
        {
            command += $" --store-name \"{storeName}\"";
        }

        return command + $" --account \"{account}\"";
    }

    /* ---- PURE: who may be granted, and the one new entry ---- */

    /// <summary>
    /// True for a principal that stands for many accounts, by SID alone: Everyone, Users, Authenticated Users, Interactive,
    /// Network, Batch, Service, Anonymous, Terminal Server User, Remote Interactive Logon, This Organization, Local account,
    /// Local, Console Logon, ALL SERVICES, every application package group, Guests, Power Users, Remote Desktop Users,
    /// Pre-Windows 2000 Compatible Access, IIS_IUSRS, Remote Management Users, and a domain's Domain Users, Domain Guests
    /// and Domain Computers. The command refuses these whatever <c>--account</c> says (#7718): a TLS private key is read by
    /// the service account alone. <see cref="RefuseByKind"/> is the second check, by what the SID names, and refuses every
    /// other group.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static bool IsBroadPrincipal(SecurityIdentifier sid)
    {
        ArgumentNullException.ThrowIfNull(sid);

        var value = sid.Value;
        return value is "S-1-1-0" or "S-1-5-32-545" or "S-1-5-11" or "S-1-5-4" or "S-1-5-2" or "S-1-5-7" or "S-1-5-32-546"
                or "S-1-5-6" or "S-1-5-3" or "S-1-5-13" or "S-1-5-14" or "S-1-5-15" or "S-1-5-113" or "S-1-5-80-0"
                or "S-1-5-32-547" or "S-1-5-32-554" or "S-1-5-32-555" or "S-1-5-32-568" or "S-1-5-32-580"
            || value.StartsWith("S-1-2-", StringComparison.Ordinal)
            || value.StartsWith("S-1-15-2-", StringComparison.Ordinal)
            || (value.StartsWith("S-1-5-21-", StringComparison.Ordinal)
                && (value.EndsWith("-513", StringComparison.Ordinal) || value.EndsWith("-514", StringComparison.Ordinal)
                    || value.EndsWith("-515", StringComparison.Ordinal)));
    }

    /// <summary>What <c>LookupAccountSid</c> says a SID names (<c>SID_NAME_USE</c>).</summary>
    internal enum SidNameUse
    {
        User = 1,
        Group = 2,
        Domain = 3,
        Alias = 4,
        WellKnownGroup = 5,
        DeletedAccount = 6,
        Invalid = 7,
        Unknown = 8,
        Computer = 9,
        Label = 10,
        LogonSession = 11,
    }

    /// <summary>
    /// PURE: the reason <paramref name="sid"/>, which names a <paramref name="use"/>, may not be granted, or null when it
    /// may. The command grants to ONE account, so only a user (a domain user, a local user, a gMSA) passes, plus the
    /// service identities that Windows reports as a group but that name one service: LocalSystem, LocalService,
    /// NetworkService, a per-service virtual account (<c>NT SERVICE\name</c>, S-1-5-80-x, but not the S-1-5-80-0 group
    /// ALL SERVICES), an IIS application pool identity (S-1-5-82-*) and the local Administrators group.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static string? RefuseByKind(SecurityIdentifier sid, SidNameUse use)
    {
        ArgumentNullException.ThrowIfNull(sid);

        if (IsServiceIdentity(sid) || use == SidNameUse.User)
        {
            return null;
        }

        return use switch
        {
            SidNameUse.Group or SidNameUse.Alias or SidNameUse.WellKnownGroup => "is a group",
            SidNameUse.Computer => "is a computer",
            _ => $"is not a user or service account ({use})",
        };
    }

    [SupportedOSPlatform("windows")]
    private static bool IsServiceIdentity(SecurityIdentifier sid)
    {
        var value = sid.Value;
        return value is "S-1-5-18" or "S-1-5-19" or "S-1-5-20" or "S-1-5-32-544"
            || (value.StartsWith("S-1-5-80-", StringComparison.Ordinal) && value != "S-1-5-80-0")
            || value.StartsWith("S-1-5-82-", StringComparison.Ordinal);
    }

    /// <summary>
    /// PURE: what to do to <paramref name="descriptor"/> (a self-relative security descriptor) so that
    /// <paramref name="account"/> has Read. An account whose own entries already cover <see cref="ReadMask"/> is
    /// <see cref="GrantOutcome.AlreadyGranted"/>, and nothing is built. An explicit Deny on the account that overlaps
    /// Read is <see cref="GrantOutcome.BlockedByDeny"/> (it wins over any Allow). Otherwise ONE allow entry is added,
    /// Read only, to the account, ahead of the inherited entries, and every existing entry is kept as it was.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static GrantPlan PlanGrant(byte[] descriptor, SecurityIdentifier account)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentNullException.ThrowIfNull(account);

        var raw = new RawSecurityDescriptor(descriptor, 0);
        var acl = raw.DiscretionaryAcl;
        if (acl is null)
        {
            /* A NULL DACL grants everyone everything. Adding an entry would TAKE access from everyone else. */
            return new GrantPlan(GrantOutcome.AlreadyGranted, null, "the key has no access list, so every account can already read it");
        }

        var allowed = 0;
        var denied = 0;
        var firstInherited = -1;
        for (var i = 0; i < acl.Count; i++)
        {
            var entry = acl[i];
            if (firstInherited < 0 && (entry.AceFlags & AceFlags.Inherited) != 0)
            {
                firstInherited = i;
            }

            if (entry is not CommonAce ace
                || (ace.AceFlags & AceFlags.InheritOnly) != 0
                || !ace.SecurityIdentifier.Equals(account))
            {
                continue;
            }

            var mask = Normalize(ace.AccessMask);
            if (ace.AceQualifier == AceQualifier.AccessAllowed)
            {
                allowed |= mask;
            }
            else if (ace.AceQualifier == AceQualifier.AccessDenied)
            {
                denied |= mask;
            }
        }

        if ((denied & ReadMask) != 0)
        {
            return new GrantPlan(GrantOutcome.BlockedByDeny, null, null);
        }

        if ((allowed & ReadMask) == ReadMask)
        {
            return new GrantPlan(GrantOutcome.AlreadyGranted, null, null);
        }

        var index = firstInherited < 0 ? acl.Count : firstInherited;
        acl.InsertAce(index, new CommonAce(AceFlags.None, AceQualifier.AccessAllowed, ReadMask, account, false, null));

        var bytes = new byte[raw.BinaryLength];
        raw.GetBinaryForm(bytes, 0);
        return new GrantPlan(GrantOutcome.Added, bytes, null);
    }

    /// <summary>The generic bits an entry may carry, mapped to the file rights they mean.</summary>
    private static int Normalize(int mask)
    {
        if ((mask & GenericAll) != 0)
        {
            return -1;
        }

        return (mask & GenericRead) != 0 ? mask | ReadMask : mask;
    }

    /* ---- the orchestration, over its seams ---- */

    /// <summary>
    /// Runs the grant. Returns 0 on "granted" and "already granted", 1 on every refusal and failure, and writes what it
    /// did, for which account and certificate, to <paramref name="output"/>; a refusal goes to <paramref name="error"/>.
    /// Every refusal happens before anything is written, in this order: elevation (LocalMachine), the account, the
    /// store, the certificate (zero or two matches, no private key), the key, the Deny check.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static int Run(GrantArgs args, GrantSeams seams, TextWriter output, TextWriter error)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(seams);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        var where = $"{args.Location}\\{args.StoreName}";

        if (args.Location == StoreLocation.LocalMachine && !seams.IsElevated())
        {
            error.WriteLine($"{VerbName} changes the access list of a LocalMachine certificate key, so it needs an elevated prompt (Run as administrator).");
            error.WriteLine("Nothing was changed.");
            return 1;
        }

        var account = seams.ResolveAccount(args.Account);
        if (account.Sid is null)
        {
            error.WriteLine($"{VerbName}: {account.Error ?? "the account could not be resolved"}");
            error.WriteLine("Nothing was changed.");
            return 1;
        }

        if (IsBroadPrincipal(account.Sid))
        {
            error.WriteLine($"{VerbName}: {account.Display} stands for many accounts. This command grants a TLS private key to one account (the service's own), never to a group.");
            error.WriteLine("Nothing was changed.");
            return 1;
        }

        IReadOnlyList<X509Certificate2> all;
        try
        {
            all = seams.ReadStore(args.Location, args.StoreName);
        }
        catch (Exception ex)
        {
            error.WriteLine($"{VerbName}: the {where} certificate store could not be opened ({ex.Message})");
            error.WriteLine("Nothing was changed.");
            return 1;
        }

        try
        {
            X509Certificate2 certificate;
            try
            {
                certificate = DarlingTlsStore.SelectByThumbprint(all, args.Thumbprint, where, "thumbprint");
            }
            catch (InvalidOperationException ex)
            {
                error.WriteLine($"{VerbName}: {ex.Message}");
                error.WriteLine("Nothing was changed.");
                return 1;
            }

            var which = $"certificate {certificate.Subject} ({certificate.Thumbprint}) in {where}";
            IKeyAclTarget target;
            try
            {
                target = seams.OpenTarget(certificate);
            }
            catch (Exception ex) when (ex is InvalidOperationException or CryptographicException or IOException or UnauthorizedAccessException)
            {
                error.WriteLine($"{VerbName}: the private key of {which} could not be opened to read its access list ({ex.Message})");
                error.WriteLine("Nothing was changed.");
                return 1;
            }

            using (target)
            {
                return GrantOn(target, account.Sid, account.Display, which, output, error);
            }
        }
        finally
        {
            foreach (var certificate in all)
            {
                certificate.Dispose();
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static int GrantOn(
        IKeyAclTarget target, SecurityIdentifier account, string display, string which, TextWriter output, TextWriter error)
    {
        GrantPlan plan;
        try
        {
            plan = PlanGrant(target.ReadDescriptor(), account);
        }
        catch (Exception ex) when (ex is InvalidOperationException or CryptographicException or IOException
            or UnauthorizedAccessException or ArgumentException)
        {
            error.WriteLine($"{VerbName}: the access list of the private key of {which} could not be read ({ex.Message})");
            error.WriteLine("Nothing was changed.");
            return 1;
        }

        if (plan.Outcome == GrantOutcome.BlockedByDeny)
        {
            error.WriteLine($"{VerbName}: {display} has an explicit Deny entry on the private key of {which} that covers Read, and a Deny wins over any Allow.");
            error.WriteLine("Remove the Deny entry (certlm.msc > the certificate > All Tasks > Manage Private Keys) and run this again. Nothing was changed.");
            return 1;
        }

        if (plan.Outcome == GrantOutcome.AlreadyGranted)
        {
            output.WriteLine($"Already granted: {display} already has Read on the private key of {which} ({target.Kind}).");
            if (plan.Note is not null)
            {
                output.WriteLine($"  ({plan.Note})");
            }

            output.WriteLine("No change was made.");
            return 0;
        }

        try
        {
            target.WriteDescriptor(plan.NewDescriptor!);
        }
        catch (Exception ex) when (ex is InvalidOperationException or CryptographicException or IOException or UnauthorizedAccessException)
        {
            error.WriteLine($"{VerbName}: the access list of the private key of {which} could not be changed ({ex.Message})");
            error.WriteLine("The access list was not changed.");
            return 1;
        }

        /* The verdict is the re-read, never the write that returned without throwing. */
        var verified = false;
        try
        {
            verified = PlanGrant(target.ReadDescriptor(), account).Outcome == GrantOutcome.AlreadyGranted;
        }
        catch (Exception ex) when (ex is InvalidOperationException or CryptographicException or IOException
            or UnauthorizedAccessException or ArgumentException)
        {
            error.WriteLine($"{VerbName}: the access list was written but could not be read back ({ex.Message})");
        }

        if (!verified)
        {
            error.WriteLine($"{VerbName}: {display} does not have Read on the private key of {which} after the change. Check it in certlm.msc > the certificate > All Tasks > Manage Private Keys.");
            return 1;
        }

        output.WriteLine($"Granted Read on the private key of {which} ({target.Kind}) to {display}.");
        output.WriteLine("One Read entry was added for that account. Every existing entry was kept.");
        output.WriteLine($"Restart the '{DarlingCliCommands.ServiceName}' service so it reads the key.");
        return 0;
    }

    /* ---- the Windows half ---- */

    /// <summary>The real command: the machine's store, the service's registered account, this process's elevation.</summary>
    [SupportedOSPlatform("windows")]
    internal static int RunForReal(GrantArgs args, TextWriter output, TextWriter error)
        => Run(
            args,
            new GrantSeams(
                DarlingCliCommands.IsElevated,
                DarlingTlsStore.ReadStore,
                name => ResolveAccount(name, DarlingCliCommands.ServiceName),
                certificate => OpenKeyTarget(certificate)),
            output,
            error);

    /// <summary>
    /// The account to grant: <paramref name="name"/> when given (a name, or a SID in <c>S-1-</c> form), otherwise the
    /// account the service is REGISTERED under, which is the account it runs as. The caller is never a fallback: the
    /// operator running this is not the service. A service that is not registered, with no <c>--account</c>, is an error.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static AccountResolution ResolveAccount(string? name, string serviceName)
    {
        SecurityIdentifier? sid;
        if (string.IsNullOrWhiteSpace(name))
        {
            sid = DarlingFileSecurity.RegisteredServiceAccount(serviceName);
            if (sid is null)
            {
                return new AccountResolution(
                    null, "", $"the '{serviceName}' service is not registered on this machine, so there is no service account to grant. Pass --account <name>.");
            }
        }
        else
        {
            try
            {
                var trimmed = name.Trim();
                sid = trimmed.StartsWith("S-1-", StringComparison.OrdinalIgnoreCase)
                    ? new SecurityIdentifier(trimmed)
                    : (SecurityIdentifier)new NTAccount(trimmed).Translate(typeof(SecurityIdentifier));
            }
            catch (SystemException ex)
            {
                return new AccountResolution(null, name, $"the account '{name.Trim()}' could not be resolved on this machine ({ex.Message})");
            }
        }

        string display;
        try
        {
            display = ((NTAccount)sid.Translate(typeof(NTAccount))).Value;
        }
        catch (SystemException)
        {
            display = sid.Value;
        }

        if (IsBroadPrincipal(sid) || IsServiceIdentity(sid))
        {
            /* A broad group is refused by Run, with the message for a broad group. A service identity is accepted whatever
               the lookup says: a per-service virtual account does not always resolve, and Windows calls it a group. */
            return new AccountResolution(sid, display, null);
        }

        /* The command grants to ONE account: ask Windows what the SID names and refuse every group. A lookup that fails
           refuses too, because "not a group" was not shown. */
        if (!TryLookupSidNameUse(sid, out var use))
        {
            return new AccountResolution(
                null, display, $"could not tell whether '{display}' is a user or a group (the account lookup failed), so it is not granted. This command grants to one account, the service's own.");
        }

        var refusal = RefuseByKind(sid, use);
        return refusal is null
            ? new AccountResolution(sid, display, null)
            : new AccountResolution(null, display, $"'{display}' {refusal}. This command grants to one account (the service's own), never to a group.");
    }

    [SupportedOSPlatform("windows")]
    private static bool TryLookupSidNameUse(SecurityIdentifier sid, out SidNameUse use)
    {
        use = SidNameUse.Unknown;
        var binary = new byte[sid.BinaryLength];
        sid.GetBinaryForm(binary, 0);

        uint nameLength = 0;
        uint domainLength = 0;
        /* The first call only sizes the buffers: it fails with ERROR_INSUFFICIENT_BUFFER, which is expected. */
        _ = LookupAccountSidW(null, binary, null, ref nameLength, null, ref domainLength, out _);
        if (nameLength == 0 || domainLength == 0)
        {
            return false;
        }

        var name = new char[nameLength];
        var domain = new char[domainLength];
        if (!LookupAccountSidW(null, binary, name, ref nameLength, domain, ref domainLength, out var kind))
        {
            return false;
        }

        use = (SidNameUse)kind;
        return true;
    }

    [System.Runtime.InteropServices.DllImport("advapi32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool LookupAccountSidW(
        string? systemName, byte[] sid, [System.Runtime.InteropServices.Out] char[]? name, ref uint nameLength,
        [System.Runtime.InteropServices.Out] char[]? referencedDomainName, ref uint domainLength, out int use);

    /// <summary>
    /// The security descriptor of the certificate's private key. A CNG key (RSA or ECDSA) is read and written through its
    /// <c>Security Descr</c> property. A legacy CAPI RSA key is a file under <c>Crypto\RSA</c>: its DACL is the file's.
    /// Anything else (a smart-card or HSM key) is refused: it has no file here and no property this can set.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal static IKeyAclTarget OpenKeyTarget(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);

        /* A CNG key's handle belongs to the algorithm object that produced it: disposing the object closes the handle.
           So the target takes the object and closes it itself. */
        var rsa = certificate.GetRSAPrivateKey();
        switch (rsa)
        {
            case RSACng cng:
                return new CngKeyAclTarget(cng.Key, rsa);

            case RSACryptoServiceProvider capi:
                try
                {
                    /* Disposing a provider whose key is not persisted DELETES the key. A certificate's key is persisted,
                       but the flag is set here so that this command can never be the thing that removes one. */
                    capi.PersistKeyInCsp = true;
                    return new CapiFileAclTarget(CapiKeyFile(capi));
                }
                finally
                {
                    rsa.Dispose();
                }
        }

        rsa?.Dispose();
        var ecdsa = certificate.GetECDsaPrivateKey();
        if (ecdsa is ECDsaCng ecCng)
        {
            return new CngKeyAclTarget(ecCng.Key, ecdsa);
        }

        ecdsa?.Dispose();
        throw new InvalidOperationException("the private key is not a CNG key or a legacy CAPI RSA key, so its access list cannot be changed here");
    }

    [SupportedOSPlatform("windows")]
    private static string CapiKeyFile(RSACryptoServiceProvider key)
    {
        var info = key.CspKeyContainerInfo;
        var folder = info.MachineKeyStore
            ? Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "Microsoft", "Crypto", "RSA", "MachineKeys")
            : Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Microsoft", "Crypto", "RSA",
                WindowsIdentity.GetCurrent().User?.Value ?? string.Empty);

        var path = Path.Combine(folder, info.UniqueKeyContainerName);
        return File.Exists(path)
            ? path
            : throw new InvalidOperationException("the legacy CAPI key has no key file on this machine (a smart-card or HSM key?), so its access list cannot be changed here");
    }

    [SupportedOSPlatform("windows")]
    private sealed class CngKeyAclTarget : IKeyAclTarget
    {
        private const string SecurityDescriptorProperty = "Security Descr";

        /* The DACL part of the descriptor (DACL_SECURITY_INFORMATION, 4). A read takes only this:
           NCryptGetProperty rejects the persist flag ("Invalid flags specified"). A write adds Persist
           so the descriptor is stored with the key. */
        private const CngPropertyOptions DaclOnly = (CngPropertyOptions)4;
        private const CngPropertyOptions DaclPersisted = CngPropertyOptions.Persist | DaclOnly;

        private readonly CngKey _key;
        private readonly IDisposable _owner;

        public CngKeyAclTarget(CngKey key, IDisposable owner)
        {
            _key = key;
            _owner = owner;
        }

        public string Kind => "CNG key";

        public byte[] ReadDescriptor()
            => _key.GetProperty(SecurityDescriptorProperty, DaclOnly).GetValue()
                ?? throw new InvalidOperationException("the key reports no security descriptor");

        public void WriteDescriptor(byte[] descriptor)
            => _key.SetProperty(new CngProperty(SecurityDescriptorProperty, descriptor, DaclPersisted));

        public void Dispose()
        {
            _key.Dispose();
            _owner.Dispose();
        }
    }

    [SupportedOSPlatform("windows")]
    private sealed class CapiFileAclTarget : IKeyAclTarget
    {
        private readonly string _path;

        public CapiFileAclTarget(string path) => _path = path;

        public string Kind => "CAPI key file";

        public byte[] ReadDescriptor()
            => new FileInfo(_path).GetAccessControl(AccessControlSections.Access).GetSecurityDescriptorBinaryForm();

        public void WriteDescriptor(byte[] descriptor)
        {
            var security = new FileSecurity();
            security.SetSecurityDescriptorBinaryForm(descriptor, AccessControlSections.Access);
            new FileInfo(_path).SetAccessControl(security);
        }

        public void Dispose()
        {
        }
    }
}
