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
using System.Linq;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7718 Part B: <c>--grant-cert-key-access</c>, the command that gives the service account Read on the private key of
/// a certificate in a Windows certificate store. The rules it must keep are tested where they can be: argument
/// parsing, the refusals (elevation, account, certificate, Deny), the ONE new entry (Read only, the given account,
/// every existing entry kept, in canonical order), idempotency and the re-read verdict run over in-memory security
/// descriptors and self-signed certificates, so no test touches the machine store. One test (Windows only) runs the
/// real grant on a certificate with a persisted key it creates in the current user's store and deletes afterwards.
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class DarlingTlsKeyAccessTests
{
    private static readonly SecurityIdentifier Service = new("S-1-5-80-1111-2222-3333-4444-5555");
    private static readonly SecurityIdentifier Admins = new("S-1-5-32-544");
    private static readonly SecurityIdentifier System_ = new("S-1-5-18");
    private static readonly SecurityIdentifier Inherited = new("S-1-5-32-550");

    private const int FullControl = 0x1F01FF;

    /* ---- the verb ---- */

    [Theory]
    [InlineData("--grant-cert-key-access", true)]
    [InlineData("--GRANT-CERT-KEY-ACCESS", true)]
    [InlineData("--grant-cert-key", false)]
    [InlineData("--harden-files", false)]
    public void IsGrantCertKeyAccessVerb_RecognizesTheVerb_CaseInsensitive(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsGrantCertKeyAccessVerb(arg));
        if (expected)
        {
            /* Reachable: IsKnownVerb is the allow-list Program's dispatch is behind. */
            Assert.True(DarlingCliCommands.IsKnownVerb(arg));
        }
    }

    [Fact]
    public void TheHelpText_ListsTheVerb_AndSaysItNeedsElevation()
    {
        var help = DarlingCliCommands.UsageText();
        var line = help.Split('\n').Single(l => l.Contains("--grant-cert-key-access", StringComparison.Ordinal));

        Assert.Contains("elevated", line, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("--account", line, StringComparison.Ordinal);
    }

    /* ---- PURE: the arguments ---- */

    [Fact]
    public void TryParseArgs_AThumbprintAlone_UsesTheConfigDefaults()
    {
        Assert.True(DarlingTlsKeyAccess.TryParseArgs(new[] { "ab cd 12" }, out var args, out var error));

        Assert.Null(error);
        Assert.Equal("ABCD12", args!.Thumbprint);
        Assert.Null(args.Account);
        Assert.Equal(StoreLocation.LocalMachine, args.Location);
        Assert.Equal("My", args.StoreName);
    }

    [Fact]
    public void TryParseArgs_EveryOption_InAnyOrder()
    {
        Assert.True(DarlingTlsKeyAccess.TryParseArgs(
            new[] { "--store-name", "WebHosting", "--ACCOUNT", @"CORP\svc-darling", "aa11", "--store-location", "currentuser" },
            out var args,
            out _));

        Assert.Equal("AA11", args!.Thumbprint);
        Assert.Equal(@"CORP\svc-darling", args.Account);
        Assert.Equal(StoreLocation.CurrentUser, args.Location);
        Assert.Equal("WebHosting", args.StoreName);
    }

    [Theory]
    [InlineData(new string[0], "thumbprint is required")]
    [InlineData(new[] { "--account", "CORP\\svc" }, "thumbprint is required")]
    [InlineData(new[] { "AB", "--account" }, "--account needs a value")]
    [InlineData(new[] { "AB", "--account", "--store-name", "My" }, "--account needs a value")]
    [InlineData(new[] { "AB", "--store-name" }, "--store-name needs a value")]
    [InlineData(new[] { "AB", "--account", "a", "--account", "b" }, "--account was given twice")]
    [InlineData(new[] { "AB", "--store-location", "LocalMachine", "--store-location", "CurrentUser" }, "--store-location was given twice")]
    [InlineData(new[] { "AB", "CD" }, "Unknown or unexpected argument")]
    [InlineData(new[] { "AB", "--everyone" }, "Unknown or unexpected argument")]
    [InlineData(new[] { "AB", "--store-location", "Machine" }, "is not LocalMachine or CurrentUser")]
    [InlineData(new[] { "AB", "--store-name", " " }, "--store-name is empty")]
    [InlineData(new[] { "AB", "--account", "" }, "--account is empty")]
    [InlineData(new[] { "AB", "--account", "  " }, "--account is empty")]
    [InlineData(new[] { "AB", "--store-name", @"\\host\My" }, "--store-name is a store name, not a path")]
    [InlineData(new[] { "AB", "--store-name", "//host/My" }, "--store-name is a store name, not a path")]
    public void TryParseArgs_Refuses(string[] rest, string expected)
    {
        Assert.False(DarlingTlsKeyAccess.TryParseArgs(rest, out var args, out var error));

        Assert.Null(args);
        Assert.Contains(expected, error, StringComparison.Ordinal);
        Assert.Contains("Usage: --grant-cert-key-access <thumbprint>", error, StringComparison.Ordinal);
    }

    /* ---- PURE: who may be granted ---- */

    [Theory]
    [InlineData("S-1-1-0")]
    [InlineData("S-1-5-32-545")]
    [InlineData("S-1-5-11")]
    [InlineData("S-1-5-4")]
    [InlineData("S-1-5-2")]
    [InlineData("S-1-5-7")]
    [InlineData("S-1-5-32-546")]
    [InlineData("S-1-5-21-111-222-333-513")]
    [InlineData("S-1-5-21-111-222-333-514")]
    [InlineData("S-1-5-21-111-222-333-515")] // Domain Computers
    [InlineData("S-1-5-6")] // SERVICE
    [InlineData("S-1-5-80-0")] // NT SERVICE\ALL SERVICES
    [InlineData("S-1-2-0")] // LOCAL
    [InlineData("S-1-2-1")] // CONSOLE LOGON
    [InlineData("S-1-5-3")] // BATCH
    [InlineData("S-1-5-13")] // TERMINAL SERVER USER
    [InlineData("S-1-5-14")] // REMOTE INTERACTIVE LOGON
    [InlineData("S-1-5-15")] // This Organization
    [InlineData("S-1-5-113")] // Local account
    [InlineData("S-1-15-2-1")] // ALL APPLICATION PACKAGES
    [InlineData("S-1-15-2-2")] // ALL RESTRICTED APPLICATION PACKAGES
    [InlineData("S-1-5-32-554")] // Pre-Windows 2000 Compatible Access
    [InlineData("S-1-5-32-547")] // Power Users
    [InlineData("S-1-5-32-555")] // Remote Desktop Users
    [InlineData("S-1-5-32-568")] // IIS_IUSRS
    [InlineData("S-1-5-32-580")] // Remote Management Users
    public void IsBroadPrincipal_RefusesTheGroupsEveryoneIsIn(string sid)
        => Assert.True(DarlingTlsKeyAccess.IsBroadPrincipal(new SecurityIdentifier(sid)));

    [Theory]
    [InlineData("S-1-5-32-559", (int)DarlingTlsKeyAccess.SidNameUse.Alias)] // Performance Log Users
    [InlineData("S-1-5-21-111-222-333-1105", (int)DarlingTlsKeyAccess.SidNameUse.Group)] // a domain group
    [InlineData("S-1-5-21-111-222-333-1106", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)]
    [InlineData("S-1-5-21-111-222-333-1107", (int)DarlingTlsKeyAccess.SidNameUse.Computer)]
    [InlineData("S-1-5-21-111-222-333-1108", (int)DarlingTlsKeyAccess.SidNameUse.Unknown)]
    [InlineData("S-1-5-21-111-222-333-1109", (int)DarlingTlsKeyAccess.SidNameUse.DeletedAccount)]
    [InlineData("S-1-5-80-0", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)]
    public void RefuseByKind_RefusesEveryGroupAndEveryNonAccount(string sid, int use)
        => Assert.NotNull(DarlingTlsKeyAccess.RefuseByKind(new SecurityIdentifier(sid), (DarlingTlsKeyAccess.SidNameUse)use));

    [Theory]
    [InlineData("S-1-5-21-111-222-333-1105", (int)DarlingTlsKeyAccess.SidNameUse.User)] // a domain user or a gMSA
    [InlineData("S-1-5-18", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)] // LocalSystem
    [InlineData("S-1-5-19", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)] // LocalService
    [InlineData("S-1-5-20", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)] // NetworkService
    [InlineData("S-1-5-80-1111-2222-3333-4444-5555", (int)DarlingTlsKeyAccess.SidNameUse.WellKnownGroup)] // NT SERVICE\name
    [InlineData("S-1-5-82-1111-2222-3333-4444-5555", (int)DarlingTlsKeyAccess.SidNameUse.User)] // an IIS app pool identity
    [InlineData("S-1-5-32-544", (int)DarlingTlsKeyAccess.SidNameUse.Alias)] // Administrators
    public void RefuseByKind_AcceptsAUserAndTheServiceIdentities(string sid, int use)
        => Assert.Null(DarlingTlsKeyAccess.RefuseByKind(new SecurityIdentifier(sid), (DarlingTlsKeyAccess.SidNameUse)use));

    [Theory]
    [InlineData("S-1-5-80-1111-2222-3333-4444-5555")]
    [InlineData("S-1-5-21-111-222-333-1105")]
    [InlineData("S-1-5-18")]
    [InlineData("S-1-5-32-544")]
    [InlineData("S-1-5-32-559")]
    public void IsBroadPrincipal_AllowsAServiceAccountAdministratorsAndSystem(string sid)
        => Assert.False(DarlingTlsKeyAccess.IsBroadPrincipal(new SecurityIdentifier(sid)));

    /* ---- PURE: the one new entry ---- */

    [Fact]
    public void ReadMask_IsReadAndNothingMore()
    {
        Assert.Equal(0x120089, DarlingTlsKeyAccess.ReadMask);
        Assert.Equal(0, DarlingTlsKeyAccess.ReadMask & (int)(FileSystemRights.Write | FileSystemRights.Delete
            | FileSystemRights.ChangePermissions | FileSystemRights.TakeOwnership | FileSystemRights.ExecuteFile));
    }

    [Fact]
    public void PlanGrant_AddsOneReadEntryForTheAccount_AndKeepsEveryExistingEntry_InOrder()
    {
        var descriptor = Descriptor(
            Ace(AceQualifier.AccessDenied, 0x2, Inherited),
            Ace(AceQualifier.AccessAllowed, FullControl, System_),
            Ace(AceQualifier.AccessAllowed, FullControl, Admins),
            Ace(AceQualifier.AccessAllowed, FullControl, Admins, AceFlags.Inherited),
            Ace(AceQualifier.AccessAllowed, 0x120089, Inherited, AceFlags.Inherited));
        var before = Entries(descriptor);

        var plan = DarlingTlsKeyAccess.PlanGrant(descriptor, Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.Added, plan.Outcome);
        var after = Entries(plan.NewDescriptor!);
        Assert.Equal(before.Count + 1, after.Count);

        /* The new entry: allow, Read only, the given account, explicit, after the explicit entries and before the
           inherited ones. */
        var added = Assert.Single(after, e => e.Sid.Equals(Service));
        Assert.Equal(AceQualifier.AccessAllowed, added.Qualifier);
        Assert.Equal(DarlingTlsKeyAccess.ReadMask, added.Mask);
        Assert.Equal(AceFlags.None, added.Flags);
        Assert.Equal(3, after.IndexOf(added));

        /* Every other entry is still there, unchanged, in its old order. */
        Assert.Equal(before, after.Where(e => !e.Sid.Equals(Service)).ToList());
    }

    [Fact]
    public void PlanGrant_NeverReplacesTheAccessList_WhenTheAccountHasOnlyOtherAccess()
    {
        var descriptor = Descriptor(Ace(AceQualifier.AccessAllowed, 0x20089, Service));

        /* Read without Synchronize is not the full Read entry: one more entry is added, the old one stays. */
        var plan = DarlingTlsKeyAccess.PlanGrant(descriptor, Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.Added, plan.Outcome);
        Assert.Equal(new[] { 0x20089, DarlingTlsKeyAccess.ReadMask }, Entries(plan.NewDescriptor!).Select(e => e.Mask).ToArray());
    }

    [Theory]
    [InlineData(0x120089)]
    [InlineData(0x1F01FF)]
    [InlineData(0x10000000)]
    [InlineData(unchecked((int)0x80000000))]
    public void PlanGrant_AnAccountThatAlreadyHasRead_IsAlreadyGranted_AndNothingIsBuilt(int mask)
    {
        var plan = DarlingTlsKeyAccess.PlanGrant(Descriptor(Ace(AceQualifier.AccessAllowed, mask, Service)), Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.AlreadyGranted, plan.Outcome);
        Assert.Null(plan.NewDescriptor);
    }

    [Fact]
    public void PlanGrant_ReadSplitAcrossTwoEntries_CountsAsGranted()
    {
        var plan = DarlingTlsKeyAccess.PlanGrant(
            Descriptor(Ace(AceQualifier.AccessAllowed, 0x20089, Service), Ace(AceQualifier.AccessAllowed, 0x100000, Service)),
            Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.AlreadyGranted, plan.Outcome);
    }

    [Fact]
    public void PlanGrant_AnEntryThatOnlyInheritsToChildren_DoesNotCount()
    {
        var plan = DarlingTlsKeyAccess.PlanGrant(
            Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Service, AceFlags.InheritOnly | AceFlags.ObjectInherit)),
            Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.Added, plan.Outcome);
    }

    [Fact]
    public void PlanGrant_AnExplicitDenyOnTheAccount_BlocksTheGrant()
    {
        var plan = DarlingTlsKeyAccess.PlanGrant(
            Descriptor(Ace(AceQualifier.AccessDenied, 0x1, Service), Ace(AceQualifier.AccessAllowed, FullControl, Service)),
            Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.BlockedByDeny, plan.Outcome);
        Assert.Null(plan.NewDescriptor);
    }

    [Fact]
    public void PlanGrant_ADenyOnSomeoneElse_DoesNotBlock()
    {
        var plan = DarlingTlsKeyAccess.PlanGrant(Descriptor(Ace(AceQualifier.AccessDenied, FullControl, Admins)), Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.Added, plan.Outcome);
    }

    [Fact]
    public void PlanGrant_ANullAccessList_IsLeftAlone()
    {
        /* A NULL DACL means everyone has everything. Adding an entry would take access from everyone else. */
        var raw = new RawSecurityDescriptor(ControlFlags.None, null, null, null, null);
        var bytes = new byte[raw.BinaryLength];
        raw.GetBinaryForm(bytes, 0);

        var plan = DarlingTlsKeyAccess.PlanGrant(bytes, Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.AlreadyGranted, plan.Outcome);
        Assert.Null(plan.NewDescriptor);
        Assert.NotNull(plan.Note);
    }

    [Fact]
    public void PlanGrant_ThenPlanGrantAgain_IsIdempotent()
    {
        var first = DarlingTlsKeyAccess.PlanGrant(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)), Service);
        var second = DarlingTlsKeyAccess.PlanGrant(first.NewDescriptor!, Service);

        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.AlreadyGranted, second.Outcome);
    }

    /* ---- the orchestration ---- */

    [Fact]
    public void Run_Grants_AndSaysWhatItDid_ForWhichAccountAndCertificate_AndThatTheServiceNeedsARestart()
    {
        using var certificate = Make("grant.corp.test");
        var thumbprint = certificate.Thumbprint;
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(thumbprint));

        Assert.Equal(0, code);
        Assert.Equal(1, target.Writes);
        Assert.Equal(DarlingTlsKeyAccess.GrantOutcome.AlreadyGranted, DarlingTlsKeyAccess.PlanGrant(target.Descriptor, Service).Outcome);
        Assert.Empty(rig.Error.ToString());
        var said = rig.Output.ToString();
        Assert.Contains(@"CONTOSO\svc-darling", said, StringComparison.Ordinal);
        Assert.Contains(thumbprint, said, StringComparison.Ordinal);
        Assert.Contains("CN=darling-store-test", said, StringComparison.Ordinal);
        Assert.Contains("LocalMachine\\My", said, StringComparison.Ordinal);
        Assert.Contains("Granted Read", said, StringComparison.Ordinal);
        Assert.Contains("Restart the 'PerformanceMonitor Darling' service", said, StringComparison.Ordinal);
        Assert.True(target.Disposed);
    }

    [Fact]
    public void Run_TheAccountAlreadyHasRead_SaysAlreadyGranted_AndWritesNothing()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, DarlingTlsKeyAccess.ReadMask, Service)));
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(0, code);
        Assert.Equal(0, target.Writes);
        Assert.Contains("Already granted", rig.Output.ToString(), StringComparison.Ordinal);
        Assert.Contains("No change was made", rig.Output.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Restart", rig.Output.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Run_LocalMachineWithoutElevation_SaysSo_AndTouchesNothing()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target) { Elevated = false };

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("elevated prompt", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Contains("Nothing was changed", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, rig.StoreReads);
        Assert.Equal(0, rig.TargetOpens);
        Assert.Equal(0, target.Writes);
    }

    [Fact]
    public void Run_CurrentUser_NeedsNoElevation()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target) { Elevated = false };

        var code = rig.Run(Args(certificate.Thumbprint, StoreLocation.CurrentUser));

        Assert.Equal(0, code);
        Assert.Equal(StoreLocation.CurrentUser, rig.LastLocation);
        Assert.Equal(1, target.Writes);
    }

    [Fact]
    public void Run_NoCertificateWithThatThumbprint_Refuses_AndWritesNothing()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args("0123456789ABCDEF0123456789ABCDEF01234567"));

        Assert.Equal(1, code);
        Assert.Contains("matches no certificate in the LocalMachine\\My store", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, rig.TargetOpens);
        Assert.Equal(0, target.Writes);
    }

    [Fact]
    public void Run_TwoCertificatesWithThatThumbprint_Refuses_AndWritesNothing()
    {
        using var certificate = Make("grant.corp.test");
        using var twin = X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pfx, "x"), "x");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate, twin }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("matches 2 certificates", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, target.Writes);
    }

    [Fact]
    public void Run_ACertificateWithNoPrivateKey_Refuses_AndWritesNothing()
    {
        using var certificate = Make("grant.corp.test", withKey: false);
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("no private key", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, rig.TargetOpens);
        Assert.Equal(0, target.Writes);
    }

    [Theory]
    [InlineData("S-1-1-0")]
    [InlineData("S-1-5-32-545")]
    [InlineData("S-1-5-11")]
    [InlineData("S-1-5-6")]
    [InlineData("S-1-5-80-0")]
    [InlineData("S-1-2-0")]
    [InlineData("S-1-5-32-547")]
    [InlineData("S-1-5-21-111-222-333-515")]
    public void Run_EveryoneUsersAndAuthenticatedUsers_AreRefused_WhateverTheOperatorTyped(string sid)
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target) { Account = new SecurityIdentifier(sid), AccountDisplay = "Everyone-ish" };

        var code = rig.Run(Args(certificate.Thumbprint, account: "anything"));

        Assert.Equal(1, code);
        Assert.Contains("stands for many accounts", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, rig.StoreReads);
        Assert.Equal(0, target.Writes);
    }

    [Theory]
    [InlineData("S-1-5-19")] // LocalService
    [InlineData("S-1-5-18")] // LocalSystem
    [InlineData("S-1-5-20")] // NetworkService
    public void Run_AServiceIdentity_IsGranted_ThroughTheRealAccountLookup(string sid)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var rig = RunWithTheRealLookup(sid, out var target, out var certificate);
        using (certificate)
        {
            Assert.Equal(0, rig.Code);
            Assert.Equal(1, target.Writes);
        }
    }

    [Theory]
    [InlineData("S-1-5-32-559")] // Performance Log Users: a BUILTIN alias the SID list does not name
    [InlineData("S-1-5-32-558")] // Performance Monitor Users
    [InlineData("S-1-5-6")] // SERVICE
    [InlineData("S-1-5-80-0")] // NT SERVICE\ALL SERVICES
    [InlineData("S-1-2-0")] // LOCAL
    [InlineData("S-1-5-32-547")] // Power Users
    [InlineData("S-1-5-21-111-222-333-1105")] // a domain SID the machine cannot resolve: fail closed
    public void Run_AGroupOrAnAccountThatCannotBeTyped_IsRefused_WithNoWrites_ThroughTheRealAccountLookup(string sid)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var rig = RunWithTheRealLookup(sid, out var target, out var certificate);
        using (certificate)
        {
            Assert.Equal(1, rig.Code);
            Assert.Equal(0, target.Writes);
            Assert.Equal(0, rig.StoreReads);
            Assert.Contains("Nothing was changed", rig.Error.ToString(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Run_AGroup_NamesTheAccount_AndSaysTheCommandGrantsToOneAccount()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var rig = RunWithTheRealLookup("S-1-5-32-559", out _, out var certificate);
        using (certificate)
        {
            var error = rig.Error.ToString();
            Assert.Contains("is a group", error, StringComparison.Ordinal);
            Assert.Contains("grants to one account", error, StringComparison.Ordinal);
        }
    }

    /// <summary>A mistyped NT SERVICE name resolves to a SID Windows makes from the name, so only the service registry can
    /// refuse it (#7718): with no service by that name the command refuses, names the account and writes nothing.</summary>
    [Theory]
    [InlineData(@"NT SERVICE\EventLog")]
    [InlineData("typed-as-sid")]
    public void Run_APerServiceAccountWithNoRegisteredService_IsRefused_WithNoWrites(string account)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var typed = account == "typed-as-sid" ? DarlingTlsKeyAccess.ServiceSid("EventLog").Value : account;
        var rig = RunWithTheRealLookup(typed, out var target, out var certificate, new[] { "Spooler", "PerformanceMonitor" });
        using (certificate)
        {
            Assert.Equal(1, rig.Code);
            Assert.Equal(0, target.Writes);
            Assert.Equal(0, rig.StoreReads);
            Assert.Contains(typed, rig.Error.ToString(), StringComparison.Ordinal);
            Assert.Contains("no service with that name is registered", rig.Error.ToString(), StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(@"NT SERVICE\EventLog")]
    [InlineData(@"NT SERVICE\eventlog")]
    [InlineData("typed-as-sid")]
    public void Run_APerServiceAccountOfARegisteredService_IsStillGranted(string account)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var typed = account == "typed-as-sid" ? DarlingTlsKeyAccess.ServiceSid("EventLog").Value : account;
        var rig = RunWithTheRealLookup(typed, out var target, out var certificate, new[] { "Spooler", "EventLog" });
        using (certificate)
        {
            Assert.Equal(0, rig.Code);
            Assert.Equal(1, target.Writes);
        }
    }

    /// <summary>The SID made from a service name is what Windows makes: the check is only as right as this.</summary>
    [Theory]
    [InlineData("Spooler")]
    [InlineData("EventLog")]
    public void ServiceSid_IsTheSidWindowsMakesForTheName(string service)
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "The account lookup is Windows only.");

        var windows = (SecurityIdentifier)new NTAccount(@"NT SERVICE\" + service).Translate(typeof(SecurityIdentifier));

        Assert.Equal(windows, DarlingTlsKeyAccess.ServiceSid(service));
    }

    [SupportedOSPlatform("windows")]
    private static (int Code, int StoreReads, StringWriter Error) RunWithTheRealLookup(
        string sid, out FakeTarget target, out X509Certificate2 certificate, string[]? registeredServices = null)
    {
        var made = Make("grant.corp.test");
        var fake = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        certificate = made;
        target = fake;
        var reads = 0;
        var error = new StringWriter();
        var seams = new DarlingTlsKeyAccess.GrantSeams(
            () => true,
            (_, _) =>
            {
                reads++;
                return new[] { made };
            },
            name => DarlingTlsKeyAccess.ResolveAccount(name, "no such service here", () => registeredServices ?? Array.Empty<string>()),
            _ => fake);

        var code = DarlingTlsKeyAccess.Run(Args(made.Thumbprint, account: sid), seams, new StringWriter(), error);
        return (code, reads, error);
    }

    [Fact]
    public void Run_AnAccountThatCannotBeResolved_IsRefused_WithTheReason()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target) { Account = null, AccountError = "the service is not registered" };

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("the service is not registered", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, target.Writes);
    }

    [Fact]
    public void Run_TheAccountNamedOnTheCommandLine_IsWhatTheResolverIsAsked()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins)));
        var rig = new Rig(new[] { certificate }, target);

        rig.Run(Args(certificate.Thumbprint, account: @"CORP\svc-other"));

        Assert.Equal(@"CORP\svc-other", rig.AccountAsked);
    }

    [Fact]
    public void Run_AnExplicitDeny_RefusesAndWritesNothing()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessDenied, 0x1, Service)));
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("explicit Deny", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Equal(0, target.Writes);
    }

    [Fact]
    public void Run_AWriteThatDoesNotTake_IsNotReportedAsGranted()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins))) { IgnoreWrites = true };
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Equal(1, target.Writes);
        Assert.DoesNotContain("Granted Read", rig.Output.ToString(), StringComparison.Ordinal);
        Assert.Contains("does not have Read", rig.Error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Run_AWriteThatThrows_IsAFailure_NamingTheCause()
    {
        using var certificate = Make("grant.corp.test");
        var target = new FakeTarget(Descriptor(Ace(AceQualifier.AccessAllowed, FullControl, Admins))) { WriteFails = true };
        var rig = new Rig(new[] { certificate }, target);

        var code = rig.Run(Args(certificate.Thumbprint));

        Assert.Equal(1, code);
        Assert.Contains("could not be changed (Access is denied)", rig.Error.ToString(), StringComparison.Ordinal);
        Assert.Contains("The access list was not changed", rig.Error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Run_AStoreThatCannotBeOpened_IsNamed()
    {
        var rig = new Rig(Array.Empty<X509Certificate2>(), new FakeTarget(Descriptor())) { ReadFails = true };

        var code = rig.Run(Args("AB"));

        Assert.Equal(1, code);
        Assert.Contains("the LocalMachine\\My certificate store could not be opened (no such store)", rig.Error.ToString(), StringComparison.Ordinal);
    }

    /* ---- the loader's Critical line points at the command ---- */

    [Theory]
    [InlineData(StoreLocation.LocalMachine, "My", "")]
    [InlineData(StoreLocation.CurrentUser, "My", " --store-location CurrentUser")]
    [InlineData(StoreLocation.LocalMachine, "WebHosting", " --store-name \"WebHosting\"")]
    public void TheUnreadableKeyLine_NamesTheCommand_WithTheAccountThatFailed(StoreLocation location, string storeName, string options)
    {
        using var certificate = Make("grant.corp.test");
        var thumbprint = certificate.Thumbprint;
        var all = new List<X509Certificate2> { certificate };

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingTlsStore.Load(
            new WebTlsStoreConfig { Location = location.ToString(), Name = storeName, Thumbprint = thumbprint },
            "web",
            (_, _) => all,
            _ => new CryptographicException("Keyset does not exist"),
            () => @"NT SERVICE\PerformanceMonitor Darling",
            DateTimeOffset.UtcNow));

        Assert.Contains(
            $"PerformanceMonitor.Darling.Service.exe --grant-cert-key-access {thumbprint}{options} --account \"NT SERVICE\\PerformanceMonitor Darling\"",
            ex.Message,
            StringComparison.Ordinal);
        Assert.Contains("then restart the service", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCommandInTheCriticalLine_ParsesBackToTheSameRequest()
    {
        var command = DarlingTlsKeyAccess.GrantCommandFor("ab cd", StoreLocation.CurrentUser, "WebHosting", @"CORP\svc");
        var rest = command.Split(' ').SkipWhile(part => part != DarlingTlsKeyAccess.VerbName).Skip(1)
            .Select(part => part.Trim('"')).ToArray();

        Assert.True(DarlingTlsKeyAccess.TryParseArgs(rest, out var args, out _));

        Assert.Equal("ABCD", args!.Thumbprint);
        Assert.Equal(StoreLocation.CurrentUser, args.Location);
        Assert.Equal("WebHosting", args.StoreName);
        Assert.Equal(@"CORP\svc", args.Account);
    }

    /* ---- the shipped path: a real persisted key in the current user's store (Windows only) ---- */

    [Fact]
    public void RealGrant_OnACurrentUserKey_AddsOneReadEntryAndKeepsTheRest_ThenSaysAlreadyGranted()
    {
        Assert.SkipWhen(!OperatingSystem.IsWindows(), "Certificate keys and their ACLs are Windows only.");

        const string account = "S-1-5-19"; // LocalService: present on every Windows, one account, never a group
        using var installed = new InstalledCertificate(Make($"darling-7718-grant-{Guid.NewGuid():N}.corp.test"));
        var thumbprint = installed.Certificate.Thumbprint;

        byte[] ReadAcl()
        {
            using var key = DarlingTlsKeyAccess.OpenKeyTarget(installed.Certificate);
            return key.ReadDescriptor();
        }

        var before = Entries(ReadAcl());
        Assert.NotEmpty(before);
        Assert.DoesNotContain(before, e => e.Sid.Value == account);

        DarlingTlsKeyAccess.GrantSeams Seams() => new(
            () => false,
            DarlingTlsStore.ReadStore,
            name => DarlingTlsKeyAccess.ResolveAccount(name, "no such service here"),
            certificate => DarlingTlsKeyAccess.OpenKeyTarget(certificate));

        var args = new DarlingTlsKeyAccess.GrantArgs(thumbprint, account, StoreLocation.CurrentUser, "My");

        var first = new StringWriter();
        var firstError = new StringWriter();
        var code = DarlingTlsKeyAccess.Run(args, Seams(), first, firstError);
        Assert.True(code == 0, firstError.ToString());
        Assert.Contains("Granted Read", first.ToString(), StringComparison.Ordinal);

        var after = Entries(ReadAcl());
        Assert.Equal(before.Count + 1, after.Count);
        var added = Assert.Single(after, e => e.Sid.Value == account);
        Assert.Equal(AceQualifier.AccessAllowed, added.Qualifier);
        /* The key store reads the entry back with GENERIC_READ (0x80000000) added to the FILE_GENERIC_READ bits written
           (CI saw 0x80120089 for 0x120089). It is the same Read grant, so compare the file rights alone. */
        Assert.Equal(DarlingTlsKeyAccess.ReadMask, added.Mask & ~unchecked((int)0x80000000));
        var kept = after.Where(e => e.Sid.Value != account).ToList();
        Assert.Equal(before.Count, kept.Count);
        Assert.All(before, e => Assert.Contains(e, kept));

        var second = new StringWriter();
        var secondError = new StringWriter();
        var again = DarlingTlsKeyAccess.Run(args, Seams(), second, secondError);
        Assert.True(again == 0, secondError.ToString());
        Assert.Contains("Already granted", second.ToString(), StringComparison.Ordinal);
        Assert.Equal(after.Count, Entries(ReadAcl()).Count);
    }

    /* ---- helpers ---- */

    private static DarlingTlsKeyAccess.GrantArgs Args(
        string thumbprint, StoreLocation location = StoreLocation.LocalMachine, string? account = null)
        => new(thumbprint, account, location, "My");

    private sealed record Entry(AceQualifier Qualifier, int Mask, SecurityIdentifier Sid, AceFlags Flags);

    private static GenericAce Ace(AceQualifier qualifier, int mask, SecurityIdentifier sid, AceFlags flags = AceFlags.None)
        => new CommonAce(flags, qualifier, mask, sid, false, null);

    private static byte[] Descriptor(params GenericAce[] aces)
    {
        var acl = new RawAcl(2, aces.Length);
        for (var i = 0; i < aces.Length; i++)
        {
            acl.InsertAce(i, aces[i]);
        }

        var raw = new RawSecurityDescriptor(ControlFlags.DiscretionaryAclPresent, null, null, null, acl);
        var bytes = new byte[raw.BinaryLength];
        raw.GetBinaryForm(bytes, 0);
        return bytes;
    }

    private static List<Entry> Entries(byte[] descriptor)
    {
        var acl = new RawSecurityDescriptor(descriptor, 0).DiscretionaryAcl!;
        var list = new List<Entry>();
        for (var i = 0; i < acl.Count; i++)
        {
            var ace = (CommonAce)acl[i];
            list.Add(new Entry(ace.AceQualifier, ace.AccessMask, ace.SecurityIdentifier, ace.AceFlags));
        }

        return list;
    }

    private static X509Certificate2 Make(string dnsName, bool withKey = true)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=darling-store-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(dnsName);
        request.CertificateExtensions.Add(san.Build());

        var created = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-5), DateTimeOffset.UtcNow.AddDays(90));
        if (withKey)
        {
            return created;
        }

        using (created)
        {
            return X509CertificateLoader.LoadCertificate(created.Export(X509ContentType.Cert));
        }
    }

    /// <summary>An in-memory key: a descriptor that records writes.</summary>
    private sealed class FakeTarget : DarlingTlsKeyAccess.IKeyAclTarget
    {
        public FakeTarget(byte[] descriptor) => Descriptor = descriptor;

        public byte[] Descriptor { get; private set; }

        public int Writes { get; private set; }

        public bool Disposed { get; private set; }

        public bool IgnoreWrites { get; init; }

        public bool WriteFails { get; init; }

        public string Kind => "CNG key";

        public byte[] ReadDescriptor() => Descriptor;

        public void WriteDescriptor(byte[] descriptor)
        {
            Writes++;
            if (WriteFails)
            {
                throw new UnauthorizedAccessException("Access is denied");
            }

            if (!IgnoreWrites)
            {
                Descriptor = descriptor;
            }
        }

        public void Dispose() => Disposed = true;
    }

    /// <summary>The seams of one run, counting what the run did to them.</summary>
    private sealed class Rig
    {
        private readonly IReadOnlyList<X509Certificate2> _store;
        private readonly FakeTarget _target;

        public Rig(IReadOnlyList<X509Certificate2> store, FakeTarget target)
        {
            _store = store;
            _target = target;
        }

        public bool Elevated { get; init; } = true;

        public SecurityIdentifier? Account { get; init; } = Service;

        public string AccountDisplay { get; init; } = @"CONTOSO\svc-darling";

        public string? AccountError { get; init; }

        public bool ReadFails { get; init; }

        public string? AccountAsked { get; private set; }

        public StoreLocation? LastLocation { get; private set; }

        public int StoreReads { get; private set; }

        public int TargetOpens { get; private set; }

        public StringWriter Output { get; } = new();

        public StringWriter Error { get; } = new();

        public int Run(DarlingTlsKeyAccess.GrantArgs args)
        {
            var seams = new DarlingTlsKeyAccess.GrantSeams(
                () => Elevated,
                (location, _) =>
                {
                    StoreReads++;
                    LastLocation = location;
                    if (ReadFails)
                    {
                        throw new CryptographicException("no such store");
                    }

                    /* Run disposes what it reads, as the real store read hands out copies, so this hands out copies. */
                    return _store.Select(c => new X509Certificate2(c)).ToList();
                },
                name =>
                {
                    AccountAsked = name;
                    return new DarlingTlsKeyAccess.AccountResolution(Account, AccountDisplay, AccountError);
                },
                _ =>
                {
                    TargetOpens++;
                    return _target;
                });

            return DarlingTlsKeyAccess.Run(args, seams, Output, Error);
        }
    }

    /// <summary>A certificate with a persisted key in the current user's personal store, removed with its key on dispose.</summary>
    [SupportedOSPlatform("windows")]
    private sealed class InstalledCertificate : IDisposable
    {
        public InstalledCertificate(X509Certificate2 generated)
        {
            using (generated)
            {
                var pfx = generated.Export(X509ContentType.Pfx, "x");
                Certificate = X509CertificateLoader.LoadPkcs12(
                    pfx, "x", X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.UserKeySet);
            }

            using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
            store.Open(OpenFlags.ReadWrite);
            store.Add(Certificate);
        }

        public X509Certificate2 Certificate { get; }

        public void Dispose()
        {
            try
            {
                using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
                store.Open(OpenFlags.ReadWrite);
                store.Remove(Certificate);
                using var key = Certificate.GetRSAPrivateKey();
                if (key is RSACng cng)
                {
                    cng.Key.Delete();
                }
            }
            catch (CryptographicException)
            {
                /* Cleanup only: a key that is already gone is the state we wanted. */
            }
            finally
            {
                Certificate.Dispose();
            }
        }
    }
}
