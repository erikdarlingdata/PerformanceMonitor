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
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3914: the store login the web dashboard and the MCP server connect with outside managed mode. They used to
/// connect as the owner there, so a web session or an MCP token-holder had none of the <c>viewer</c>/<c>mcp</c>
/// roles' protections. The ruling: the compose distribution's own store gets the roles the service provisions,
/// any other store gets <c>postgres.webConnectionString</c> / <c>postgres.mcpConnectionString</c>, and a surface
/// left on the owner says at startup what that gives up.
///
/// <para>Pins the pure decision (<see cref="DarlingStoreLogins.Resolve"/>), including a start that did not provision
/// the roles, the warning text, the derived role login, the credential an earlier start left and the checks it has
/// to pass, the compose-store refusals, the batch the compose store gets, the reload gate, the two settings, and —
/// by source, because a behavioural test of a pure function cannot see it — the wiring: that each host's
/// unmanaged pool is built from the resolver and its managed pool exactly as before, and that the worker
/// provisions the compose store and seeds the reload baseline from what it wrote. The live half is
/// <see cref="ComposeStoreRolesLiveTests"/>.</para>
///
/// <para>In the <c>darling-config-env</c> collection because one test resets and publishes the process-wide
/// compose-store verdict, which the live tests in that collection also publish.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingStoreLoginsTests
{
    private const string Owner = "Host=store;Port=5432;Username=darling;Password=OwnerSecret1;Database=darling";
    private const string ViewerLogin = "Host=store;Port=5432;Username=viewer;Password=ViewerSecret2;Database=darling";
    private const string McpLogin = "Host=store;Port=5432;Username=mcp;Password=McpSecret3;Database=darling";

    /* ─────────────────────────── the decision ─────────────────────────── */

    [Fact]
    public void AConfiguredLogin_Wins_InOrOutOfAContainer()
    {
        var outside = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, ViewerLogin, inContainer: false, verdict: null);
        Assert.Equal(DarlingStoreLogins.LoginSource.Configured, outside.Source);
        Assert.Equal(ViewerLogin, outside.ConnectionString);
        Assert.Null(outside.Warning);

        /* Even over a provisioned compose store: the operator's explicit login is the operator's call. */
        var inside = DarlingStoreLogins.Resolve(
            DarlingStoreLogins.Surface.Mcp, Owner, McpLogin, inContainer: true, Provisioned());
        Assert.Equal(DarlingStoreLogins.LoginSource.Configured, inside.Source);
        Assert.Equal(McpLogin, inside.ConnectionString);
    }

    [Fact]
    public void InAContainer_TheComposeStoresProvisionedRole_IsEachSurfacesLogin()
    {
        var verdict = Provisioned();

        var web = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, null, inContainer: true, verdict);
        Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRole, web.Source);
        Assert.Equal(ViewerLogin, web.ConnectionString);
        Assert.Null(web.Warning);

        var mcp = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Mcp, Owner, null, inContainer: true, verdict);
        Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRole, mcp.Source);
        Assert.Equal(McpLogin, mcp.ConnectionString);
    }

    [Fact]
    public void OutsideAContainer_ASurfaceWithNoLoginOfItsOwn_IsTheOwner_AndSaysSo()
    {
        /* A verdict outside a container can only be stale process state; the container is what makes the store
           the compose one, so it is not consulted. */
        var login = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, null, inContainer: false, Provisioned());

        Assert.Equal(DarlingStoreLogins.LoginSource.Owner, login.Source);
        Assert.Equal(Owner, login.ConnectionString);
        Assert.Equal(DarlingStoreLogins.OwnerFallbackWarning(DarlingStoreLogins.Surface.Web, null), login.Warning);
    }

    [Fact]
    public void ARefusedOrFailedComposeStore_FallsBackToTheOwner_WithItsReason()
    {
        const string reason = "The service logs in to this store as 'someone', which is not the store's bootstrap superuser.";
        var login = DarlingStoreLogins.Resolve(
            DarlingStoreLogins.Surface.Mcp, Owner, null, inContainer: true, DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(reason));

        Assert.Equal(DarlingStoreLogins.LoginSource.Owner, login.Source);
        Assert.Equal(Owner, login.ConnectionString);
        Assert.NotNull(login.Warning);
        Assert.StartsWith(DarlingStoreLogins.OwnerFallbackWarning(DarlingStoreLogins.Surface.Mcp, null), login.Warning, StringComparison.Ordinal);
        Assert.EndsWith(reason, login.Warning, StringComparison.Ordinal);
    }

    /// <summary>
    /// The decision table for a start that did NOT provision the compose store's roles (#3914 review, F1). Every such
    /// outcome — a refusal, a failure, a collector that stood down first — keeps each surface on its role when the
    /// service holds a trusted credential from an earlier start: the role still carries that password, and one bad
    /// start must not hand the web dashboard and the MCP server the owner for the rest of the process. The login is
    /// the same derived one a provisioned start builds, and the warning says it is an earlier start's.
    /// </summary>
    [Theory]
    [InlineData("refusal", "Web", "viewer")]
    [InlineData("refusal", "Mcp", "mcp")]
    [InlineData("failure", "Web", "viewer")]
    [InlineData("failure", "Mcp", "mcp")]
    [InlineData("stand-down", "Web", "viewer")]
    [InlineData("stand-down", "Mcp", "mcp")]
    public void ANotProvisionedStart_WithATrustedEarlierCredential_KeepsTheSurfaceOnItsRole(string outcome, string surfaceName, string role)
    {
        var surface = Enum.Parse<DarlingStoreLogins.Surface>(surfaceName);
        var verdict = NotProvisioned(outcome);

        var login = DarlingStoreLogins.Resolve(
            surface, Owner, null, inContainer: true, verdict, EarlierComposeCredential.Trusted("Earlier3914"));

        Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRoleFromEarlierStart, login.Source);
        Assert.Equal(DarlingStoreLogins.BuildComposeStoreRoleConnectionString(Owner, role, "Earlier3914"), login.ConnectionString);
        Assert.Equal(role, new NpgsqlConnectionStringBuilder(login.ConnectionString).Username);
        Assert.DoesNotContain("OwnerSecret1", login.ConnectionString, StringComparison.Ordinal);

        Assert.Equal(DarlingStoreLogins.EarlierCredentialWarning(surface, verdict.NotProvisionedReason), login.Warning);
        Assert.Contains(verdict.NotProvisionedReason!, login.Warning, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same starts with no trusted credential from an earlier one — none written yet, or one the checks turned
    /// away — are the only ones that fall back to the owner, and the warning gives both reasons: why this start did
    /// not provision, and why no earlier credential could stand in.
    /// </summary>
    [Theory]
    [InlineData("refusal")]
    [InlineData("failure")]
    [InlineData("stand-down")]
    public void ANotProvisionedStart_WithNoTrustedEarlierCredential_FallsBackToTheOwner_WithBothReasons(string outcome)
    {
        var verdict = NotProvisioned(outcome);
        const string missing = "/var/lib/darling/credentials/pg-viewer-credential is not trusted (its mode 0644 gives other users access)";

        var login = DarlingStoreLogins.Resolve(
            DarlingStoreLogins.Surface.Web, Owner, null, inContainer: true, verdict, EarlierComposeCredential.None(missing));

        Assert.Equal(DarlingStoreLogins.LoginSource.Owner, login.Source);
        Assert.Equal(Owner, login.ConnectionString);
        Assert.StartsWith(DarlingStoreLogins.OwnerFallbackWarning(DarlingStoreLogins.Surface.Web, null), login.Warning, StringComparison.Ordinal);
        Assert.Contains(verdict.NotProvisionedReason!, login.Warning, StringComparison.Ordinal);
        Assert.EndsWith($" No viewer credential from an earlier start can stand in: {missing}.", login.Warning, StringComparison.Ordinal);
    }

    /// <summary>An earlier start's credential is the fallback of a start that did not provision, and nothing more: a
    /// configured login and this start's own provisioning both outrank it, and outside a container it is never
    /// consulted.</summary>
    [Fact]
    public void AnEarlierCredential_NeverOutranksAConfiguredLogin_ThisStartsProvisioning_OrTheOutsideOfAContainer()
    {
        var earlier = EarlierComposeCredential.Trusted("Earlier3914");

        var configured = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, ViewerLogin, inContainer: true, NotProvisioned("refusal"), earlier);
        Assert.Equal(DarlingStoreLogins.LoginSource.Configured, configured.Source);
        Assert.Equal(ViewerLogin, configured.ConnectionString);

        var provisioned = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, null, inContainer: true, Provisioned(), earlier);
        Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRole, provisioned.Source);
        Assert.Equal(ViewerLogin, provisioned.ConnectionString);

        var outside = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, null, inContainer: false, NotProvisioned("refusal"), earlier);
        Assert.Equal(DarlingStoreLogins.LoginSource.Owner, outside.Source);
        Assert.Equal(DarlingStoreLogins.OwnerFallbackWarning(DarlingStoreLogins.Surface.Web, null), outside.Warning);
    }

    /// <summary>
    /// An operator reading the log can tell the three cases apart (#3914 review, F1): provisioned this start (no
    /// warning from the surface; the worker says it provisioned), an earlier start's credential, and the owner.
    /// </summary>
    [Theory]
    [InlineData("Web", "The web dashboard", "viewer")]
    [InlineData("Mcp", "The MCP server", "mcp")]
    public void TheThreeCases_ReadDifferently(string surfaceName, string what, string role)
    {
        var surface = Enum.Parse<DarlingStoreLogins.Surface>(surfaceName);
        const string reason = "The collector stopped before it could provision the store's roles (Store: connection refused).";

        Assert.Null(DarlingStoreLogins.Resolve(surface, Owner, null, inContainer: true, Provisioned()).Warning);

        var earlier = DarlingStoreLogins.EarlierCredentialWarning(surface, reason);
        Assert.StartsWith(
            $"{what} connects to the store as the {role} role with the credential an earlier start provisioned, not as the owner: "
            + "the store's least-privilege roles were not provisioned this start. " + reason,
            earlier, StringComparison.Ordinal);
        Assert.EndsWith(
            $"If the store refuses that login (the {role} role was dropped, or its password changed by hand), {char.ToLowerInvariant(what[0])}{what[1..]} "
            + "stays down with the store's error until a start provisions the roles again; it never falls back to the owner login.",
            earlier, StringComparison.Ordinal);

        var owner = DarlingStoreLogins.OwnerFallbackWarning(surface, reason);
        Assert.StartsWith($"{what} connects to the store as the owner login (postgres.connectionString)", owner, StringComparison.Ordinal);
        Assert.DoesNotContain("earlier start", owner, StringComparison.Ordinal);
        Assert.DoesNotContain("owner login (postgres.connectionString)", earlier, StringComparison.Ordinal);
    }

    /// <summary>The ruling's warning: the surface, why, and the three things the owner login gives up.</summary>
    [Theory]
    [InlineData("Web", "The web dashboard", "postgres.webConnectionString", "viewer")]
    [InlineData("Mcp", "The MCP server", "postgres.mcpConnectionString", "mcp")]
    public void TheOwnerFallbackWarning_NamesWhatTheOwnerLoginGivesUp(
        string surface, string what, string setting, string role)
    {
        var warning = DarlingStoreLogins.OwnerFallbackWarning(Enum.Parse<DarlingStoreLogins.Surface>(surface), null);

        Assert.StartsWith(what, warning, StringComparison.Ordinal);
        Assert.Contains($"because {setting} is not set", warning, StringComparison.Ordinal);
        Assert.Contains("the secret-column carve", warning, StringComparison.Ordinal);
        Assert.Contains("the narrow write grants", warning, StringComparison.Ordinal);
        Assert.Contains("the statement_timeout backstop", warning, StringComparison.Ordinal);
        Assert.Contains($"Set {setting} to the {role} role tools/provision-roles.sql creates.", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void AConfiguredLogin_ThatIsTheOwnerAfterAll_IsWarnedAbout()
    {
        const string sameOwner = "Host=store;Port=5432;Username=darling;Password=OwnerSecret1;Database=darling;Application Name=web";
        var login = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, sameOwner, inContainer: false, verdict: null);

        Assert.Equal(DarlingStoreLogins.LoginSource.Configured, login.Source);
        Assert.NotNull(login.Warning);
        Assert.Contains("postgres.webConnectionString logs in as 'darling', the owner login", login.Warning, StringComparison.Ordinal);
        Assert.Contains("so the web dashboard still gives up", login.Warning, StringComparison.Ordinal);
        Assert.Contains("the secret-column carve, the narrow write grants and the statement_timeout backstop", login.Warning, StringComparison.Ordinal);
    }

    [Fact]
    public void AConfiguredLogin_NamingARoleTheComposeStoreProvisions_IsWarnedAbout_OnlyThere()
    {
        var onCompose = DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, ViewerLogin, inContainer: true, Provisioned());
        Assert.NotNull(onCompose.Warning);
        Assert.Contains("names the 'viewer' role, which the service provisions on this compose store and re-keys", onCompose.Warning, StringComparison.Ordinal);
        Assert.Contains("and the web dashboard connects as the viewer role the service provisions.", onCompose.Warning, StringComparison.Ordinal);

        /* On any other store the same login is exactly what the operator should configure. */
        Assert.Null(DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, ViewerLogin, inContainer: false, Provisioned()).Warning);
        Assert.Null(DarlingStoreLogins.Resolve(
            DarlingStoreLogins.Surface.Web, Owner, ViewerLogin, inContainer: true, DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("no")).Warning);
    }

    [Fact]
    public void AMalformedConfiguredLogin_GetsNoGuess()
    {
        /* Npgsql's own error at the first connect names the problem; the resolver does not throw over it. */
        Assert.Null(DarlingStoreLogins.ConfiguredLoginWarning(
            DarlingStoreLogins.Surface.Web, "this is not = a; connection string =", Owner, verdict: null));
    }

    /* ─────────────────────────── the derived role login ─────────────────────────── */

    [Fact]
    public void TheComposeStoreRoleLogin_InheritsWhereTheStoreIs_AndNothingThatIsTheOwners()
    {
        const string owner =
            "Host=store;Port=6543;Username=darling;Password=OwnerSecret1;Database=darling2;SSL Mode=VerifyFull;" +
            "Root Certificate=/run/secrets/ca.pem;Timeout=33;Options=-c statement_timeout=0;Passfile=/root/.pgpass;" +
            "SSL Certificate=/run/secrets/client.pem;SSL Key=/run/secrets/client.key;Maximum Pool Size=100;Search Path=public";

        var derived = DarlingStoreLogins.BuildComposeStoreRoleConnectionString(owner, "viewer", "ViewerSecret2");
        var builder = new NpgsqlConnectionStringBuilder(derived);

        Assert.Equal("store", builder.Host);
        Assert.Equal(6543, builder.Port);
        Assert.Equal("darling2", builder.Database);
        Assert.Equal(SslMode.VerifyFull, builder.SslMode);
        Assert.Equal("/run/secrets/ca.pem", builder.RootCertificate);
        Assert.Equal(33, builder.Timeout);

        Assert.Equal("viewer", builder.Username);
        Assert.Equal("ViewerSecret2", builder.Password);
        Assert.Equal(DarlingManagedPostgres.SearchPath, builder.SearchPath);
        Assert.Equal(DarlingStoreLogins.RoleLoginMaxPoolSize, builder.MaxPoolSize);

        /* Options is the dangerous one: a -c statement_timeout=0 there switches off the role's backstop. */
        Assert.True(string.IsNullOrEmpty(builder.Options), builder.Options);
        Assert.True(string.IsNullOrEmpty(builder.Passfile), builder.Passfile);
        Assert.True(string.IsNullOrEmpty(builder.SslCertificate), builder.SslCertificate);
        Assert.True(string.IsNullOrEmpty(builder.SslKey), builder.SslKey);
        Assert.DoesNotContain("OwnerSecret1", derived, StringComparison.Ordinal);
    }

    /// <summary>
    /// The derived login keeps every connection setting of the owner's that can only make a connection stricter
    /// (#3914 review, F7): channel binding, the authentication methods it will accept, certificate revocation
    /// checking and the TLS negotiation. An owner that requires channel binding or SCRAM must not hand its roles a
    /// connection that settles for less. An owner that sets none of them hands the role Npgsql's defaults.
    /// </summary>
    [Fact]
    public void TheComposeStoreRoleLogin_KeepsTheOwnersStricterConnectionSettings()
    {
        var owner = new NpgsqlConnectionStringBuilder(Owner)
        {
            SslMode = SslMode.VerifyFull,
            ChannelBinding = ChannelBinding.Require,
            RequireAuth = "ScramSHA256",
            CheckCertificateRevocation = true,
            SslNegotiation = SslNegotiation.Direct,
        }.ConnectionString;

        var derived = new NpgsqlConnectionStringBuilder(DarlingStoreLogins.BuildComposeStoreRoleConnectionString(owner, "mcp", "McpSecret3"));

        Assert.Equal(ChannelBinding.Require, derived.ChannelBinding);
        Assert.Equal("ScramSHA256", derived.RequireAuth);
        Assert.True(derived.CheckCertificateRevocation);
        Assert.Equal(SslNegotiation.Direct, derived.SslNegotiation);
        Assert.Equal("mcp", derived.Username);

        var plain = new NpgsqlConnectionStringBuilder(DarlingStoreLogins.BuildComposeStoreRoleConnectionString(Owner, "mcp", "McpSecret3"));
        var defaults = new NpgsqlConnectionStringBuilder();
        Assert.Equal(defaults.ChannelBinding, plain.ChannelBinding);
        Assert.Equal(defaults.RequireAuth, plain.RequireAuth);
        Assert.Equal(defaults.CheckCertificateRevocation, plain.CheckCertificateRevocation);
        Assert.Equal(defaults.SslNegotiation, plain.SslNegotiation);
    }

    /* ─────────────────────────── the verdict seam ─────────────────────────── */

    [Fact]
    public void TheVerdict_IsSettledByAStandDown_AndNeverOverwrittenByOne()
    {
        try
        {
            /* A collector that stops before provisioning must not leave a container host waiting forever.
               #4316 round 1 B1: PublishStopped no longer takes free text, so the reason it settles carries
               the step's fixed sentence rather than an arbitrary exception message. */
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            new CollectorRuntimeState().PublishStopped(CollectorRuntimeState.StartupStep.Store);
            var settled = DarlingStoreLogins.ReadComposeStoreVerdict();
            Assert.NotNull(settled);
            Assert.False(settled.Provisioned);
            Assert.Contains(
                $"The collector stopped before it could provision the store's roles (Store: {CollectorRuntimeState.FailureDetailFor(CollectorRuntimeState.StartupStep.Store)})",
                settled.NotProvisionedReason, StringComparison.Ordinal);

            /* A stand-down never reached the worker's directory argument, so the hosts look for an earlier start's
               credential where the shipped image keeps them. */
            Assert.Equal(DarlingManagedRoles.ComposeStoreCredentialDirectory, settled.CredentialDirectory);

            /* And a published verdict wins over a later stand-down's settle. */
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            DarlingStoreLogins.PublishComposeStoreVerdict(Provisioned());
            DarlingStoreLogins.SettleComposeStoreVerdict("too late");
            Assert.True(DarlingStoreLogins.ReadComposeStoreVerdict()!.Provisioned);
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        }
    }

    /// <summary>A refused or failed start carries the directory the worker provisions from, so the hosts read the
    /// earlier credentials from the same place; a provisioned verdict carries none, since nothing reads it.</summary>
    [Fact]
    public void ANotProvisionedVerdict_CarriesWhereTheEarlierCredentialsAre()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-3914-verdict");

        Assert.Equal(directory, DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("no", directory).CredentialDirectory);
        Assert.Equal(DarlingManagedRoles.ComposeStoreCredentialDirectory, DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("no").CredentialDirectory);
        Assert.Null(Provisioned().CredentialDirectory);
    }

    [Fact]
    public void AVerdict_CannotPrintItsLogins()
    {
        /* Classes, not records: a generated ToString would put the password into any log line that formats one. */
        Assert.DoesNotContain("ViewerSecret2", Provisioned().ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(
            "ViewerSecret2",
            DarlingStoreLogins.Resolve(DarlingStoreLogins.Surface.Web, Owner, null, inContainer: true, Provisioned()).ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(typeof(DarlingStoreLogins.ComposeStoreVerdict).GetMethods(), m => m.Name == "<Clone>$");
        Assert.DoesNotContain(typeof(ComposeStoreProvisioning).GetMethods(), m => m.Name == "<Clone>$");
        Assert.DoesNotContain(typeof(DarlingStoreLogins.StoreLogin).GetMethods(), m => m.Name == "<Clone>$");

        /* Nor the credential an earlier start left. */
        Assert.DoesNotContain("ViewerSecret2", EarlierComposeCredential.Trusted("ViewerSecret2").ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(typeof(EarlierComposeCredential).GetMethods(), m => m.Name == "<Clone>$");
    }

    /* ─────────────────────────── the credential an earlier start left ─────────────────────────── */

    /// <summary>
    /// What a host reads when this start did not provision (#3914 review, F1): a credential file the worker's own
    /// checks trust, and nothing else. Real files in a temp directory, since the checks are the file system's; the
    /// Unix mode arms are pinned purely below. It never deletes, generates or writes — a distrusted file is the next
    /// provisioning's to replace.
    /// </summary>
    [Fact]
    public void AnEarlierCredential_IsReadOnlyFromATrustedFile_AndNothingIsDeletedOrWritten()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The file checks here are the Windows ones; the Unix arms are pinned purely.");

        var root = Directory.CreateTempSubdirectory("darling-3914-earlier-");
        try
        {
            var directory = Path.Combine(root.FullName, "credentials");
            var viewerFile = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName("viewer"));

            /* No directory yet: nothing to use, and nothing created. */
            var none = DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance);
            Assert.Null(none.Password);
            Assert.Equal($"{directory} does not exist", none.MissingReason);
            Assert.False(Directory.Exists(directory));

            /* No file: nothing to use. */
            Directory.CreateDirectory(directory);
            Assert.Equal($"{viewerFile} does not exist", DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance).MissingReason);

            /* A trusted file: its password, trimmed. */
            File.WriteAllText(viewerFile, "Earlier3914\n");
            var trusted = DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance);
            Assert.Equal("Earlier3914", trusted.Password);
            Assert.Null(trusted.MissingReason);

            /* One ordinary users can read is distrusted — a plaintext password they could read may be theirs — and
               left where it is. */
            var exposed = new FileInfo(viewerFile).GetAccessControl();
            exposed.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(viewerFile).SetAccessControl(exposed);
            var readable = DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance);
            Assert.Null(readable.Password);
            Assert.Equal($"{viewerFile} is not trusted (ordinary local users can read it)", readable.MissingReason);
            Assert.True(File.Exists(viewerFile));

            /* Empty, blank, or a directory where the file should be: none is a credential. */
            File.Delete(viewerFile);
            File.WriteAllText(viewerFile, "");
            Assert.Equal($"{viewerFile} is not trusted (it is empty, or not a regular file)", DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance).MissingReason);
            File.WriteAllText(viewerFile, " \r\n ");
            Assert.Equal($"{viewerFile} is not trusted (it holds no password)", DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance).MissingReason);
            File.Delete(viewerFile);
            Directory.CreateDirectory(viewerFile);
            Assert.Equal($"{viewerFile} is not trusted (it is a directory)", DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance).MissingReason);
            Assert.True(Directory.Exists(viewerFile));
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// #4004's review: a host's earlier-credential read after another caller closed the directory. Whatever looked at
    /// the directory first (the worker, as provisioning or as the log-hash key's load) set it 0700, so a read judged by
    /// what IT found trusted a password planted while the directory was 0777. Round 2: that first look removed the
    /// planted password there and then, so the read finds none, and a later start cannot find it either. A directory
    /// opened again after that look is found open again by the next look, and emptied again.
    /// </summary>
    [Fact]
    public void AnEarlierCredential_InADirectoryOpenUntilThisStart_IsNotRead_AfterAnotherCallerClosedIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-4004-earlier-");
        try
        {
            var directory = Path.Combine(root.FullName, "credentials");
            Directory.CreateDirectory(directory);
            var viewerFile = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName("viewer"));
            StandInUnixModes.WriteOwnerOnly(viewerFile, "Planted4004");
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var first = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, create: true, NullLogger.Instance);
                Assert.Equal("700", modes.Octal(directory));
                Assert.Null(first.Distrust);
                Assert.False(File.Exists(viewerFile), "the planted password outlived the look that found its directory open");

                var earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance);

                Assert.Null(earlier.Password);
                Assert.Equal($"{viewerFile} does not exist", earlier.MissingReason);

                /* Opened again after the first look, and planted again: round 1 judged every later caller by the mode the
                   first look found (0700 by now) and trusted this. */
                modes.Report(directory, "777");
                StandInUnixModes.WriteOwnerOnly(viewerFile, "PlantedAgain4004");

                var again = DarlingManagedRoles.ReadEarlierComposeCredential(directory, "viewer", NullLogger.Instance);

                Assert.Null(again.Password);
                Assert.False(File.Exists(viewerFile), "a password planted after the first look was read");
                Assert.Equal("700", modes.Octal(directory));
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The Unix credentials-directory verdict (#3914 review, F4), pure because the test host is Windows. The service
    /// sets the directory to 0700 every start and reads the mode back: still reachable by others afterwards means
    /// nothing in it is read or written; writable by others BEFORE means a 0600 file could have been planted by
    /// someone the mode check cannot tell from the service, so nothing is read this start and the new files replace
    /// the old; group or other read without write planted nothing, and owner-only is trusted.
    /// </summary>
    [Theory]
    [InlineData("700", "700", null, true)]
    [InlineData("755", "700", null, true)]
    [InlineData("750", "700", null, true)]
    [InlineData("777", "700", "its mode was 0777, which let other users put files in it until the service set it to owner-only this start", true)]
    [InlineData("770", "700", "its mode was 0770, which let other users put files in it until the service set it to owner-only this start", true)]
    [InlineData("702", "700", "its mode was 0702, which let other users put files in it until the service set it to owner-only this start", true)]
    [InlineData("777", "777", "its mode is still 0777 after the service set it to owner-only, which a filesystem that ignores Unix modes does", false)]
    [InlineData("700", "750", "its mode is still 0750 after the service set it to owner-only, which a filesystem that ignores Unix modes does", false)]
    public void TheCredentialsDirectory_IsTrustedOnlyWhenNoOneElseCouldReachIt(string before, string after, string? distrust, bool mayWrite)
    {
        var verdict = DarlingManagedRoles.JudgeComposeCredentialDirectory(
            (UnixFileMode)Convert.ToInt32(before, 8), (UnixFileMode)Convert.ToInt32(after, 8));

        Assert.Equal(distrust, verdict.Distrust);
        Assert.Equal(mayWrite, verdict.MayWrite);
    }

    [Fact]
    public void AMode_IsWrittenTheWayAnOperatorTypesIt()
    {
        Assert.Equal("0700", DarlingManagedRoles.Octal(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute));
        Assert.Equal("0644", DarlingManagedRoles.Octal((UnixFileMode)Convert.ToInt32("644", 8)));
    }

    /// <summary>
    /// A setting that is there but resolves to whitespace keeps its surface down (#3914 review, F3). It used to come
    /// back as the whitespace, which the decision read as "not set": no verdict wait, and the owner login the setting
    /// exists to avoid. Outside a container, so no verdict is involved at all.
    /// </summary>
    [Fact]
    public async Task ASettingThatResolvesToWhitespace_KeepsTheSurfaceDown_NeverTheOwner()
    {
        var name = "DARLING_TEST_3914_BLANK_" + Guid.NewGuid().ToString("N");
        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        try
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", null);
            Environment.SetEnvironmentVariable(name, "   ");
            var log = new CapturingTestLogger();

            var login = await DarlingStoreLogins.ResolveUnmanagedAsync(
                DarlingStoreLogins.Surface.Web,
                new PostgresConfig { ConnectionString = Owner, WebConnectionString = "env:" + name },
                log, CancellationToken.None);

            Assert.Null(login);
            Assert.Contains("Error: The web dashboard not started this attempt: postgres.webConnectionString", log.Joined, StringComparison.Ordinal);
            Assert.Contains(name, log.Joined, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
        }
    }

    /* ─────────────────────────── which store is the service's own ─────────────────────────── */

    [Fact]
    public void TheComposeStore_IsOnlyOneWhoseLoginIsItsBootstrapSuperuser()
    {
        var refusal = DarlingManagedRoles.RefuseComposeStore(Facts(bootstrapSuperuser: false));

        Assert.NotNull(refusal);
        Assert.Contains("not the store's bootstrap superuser", refusal, StringComparison.Ordinal);
        Assert.Contains("POSTGRES_USER", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void ASameNamedRoleTheServiceDidNotCreate_IsLeftAlone()
    {
        var refusal = DarlingManagedRoles.RefuseComposeStore(Facts(
            bootstrapSuperuser: true,
            ("viewer", DarlingManagedRoles.RoleMarker),
            ("admin", null),
            ("mcp", DarlingManagedRoles.ComposeStoreRoleMarker)));

        Assert.NotNull(refusal);
        Assert.Contains("'admin' (not created by Darling) and 'viewer' (created by tools/provision-roles.sql)", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("'mcp'", refusal, StringComparison.Ordinal);
        Assert.Contains("rather than re-key their passwords", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServicesOwnRoles_AndNoRolesAtAll_AreProvisioned()
    {
        Assert.Null(DarlingManagedRoles.RefuseComposeStore(Facts(bootstrapSuperuser: true)));
        Assert.Null(DarlingManagedRoles.RefuseComposeStore(Facts(
            bootstrapSuperuser: true,
            ("admin", DarlingManagedRoles.ComposeStoreRoleMarker),
            ("viewer", DarlingManagedRoles.ComposeStoreRoleMarker),
            ("mcp", DarlingManagedRoles.ComposeStoreRoleMarker))));
    }

    /// <summary>
    /// A cluster that holds any database but the store's own, <c>postgres</c> and the templates is not the compose
    /// store (#3914 review, F2): an operator's own multi-database cluster that the container reaches as its bootstrap
    /// superuser passes the superuser test, and roles are cluster-wide. The refusal names a few of the databases and
    /// counts the rest.
    /// </summary>
    [Theory]
    [InlineData(new[] { "operator_app" }, "the database 'operator_app'")]
    [InlineData(new[] { "a", "b" }, "the databases 'a' and 'b'")]
    [InlineData(new[] { "a", "b", "c" }, "the databases 'a', 'b' and 'c'")]
    [InlineData(new[] { "a", "b", "c", "d", "e" }, "the databases 'a', 'b', 'c' and 2 more")]
    public void AClusterThatAlsoHoldsAnotherDatabase_IsNotTheServicesOwn(string[] others, string named)
    {
        var refusal = DarlingManagedRoles.RefuseComposeStore(Facts(true, others));

        Assert.Equal(
            $"This cluster also holds {named} that the service does not own, so it is not treated as the service's own and no roles were created on it.",
            refusal);
    }

    [Fact]
    public void TheOtherDatabasesRefusal_ComesAfterTheBootstrapOne_AndCannotForgeALogLine()
    {
        Assert.Contains(
            "not the store's bootstrap superuser",
            DarlingManagedRoles.RefuseComposeStore(Facts(false, new[] { "operator_app" })),
            StringComparison.Ordinal);

        var refusal = DarlingManagedRoles.RefuseComposeStore(Facts(true, new[] { "app\nERROR: forged" }));
        Assert.NotNull(refusal);
        Assert.Contains("'app?ERROR: forged'", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("\n", refusal, StringComparison.Ordinal);
    }

    /// <summary>What the facts read counts as another database: everything but the store's own, <c>postgres</c> and
    /// the templates, by flag and by name.</summary>
    [Fact]
    public void TheFactsRead_CountsEveryDatabaseButTheStoresOwnPostgresAndTheTemplates()
    {
        var sql = Regex.Replace(DarlingManagedRoles.ComposeStoreFactsSql, @"\s+", " ");

        Assert.Contains("FROM pg_catalog.pg_database AS d", sql, StringComparison.Ordinal);
        Assert.Contains(
            "WHERE d.datname NOT IN (pg_catalog.current_database(), 'postgres', 'template0', 'template1') AND NOT d.datistemplate",
            sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ANameWithAControlCharacter_IsRefused()
    {
        var refusal = DarlingManagedRoles.RefuseComposeStore(
            new ComposeStoreFacts("dar\nling", "darling", true, new Dictionary<string, string?>(), Array.Empty<string>()));

        Assert.NotNull(refusal);
        Assert.Contains("control character", refusal, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the batch each store gets ─────────────────────────── */

    /// <summary>The managed batch keeps its bare owner and database names, its marker and its PUBLIC revoke, and
    /// omitting the target is the same as naming <see cref="ProvisioningTarget.Managed"/>.</summary>
    [Fact]
    public void TheManagedBatch_KeepsItsNamesMarkerAndPublicRevoke_AndIsTheDefaultTarget()
    {
        var byDefault = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp);
        var explicitManaged = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
            McpCommandDeadlines.ComposedQueryFallbackSeconds, PasswordReassert.All, ProvisioningTarget.Managed);

        Assert.Equal(byDefault, explicitManaged);
        Assert.Contains("REVOKE ALL ON DATABASE darling FROM PUBLIC;", byDefault, StringComparison.Ordinal);
        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE darling IN SCHEMA collect", byDefault, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE viewer IS 'darling-managed';", byDefault, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedRoles.ComposeStoreRoleMarker, byDefault, StringComparison.Ordinal);
    }

    [Fact]
    public void TheComposeStoreBatch_NamesItsOwnOwnerAndDatabase_StampsItsOwnMarker_AndLeavesPublicConnectAlone()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
            15, PasswordReassert.All, ProvisioningTarget.ComposeStore("darling", "darling"));

        Assert.Contains("ALTER DEFAULT PRIVILEGES FOR ROLE \"darling\" IN SCHEMA collect", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FOR ROLE darling ", sql, StringComparison.Ordinal);
        Assert.Contains("GRANT CONNECT ON DATABASE \"darling\" TO admin, viewer;", sql, StringComparison.Ordinal);
        Assert.Contains("COMMENT ON ROLE viewer IS 'darling-compose';", sql, StringComparison.Ordinal);
        Assert.Contains("IS DISTINCT FROM 'darling-compose'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("'darling-managed'", sql, StringComparison.Ordinal);

        /* The database-level PUBLIC revoke would take CONNECT from the operator's own roles on this store; the
           schema-level one stays, so no role can create objects in public. */
        Assert.DoesNotContain("REVOKE ALL ON DATABASE", sql, StringComparison.Ordinal);
        Assert.Contains("REVOKE CREATE ON SCHEMA public FROM PUBLIC;", sql, StringComparison.Ordinal);

        /* A renamed POSTGRES_USER / POSTGRES_DB is named as it is, quoted, with an embedded quote doubled. */
        var renamed = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
            15, PasswordReassert.All, ProvisioningTarget.ComposeStore("Store \"Owner\"", "my db"));
        Assert.Contains("FOR ROLE \"Store \"\"Owner\"\"\" IN SCHEMA collect", renamed, StringComparison.Ordinal);
        Assert.Contains("GRANT CONNECT ON DATABASE \"my db\" TO mcp;", renamed, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the reload gate ─────────────────────────── */

    [Theory]
    // store, applied, managed, windows, composeStoreProvisioned, expected
    [InlineData(120, 15, false, false, true, true)]    // the compose store this process provisioned: kept current
    [InlineData(15, 15, false, false, true, false)]    // unchanged: no catalog write
    [InlineData(120, 15, false, false, false, false)]  // any other BYO store: its roles are the operator's
    [InlineData(120, 15, false, true, false, false)]
    [InlineData(120, 15, true, true, false, true)]     // managed: exactly as before
    [InlineData(120, 15, true, false, false, false)]
    public void TheReloadGate_KeepsTheComposeStoresRolesCurrent_AndNoOneElses(
        int store, int applied, bool managed, bool windows, bool composeStoreProvisioned, bool expected)
    {
        Assert.Equal(
            expected,
            DarlingManagedRoles.ShouldReassertComposeStatementTimeout(store, applied, managed, windows, composeStoreProvisioned));
    }

    /* ─────────────────────────── the two settings ─────────────────────────── */

    [Fact]
    public void TheTwoSettings_ParseAsWritten_AndAReferenceIsNotResolvedAtParse()
    {
        /* Resolved when the host starts: an unreadable reference must stop that surface, not collection, which
           is what resolving it here (as connectionString is) would do. */
        var config = DarlingConfig.Parse("""
            {
              "postgres": {
                "connectionString": "Host=db;Username=darling;Database=darling",
                "webConnectionString": "env:DARLING_TEST_3914_NO_SUCH_VARIABLE",
                "mcpConnectionString": "file:/no/such/file/3914"
              },
              "servers": [ { "host": "sql1", "auth": "integrated" } ]
            }
            """);

        Assert.Equal("env:DARLING_TEST_3914_NO_SUCH_VARIABLE", config.Postgres.WebConnectionString);
        Assert.Equal("file:/no/such/file/3914", config.Postgres.McpConnectionString);
        Assert.Empty(config.Validate());
    }

    [Fact]
    public void ManagedMode_RejectsEitherSetting()
    {
        var config = new DarlingConfig
        {
            Postgres = new PostgresConfig { Managed = true, WebConnectionString = "Host=x", McpConnectionString = "Host=y" },
            Servers = { new MonitoredServer { Host = "sql1", Auth = "integrated" } },
        };

        var problems = config.Validate();

        Assert.Contains(problems, p => p.Contains("postgres.webConnectionString is set", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.Contains("postgres.mcpConnectionString is set", StringComparison.Ordinal));
    }

    /* ─────────────────────────── the custom-alert evaluator's login (#3970) ─────────────────────────── */

    [Fact]
    public async Task CustomAlertViewer_WithNoVerdictPublished_IsNull()
    {
        DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        try
        {
            /* No verdict at all: a bring-your-own store, or a process that never reached compose provisioning
               (managed mode, or not running in a container). Neither gets the evaluator. */
            Assert.Null(await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(
                Owner, NullLogger.Instance, CancellationToken.None));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        }
    }

    [Fact]
    public async Task CustomAlertViewer_WithAProvisionedVerdict_IsItsViewerLogin()
    {
        DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        try
        {
            DarlingStoreLogins.PublishComposeStoreVerdict(Provisioned());

            Assert.Equal(ViewerLogin, await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(
                Owner, NullLogger.Instance, CancellationToken.None));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        }
    }

    /// <summary>
    /// Unlike a host (<see cref="ResolveUnmanagedAsync"/>'s last branch), a not-provisioned verdict with no
    /// trusted credential from an earlier start never falls back to the owner login for the evaluator: it
    /// returns null, exactly as "no verdict at all" does, so the worker logs "not started" rather than hand a
    /// rule's compose metric owner rights.
    /// </summary>
    [Fact]
    public async Task CustomAlertViewer_NotProvisioned_WithNoEarlierCredential_IsNull_NeverTheOwner()
    {
        DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        try
        {
            var emptyDirectory = Path.Combine(Path.GetTempPath(), "darling-3970-no-credential-" + Guid.NewGuid().ToString("N"));
            DarlingStoreLogins.PublishComposeStoreVerdict(
                DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("refused", emptyDirectory));

            Assert.Null(await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(
                Owner, NullLogger.Instance, CancellationToken.None));
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
        }
    }

    /* ─────────────────────────── the wiring, by source ─────────────────────────── */

    /// <summary>
    /// Each host's store pool is created from the resolver on an unmanaged store and from its DPAPI credential
    /// on a managed one, and from nothing else. The pure tests above cannot see this: a host that went on
    /// passing <c>config.Postgres.ConnectionString</c> to <c>NpgsqlDataSource.Create</c> would leave all of them
    /// green. Containment, not order — the resolver call has to be INSIDE the else of the managed branch.
    /// </summary>
    [Theory]
    [InlineData("DarlingWebHostService.cs", "Web", "TryBuildViewerConnectionStringFromStoredCredential")]
    [InlineData("DarlingMcpHostService.cs", "Mcp", "TryBuildMcpConnectionStringFromStoredCredential")]
    public void EachHost_BuildsItsUnmanagedPoolFromTheResolver_AndItsManagedPoolAsBefore(
        string file, string surface, string managedBuilder)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file));
        var start = Body(code, "private async Task<bool> TryStartServerAsync(");

        Assert.DoesNotContain("config.Postgres.ConnectionString", start, StringComparison.Ordinal);

        /* #4277: every STORE data source pins its session timezone to UTC, so the resolved
           storeConnectionString is wrapped before it reaches Create rather than passed bare. */
        Assert.Contains(
            "NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(storeConnectionString))",
            start,
            StringComparison.Ordinal);

        var assignments = Regex.Matches(start, @"storeConnectionString\s*=(?!=)").Count;
        Assert.Equal(2, assignments);

        var managedIf = Regex.Match(start, @"if \(config\.Postgres\.Managed\)\s*\{");
        Assert.True(managedIf.Success, "the managed branch could not be located, so this check proves nothing");
        var open = managedIf.Index + managedIf.Length - 1;
        var managedBlock = CSharpSourceWalker.BraceBalanced(start, open);
        Assert.Contains("storeConnectionString = await WaitForManagedConnectionStringAsync(config.Postgres, stoppingToken);", managedBlock, StringComparison.Ordinal);
        Assert.DoesNotContain("DarlingStoreLogins", managedBlock, StringComparison.Ordinal);

        var afterManaged = start[(open + managedBlock.Length)..].TrimStart();
        Assert.StartsWith("else", afterManaged, StringComparison.Ordinal);
        var elseBlock = CSharpSourceWalker.BraceBalanced(afterManaged, afterManaged.IndexOf('{'));
        Assert.Matches(
            $@"storeConnectionString = await DarlingStoreLogins\.ResolveUnmanagedAsync\(\s*DarlingStoreLogins\.Surface\.{surface}, config\.Postgres, _logger, stoppingToken\);",
            elseBlock);

        /* The managed half is unchanged: its wait still derives the role login from the DPAPI credential. */
        var wait = Body(code, "private async Task<string?> WaitForManagedConnectionStringAsync(");
        Assert.Contains($"DarlingManagedPostgres.{managedBuilder}(config)", wait, StringComparison.Ordinal);
    }

    /// <summary>
    /// The worker provisions the compose store as the else of the managed provisioning branch, gated on an
    /// unmanaged store in a container, and seeds the #2918 reload baseline — and the flag that lets the reload
    /// write to these roles at all — only from a verdict that says it provisioned them.
    /// </summary>
    [Fact]
    public void TheWorker_ProvisionsTheComposeStore_AndTrustsOnlyAProvisionedVerdict()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var loop = Body(code, "private async Task RunCollectionLoopAsync(");

        const string managedIf = "if (config.Postgres.Managed && OperatingSystem.IsWindows())";
        var managedBlock = Regex.Matches(loop, Regex.Escape(managedIf) + @"\s*\{")
            .Select(m => (Start: m.Index + m.Length - 1, Block: CSharpSourceWalker.BraceBalanced(loop, m.Index + m.Length - 1)))
            .Single(b => b.Block.Contains("DarlingManagedRoles.EnsureProvisionedAsync(", StringComparison.Ordinal));

        var afterManaged = loop[(managedBlock.Start + managedBlock.Block.Length)..].TrimStart();
        const string composeIf = "else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)";
        Assert.StartsWith(composeIf, afterManaged, StringComparison.Ordinal);

        var composeBlock = CSharpSourceWalker.BraceBalanced(afterManaged, afterManaged.IndexOf('{'));
        Assert.Matches(
            @"var verdict = await DarlingStoreLogins\.ProvisionComposeStoreAsync\(\s*postgres, config\.Postgres\.ConnectionString, _logger, stoppingToken\);",
            composeBlock);

        var provisioned = Regex.Match(composeBlock, @"if \(verdict\.Provisioned\)\s*\{");
        Assert.True(provisioned.Success, "the provisioned branch could not be located");
        var trusted = CSharpSourceWalker.BraceBalanced(composeBlock, provisioned.Index + provisioned.Length - 1);
        Assert.Contains("_composeStoreRolesProvisioned = true;", trusted, StringComparison.Ordinal);
        Assert.Contains("_appliedComposeStatementTimeoutSeconds = verdict.AppliedComposeStatementTimeoutSeconds;", trusted, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(composeBlock, "_composeStoreRolesProvisioned"));
        Assert.Single(Regex.Matches(composeBlock, "_appliedComposeStatementTimeoutSeconds"));

        /* And the reload gate hears about it. */
        var reload = Body(code, "private async Task<long?> ReloadFromStoreAsync(");
        Assert.Contains(
            "config.Postgres.Managed, OperatingSystem.IsWindows(), _composeStoreRolesProvisioned)",
            reload, StringComparison.Ordinal);
    }

    /// <summary>
    /// The custom-alert evaluator's connection-string gate (#3970): the managed branch is unchanged, the compose
    /// branch is gated on EXACTLY the condition the worker provisions the compose store's roles under (so a
    /// bring-your-own store — not managed, not in a container — takes neither branch and the evaluator does not
    /// start there), and there are only ever the two assignments — never the raw owner connection string.
    /// </summary>
    [Fact]
    public void TheCustomAlertEvaluator_AdmitsTheComposeVerdictBesideManaged_AndNeverTheOwner()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var loop = Body(code, "private async Task RunCollectionLoopAsync(");

        const string declare = "string? customAlertViewerConnString = null;";
        var declareAt = loop.IndexOf(declare, StringComparison.Ordinal);
        Assert.True(declareAt >= 0, "the custom-alert evaluator's connection-string gate could not be located");
        const string sourceCreate = "await using var customAlertViewerSource =";
        var sourceAt = loop.IndexOf(sourceCreate, declareAt, StringComparison.Ordinal);
        Assert.True(sourceAt > declareAt, "the custom-alert evaluator's pool creation could not be located after the gate");
        // Past the "= null;" declaration itself, which would otherwise count as a third assignment below.
        var gate = loop[(declareAt + declare.Length)..sourceAt];

        Assert.Matches(
            @"if \(OperatingSystem\.IsWindows\(\) && config\.Postgres\.Managed\)\s*\{\s*"
            + @"customAlertViewerConnString = DarlingManagedPostgres\.TryBuildViewerConnectionStringFromStoredCredential\(config\.Postgres\);\s*\}",
            gate);

        Assert.Matches(
            @"else if \(!config\.Postgres\.Managed && Hosting\.DarlingHostBinding\.IsRunningInContainer\)\s*\{\s*"
            + @"customAlertViewerConnString = await DarlingStoreLogins\.ResolveComposeCustomAlertViewerAsync\(\s*"
            + @"config\.Postgres\.ConnectionString, _logger, stoppingToken\);\s*\}",
            gate);

        Assert.Equal(2, Regex.Matches(gate, @"customAlertViewerConnString\s*=(?!=)").Count);
        Assert.DoesNotContain("customAlertViewerConnString = config.Postgres.ConnectionString", gate, StringComparison.Ordinal);
    }

    private static string Body(string code, string signature)
    {
        var at = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(at >= 0, $"'{signature}' could not be located, so this check proves nothing");
        return CSharpSourceWalker.BraceBalanced(code, code.IndexOf('{', code.IndexOf(')', at)));
    }

    private static DarlingStoreLogins.ComposeStoreVerdict Provisioned() =>
        DarlingStoreLogins.ComposeStoreVerdict.ProvisionedWith(ViewerLogin, McpLogin, 15);

    /// <summary>A not-provisioned verdict worded as each outcome's really is.</summary>
    private static DarlingStoreLogins.ComposeStoreVerdict NotProvisioned(string outcome) => outcome switch
    {
        "refusal" => DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(
            DarlingManagedRoles.RefuseComposeStore(Facts(true, new[] { "operator_app" }))!),
        "failure" => DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(
            "Provisioning the store's least-privilege roles failed: 40P01: deadlock detected"),
        "stand-down" => DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned(
            "The collector stopped before it could provision the store's roles (Store: connection refused)."),
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, null),
    };

    private static ComposeStoreFacts Facts(bool bootstrapSuperuser, params (string Role, string? Marker)[] roles) =>
        new("darling", "darling", bootstrapSuperuser, roles.ToDictionary(r => r.Role, r => r.Marker, StringComparer.Ordinal), Array.Empty<string>());

    private static ComposeStoreFacts Facts(bool bootstrapSuperuser, string[] otherDatabases) =>
        new("darling", "darling", bootstrapSuperuser, new Dictionary<string, string?>(StringComparer.Ordinal), otherDatabases);
}
