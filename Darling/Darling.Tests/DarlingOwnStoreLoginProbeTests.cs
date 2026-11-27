/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Buffers.Binary;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database through ScratchPostgres and logins with per-run names, and touches nothing on
   the shared one. The class is in the config-env collection because the host tests read the ambient container marker. */

/// <summary>
/// #9894: before the web dashboard or the MCP server binds a network address on a store the service does not manage (outside a container), the
/// host connects once with that surface's own login and asks the store whether it is a superuser or a member of the role that owns the store.
/// The user names alone cannot see either. The decision is exposed only for a login that is neither, and a probe that cannot run keeps the surface
/// on loopback with the error class (never the connection string).
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingOwnStoreLoginProbeTests : IDisposable
{
    private const string Owner = "Host=pg.example;Database=darling;Username=postgres;Password=owner-secret";
    private const string OwnerWithNoUser = "Host=pg.example;Database=darling";
    private const string Dedicated = "Host=pg.example;Database=darling;Username=dashboard;Password=d-secret";
    private const string Token = "a-long-token-value-0123456789";

    public DarlingOwnStoreLoginProbeTests() => DarlingStoreLogins.ResetContainerExposureLogForTests();

    public void Dispose() => DarlingStoreLogins.ResetContainerExposureLogForTests();

    private static PostgresConfig OwnStore(string ownerString, string? webConnectionString = null, string? mcpConnectionString = null) => new()
    {
        Managed = false,
        ConnectionString = ownerString,
        WebConnectionString = webConnectionString,
        McpConnectionString = mcpConnectionString,
    };

    private static WebConfig Web() => new()
    {
        Enabled = true,
        Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = Token },
    };

    private static McpConfig Mcp() => new()
    {
        Enabled = true,
        Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = Token },
    };

    private static DarlingHostBinding.BindDecision ToShared(DarlingMcpHostService.McpBindDecision decision) =>
        new((DarlingHostBinding.BindMode)(int)decision.Mode, (DarlingHostBinding.BindReason)(int)decision.Reason);

    private static Func<bool, DarlingHostBinding.BindDecision> ResolveFor(DarlingStoreLogins.Surface surface, PostgresConfig postgres, bool inContainer) =>
        surface == DarlingStoreLogins.Surface.Web
            ? dedicated => DarlingWebHostService.ResolveWebBind(Web(), postgres.Managed, inContainer, dedicated)
            : dedicated => ToShared(DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres.Managed, inContainer, dedicated));

    private sealed class FakeProbe(DarlingStoreLogins.LoginProbeResult result)
    {
        public int Calls { get; private set; }

        public Task<DarlingStoreLogins.LoginProbeResult> RunAsync(DarlingStoreLogins.Surface surface, PostgresConfig postgres, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(result);
        }
    }

    private static DarlingStoreLogins.LoginProbeResult Probe(DarlingStoreLogins.LoginProbeVerdict verdict, string? errorClass = null) => new(verdict, errorClass);

    /* ---- Item 1: an owner string with no Username proves nothing on names ---- */

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AnOwnerStringWithNoUsername_IsRefusedByTheNamesAndByTheHostAlike(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var postgres = surface == DarlingStoreLogins.Surface.Web ? OwnStore(OwnerWithNoUser, webConnectionString: Dedicated) : OwnStore(OwnerWithNoUser, mcpConnectionString: Dedicated);

        Assert.Equal(DarlingStoreLogins.LoginNameCheck.OwnerUserUnknown, DarlingStoreLogins.CheckLoginNames(surface, postgres));
        Assert.False(DarlingStoreLogins.HasDedicatedLogin(surface, postgres));

        var decision = surface == DarlingStoreLogins.Surface.Web
            ? DarlingWebHostService.ResolveWebBind(Web(), postgres, inContainer: false)
            : ToShared(DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres, inContainer: false));
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, decision.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.OwnerUserUnknown, decision.Reason);
        Assert.Equal(LogLevel.Critical, DarlingHostBinding.MapBindReasonSeverity(decision.Reason));

        /* The CLI and the reports answer the same: this surface is on loopback, so its firewall plan removes the rule and opens nothing. */
        var config = new DarlingConfig { Postgres = postgres, Web = Web(), Mcp = Mcp() };
        Assert.True(DarlingCliCommands.DedicatedLoginRefusesExposure(config, mcp: !web));
        var plan = DarlingCliCommands.PlanFirewallRules(config).Single(p => p.Surface == (web ? "web dashboard" : "MCP"));
        Assert.Equal(DarlingCliCommands.FirewallRuleAction.Remove, plan.Action);
        Assert.False(string.IsNullOrEmpty(plan.Note));
    }

    /* ---- Item 4: the firewall plan follows the resolver's name-level answer ---- */

    [Theory]
    [MemberData(nameof(Surfaces))]
    public void TheFirewallPlan_IsSkippedOnlyForAnOwnStoreSurfaceTheHostKeepsOnLoopback(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var mcp = surface == DarlingStoreLogins.Surface.Mcp;
        DarlingConfig Config(PostgresConfig postgres) => new() { Postgres = postgres, Web = Web(), Mcp = Mcp() };
        PostgresConfig Own(string? surfaceString) => mcp ? OwnStore(Owner, mcpConnectionString: surfaceString) : OwnStore(Owner, webConnectionString: surfaceString);

        Assert.True(DarlingCliCommands.DedicatedLoginRefusesExposure(Config(Own(null)), mcp));
        Assert.True(DarlingCliCommands.DedicatedLoginRefusesExposure(Config(Own(Owner)), mcp));
        Assert.False(DarlingCliCommands.DedicatedLoginRefusesExposure(Config(Own(Dedicated)), mcp));
        /* An owner string that names no user is refused too: the host does not expose on it. */
        Assert.True(DarlingCliCommands.DedicatedLoginRefusesExposure(
            Config(mcp ? OwnStore(OwnerWithNoUser, mcpConnectionString: Dedicated) : OwnStore(OwnerWithNoUser, webConnectionString: Dedicated)), mcp));
        Assert.False(DarlingCliCommands.DedicatedLoginRefusesExposure(Config(new PostgresConfig { Managed = true, ConnectionString = Owner }), mcp));

        /* A listen that is not a network address is not an exposure to refuse. */
        var loopback = new DarlingConfig
        {
            Postgres = Own(null),
            Web = new WebConfig { Enabled = true, Network = new WebNetworkConfig { Listen = "127.0.0.1", AllowFrom = "192.168.1.0/24", Token = Token } },
            Mcp = new McpConfig { Enabled = true, Network = new McpNetworkConfig { Listen = "127.0.0.1", AllowFrom = "192.168.1.0/24", Token = Token } },
        };
        Assert.False(DarlingCliCommands.DedicatedLoginRefusesExposure(loopback, mcp));
    }

    /* ---- Item 2: the decision as a pure function of the probe result ---- */

    [Theory]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, true)]
    [InlineData(false, false, false)]
    public void ASuperuserOrAnOwnerRoleMember_IsPrivileged(bool superuser, bool ownerMember, bool expectedPrivileged) =>
        Assert.Equal(expectedPrivileged, DarlingStoreLogins.ClassifyLoginProbe(superuser, ownerMember).Verdict == DarlingStoreLogins.LoginProbeVerdict.Privileged);

    [Theory]
    [InlineData("Unprivileged", "NetworkAndLoopback", "NetworkExposed")]
    [InlineData("Privileged", "LoopbackOnly", "LoginPrivileged")]
    [InlineData("Failed", "LoopbackOnly", "LoginProbeFailed")]
    public void TheProbeResult_DecidesAnExposure_AndLeavesAnythingElseAlone(
        string verdictName, string modeName, string reasonName)
    {
        var verdict = Enum.Parse<DarlingStoreLogins.LoginProbeVerdict>(verdictName);
        var mode = Enum.Parse<DarlingHostBinding.BindMode>(modeName);
        var reason = Enum.Parse<DarlingHostBinding.BindReason>(reasonName);
        var exposed = new DarlingHostBinding.BindDecision(DarlingHostBinding.BindMode.NetworkAndLoopback, DarlingHostBinding.BindReason.NetworkExposed);
        var result = DarlingHostBinding.ApplyLoginProbe(exposed, Probe(verdict, "x"));
        Assert.Equal(mode, result.Mode);
        Assert.Equal(reason, result.Reason);

        var loopback = new DarlingHostBinding.BindDecision(DarlingHostBinding.BindMode.LoopbackOnly, DarlingHostBinding.BindReason.TokenMissing);
        Assert.Equal(loopback, DarlingHostBinding.ApplyLoginProbe(loopback, Probe(verdict, "x")));

        LogLevel? expectedLevel = reason == DarlingHostBinding.BindReason.NetworkExposed ? null : LogLevel.Critical;
        Assert.Equal(expectedLevel, DarlingHostBinding.MapBindReasonSeverity(reason));
        Assert.Equal(expectedLevel, DarlingMcpHostService.MapBindReasonSeverity((DarlingMcpHostService.McpBindReason)(int)reason));
    }

    /// <summary>Only an explicit Unprivileged exposes. A result nobody filled in (<c>default</c>) and a verdict the probe does not name refuse.</summary>
    [Fact]
    public void OnlyAnExplicitUnprivilegedVerdict_Exposes()
    {
        var exposed = new DarlingHostBinding.BindDecision(DarlingHostBinding.BindMode.NetworkAndLoopback, DarlingHostBinding.BindReason.NetworkExposed);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, DarlingHostBinding.ApplyLoginProbe(exposed, default).Mode);
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly,
            DarlingHostBinding.ApplyLoginProbe(exposed, Probe((DarlingStoreLogins.LoginProbeVerdict)99)).Mode);
        Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback,
            DarlingHostBinding.ApplyLoginProbe(exposed, Probe(DarlingStoreLogins.LoginProbeVerdict.Unprivileged)).Mode);
    }

    public static IEnumerable<object[]> Surfaces() =>
    [
        [true],
        [false],
    ];

    /// <summary>What a host start decides, for each login the names let through: a login of its own is probed and only an unprivileged one is
    /// exposed. The names refuse first and never probe. A managed store and a container never probe either.</summary>
    [Theory]
    [MemberData(nameof(Surfaces))]
    public async Task TheHostStart_ProbesOnlyAnOwnStoreOutsideAContainer_AndExposesOnlyAnUnprivilegedLogin(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var ct = TestContext.Current.CancellationToken;
        PostgresConfig Own(string ownerString, string? surfaceString) =>
            surface == DarlingStoreLogins.Surface.Web ? OwnStore(ownerString, webConnectionString: surfaceString) : OwnStore(ownerString, mcpConnectionString: surfaceString);

        async Task<(DarlingHostBinding.BindDecision Bind, int Calls, DarlingStoreLogins.LoginProbeResult? Result)> RunAsync(
            PostgresConfig postgres, bool inContainer, DarlingStoreLogins.LoginProbeResult probeResult)
        {
            var probe = new FakeProbe(probeResult);
            var (bind, result) = await DarlingHostBinding.ResolveBindWithLoginProbeAsync(
                surface, postgres, inContainer, ResolveFor(surface, postgres, inContainer), ct, probe.RunAsync);
            return (bind, probe.Calls, result);
        }

        var unprivileged = Probe(DarlingStoreLogins.LoginProbeVerdict.Unprivileged);

        /* A login of its own, unprivileged: exposed, after one probe. */
        var ok = await RunAsync(Own(Owner, Dedicated), inContainer: false, unprivileged);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, ok.Bind.Reason);
        Assert.Equal(1, ok.Calls);

        /* A superuser, or a member of the owner role, under a name the name check passes: loopback, Critical. */
        var privileged = await RunAsync(Own(Owner, Dedicated), inContainer: false, Probe(DarlingStoreLogins.LoginProbeVerdict.Privileged));
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, privileged.Bind.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.LoginPrivileged, privileged.Bind.Reason);

        /* A probe that cannot run: loopback, and the error class is passed on for the line. */
        var failed = await RunAsync(Own(Owner, Dedicated), inContainer: false, Probe(DarlingStoreLogins.LoginProbeVerdict.Failed, "NpgsqlException"));
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, failed.Bind.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.LoginProbeFailed, failed.Bind.Reason);
        Assert.Equal("NpgsqlException", failed.Result?.ErrorClass);

        /* An owner string with no Username: the names prove nothing, so the host refuses it, with no connection made, whatever a probe would say. */
        var unknown = await RunAsync(Own(OwnerWithNoUser, Dedicated), inContainer: false, unprivileged);
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, unknown.Bind.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.OwnerUserUnknown, unknown.Bind.Reason);
        Assert.Equal(0, unknown.Calls);
        /* ... and it is the answer the name-level overload gives, so a report and the host agree. */
        Assert.Equal(unknown.Bind, surface == DarlingStoreLogins.Surface.Web
            ? DarlingWebHostService.ResolveWebBind(Web(), Own(OwnerWithNoUser, Dedicated), inContainer: false)
            : ToShared(DarlingMcpHostService.ResolveMcpBind(Mcp(), Own(OwnerWithNoUser, Dedicated), inContainer: false)));
        /* In a container the compose port mapping is the boundary: the owner string's missing user changes nothing there. */
        var unknownInContainer = await RunAsync(Own(OwnerWithNoUser, Dedicated), inContainer: true, unprivileged);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, unknownInContainer.Bind.Reason);

        /* No login of its own (unset, or the owner's user): refused on names, no connection made. */
        foreach (var postgres in new[] { Own(Owner, null), Own(Owner, Owner), Own(OwnerWithNoUser, null) })
        {
            var refused = await RunAsync(postgres, inContainer: false, unprivileged);
            Assert.Equal(DarlingHostBinding.BindReason.DedicatedLoginRequired, refused.Bind.Reason);
            Assert.Equal(0, refused.Calls);
        }

        /* A decision that is not an exposure anyway (a listen that is not an address) makes no connection. */
        var notExposed = await DarlingHostBinding.ResolveBindWithLoginProbeAsync(
            surface, Own(Owner, Dedicated), inContainer: false,
            dedicated => DarlingHostBinding.ResolveBind("localhost-name", "192.168.1.0/24", tokenPresent: true, managed: false, inContainer: false, dedicatedLogin: dedicated),
            ct, (s, p, c) => throw new InvalidOperationException("a decision that is not an exposure must not probe"));
        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, notExposed.Bind.Mode);

        /* A container and the managed store: the existing rules, and no probe. */
        var inContainer = await RunAsync(Own(Owner, null), inContainer: true, unprivileged);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, inContainer.Bind.Reason);
        Assert.Equal(0, inContainer.Calls);
        var managedStore = new PostgresConfig { Managed = true, ConnectionString = Owner };
        var managed = await RunAsync(managedStore, inContainer: false, unprivileged);
        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, managed.Bind.Reason);
        Assert.Equal(0, managed.Calls);
    }

    [Fact]
    public void AFailedProbe_NamesTheExceptionAndSqlState_NeverTheConnectionString()
    {
        Assert.Equal("TimeoutException", DarlingStoreLogins.DescribeProbeFailure(new NpgsqlException("connect failed", new TimeoutException())));
        Assert.Equal("InvalidOperationException", DarlingStoreLogins.DescribeProbeFailure(new InvalidOperationException("Password=secret")));

        var text = DarlingStoreLogins.LoginProbeFailedText(DarlingStoreLogins.Surface.Web, "NpgsqlException");
        Assert.Contains("NpgsqlException", text, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", text, StringComparison.Ordinal);
    }

    /* ---- Item 2 through the real start path: a store that cannot be reached ---- */

    private static int ClosedPort()
    {
        var listener = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task<bool> TryStartAsync(object host, DarlingConfig config, int port)
    {
        var tryStart = host.GetType().GetMethod("TryStartServerAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var toggle = new DarlingHostBinding.EndpointToggle(true, port, DarlingHostBinding.EndpointToggleOrigin.File, false, false);
        return await (Task<bool>)tryStart.Invoke(host, [config, toggle, CancellationToken.None])!;
    }

    private sealed class TypedLogger<T>(CapturingTestLogger inner) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => inner.BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => inner.IsEnabled(logLevel);

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            inner.Log(logLevel, eventId, state, exception, formatter);
    }

    /// <summary>A host asked to listen on the network, on an own store whose login names pass but whose store cannot be reached, keeps to
    /// loopback and says so at Critical with the error class and without the password. Without the probe it would bind the network.</summary>
    [Theory]
    [MemberData(nameof(Surfaces))]
    public async Task AHostWhoseSurfaceLoginCannotBeChecked_BindsLoopbackOnly_AndLogsTheErrorClass(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", null);
        try
        {
            var closed = ClosedPort();
            var port = ClosedPort();
            const string password = "dash-pw-never-logged";
            var dedicated = $"Host=127.0.0.1;Port={closed};Username=dashboard;Password={password};Database=darlingtest";
            var postgres = new PostgresConfig
            {
                Managed = false,
                ConnectionString = $"Host=127.0.0.1;Port={closed};Username=darling;Password=x;Database=darlingtest",
                WebConnectionString = surface == DarlingStoreLogins.Surface.Web ? dedicated : null,
                McpConnectionString = surface == DarlingStoreLogins.Surface.Mcp ? dedicated : null,
            };
            var config = new DarlingConfig
            {
                Postgres = postgres,
                Web = new WebConfig { Enabled = true, Network = new WebNetworkConfig { Listen = "0.0.0.0", AllowFrom = "10.0.0.0/8", Token = Token } },
                Mcp = new McpConfig { Enabled = true, Network = new McpNetworkConfig { Listen = "0.0.0.0", AllowFrom = "10.0.0.0/8", Token = Token } },
            };
            var log = new CapturingTestLogger();
            if (surface == DarlingStoreLogins.Surface.Web)
            {
                var host = new DarlingWebHostService(
                    new TypedLogger<DarlingWebHostService>(log), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
                try
                {
                    Assert.True(await TryStartAsync(host, config, port), log.Joined);
                }
                finally
                {
                    await host.DisposeFailedStartAsync();
                }
            }
            else
            {
                var host = new DarlingMcpHostService(new TypedLogger<DarlingMcpHostService>(log), new McpRuntimeState(), new MonitoredServerRegistryState());
                try
                {
                    Assert.True(await TryStartAsync(host, config, port), log.Joined);
                }
                finally
                {
                    await host.DisposeFailedStartAsync();
                }
            }

            Assert.Contains(log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal)
                && l.Contains("could not be checked first", StringComparison.Ordinal) && l.Contains("binds loopback-only", StringComparison.Ordinal));
            Assert.DoesNotContain(password, log.Joined, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
        }
    }

    /* ---- A started host binds what the decision says: no network listener when the probe or the names refuse ---- */

    /// <summary>The first IPv4 address of this machine that is not loopback or link-local, or null: what a client on the network would dial.</summary>
    private static IPAddress? LanAddress() =>
        NetworkInterface.GetAllNetworkInterfaces()
            .Where(nic => nic.OperationalStatus == OperationalStatus.Up && nic.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(nic => nic.GetIPProperties().UnicastAddresses)
            .Select(unicast => unicast.Address)
            .FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address)
                && !address.GetAddressBytes().AsSpan().StartsWith(new byte[] { 169, 254 }));

    private static async Task<bool> CanConnectAsync(IPAddress address, int port)
    {
        using var client = new TcpClient(AddressFamily.InterNetwork);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        try
        {
            await client.ConnectAsync(address, port, timeout.Token);
            return true;
        }
        catch (Exception ex) when (ex is SocketException or OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>Starts one host, asked to listen on every interface, on an own store outside a container, and reports what it logged and what it
    /// bound: whether a client reaches it on the loopback address and on this machine's network address.</summary>
    private static async Task<(CapturingTestLogger Log, bool LoopbackReachable, bool NetworkReachable)> StartHostAsync(bool web, PostgresConfig postgres)
    {
        var lan = LanAddress();
        Assert.SkipWhen(lan is null, "This machine has no network address to dial, so a network listener cannot be told from a loopback one.");
        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", null);
        try
        {
            var port = ClosedPort();
            var config = new DarlingConfig
            {
                Postgres = postgres,
                Web = new WebConfig { Enabled = true, Network = new WebNetworkConfig { Listen = "0.0.0.0", AllowFrom = "10.0.0.0/8", Token = Token } },
                Mcp = new McpConfig { Enabled = true, Network = new McpNetworkConfig { Listen = "0.0.0.0", AllowFrom = "10.0.0.0/8", Token = Token } },
            };
            var log = new CapturingTestLogger();
            if (web)
            {
                var host = new DarlingWebHostService(
                    new TypedLogger<DarlingWebHostService>(log), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache());
                try
                {
                    Assert.True(await TryStartAsync(host, config, port), log.Joined);
                    return (log, await CanConnectAsync(IPAddress.Loopback, port), await CanConnectAsync(lan!, port));
                }
                finally
                {
                    await host.DisposeFailedStartAsync();
                }
            }

            var mcpHost = new DarlingMcpHostService(new TypedLogger<DarlingMcpHostService>(log), new McpRuntimeState(), new MonitoredServerRegistryState());
            try
            {
                Assert.True(await TryStartAsync(mcpHost, config, port), log.Joined);
                return (log, await CanConnectAsync(IPAddress.Loopback, port), await CanConnectAsync(lan!, port));
            }
            finally
            {
                await mcpHost.DisposeFailedStartAsync();
            }
        }
        finally
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
        }
    }

    private static PostgresConfig Unreachable(bool web, bool ownerNamesUser)
    {
        var closed = ClosedPort();
        var dedicated = $"Host=127.0.0.1;Port={closed};Username=dashboard;Password=dash-pw;Database=darlingtest";
        return new PostgresConfig
        {
            Managed = false,
            ConnectionString = ownerNamesUser
                ? $"Host=127.0.0.1;Port={closed};Username=darling;Password=x;Database=darlingtest"
                : $"Host=127.0.0.1;Port={closed};Database=darlingtest",
            WebConnectionString = web ? dedicated : null,
            McpConnectionString = web ? null : dedicated,
        };
    }

    /// <summary>The host start that a refused probe or an owner string with no user leaves with no network listener: the loopback address answers
    /// and the machine's network address does not, and the Critical line is the one for that reason, on both hosts.</summary>
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task AHostThatRefusesItsSurfaceLogin_BindsNoNetworkListener(bool web, bool ownerNamesUser)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var (log, loopback, network) = await StartHostAsync(web, Unreachable(web, ownerNamesUser));

        Assert.True(loopback, "the loopback listener must be up");
        Assert.False(network, "no network listener may be bound");
        var refusal = ownerNamesUser ? "could not be checked first" : DarlingStoreLogins.OwnerUserUnknownText(surface);
        Assert.Contains(log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal) && l.Contains(refusal, StringComparison.Ordinal));
    }

    /* ---- A probe that does not answer, and a stop that interrupts it ---- */

    /// <summary>A listener that accepts and then goes quiet, standing in for a store that does not answer. With <c>answerStartup</c> it speaks just
    /// enough of the PostgreSQL protocol to finish the connection (authentication OK, ready for query) and then never answers a command.</summary>
    private sealed class QuietStore : IDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly bool _answerStartup;

        public QuietStore(bool answerStartup)
        {
            _answerStartup = answerStartup;
            _listener.Start();
            _ = Task.Run(AcceptAsync);
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _ = Task.Run(() => ServeAsync(client));
                }
            }
            catch (Exception)
            {
                /* stopped */
            }
        }

        private static byte[] Message(char type, byte[] body)
        {
            var bytes = new byte[5 + body.Length];
            bytes[0] = (byte)type;
            BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(1, 4), 4 + body.Length);
            body.CopyTo(bytes, 5);
            return bytes;
        }

        private static byte[] ParameterStatus(string name, string value) =>
            Message('S', System.Text.Encoding.ASCII.GetBytes(name + "\0" + value + "\0"));

        private async Task ServeAsync(TcpClient client)
        {
            using var owned = client;
            try
            {
                var stream = client.GetStream();
                var buffer = new byte[8192];
                var read = await stream.ReadAsync(buffer, _stop.Token);
                if (_answerStartup && read >= 8 && BinaryPrimitives.ReadInt32BigEndian(buffer.AsSpan(4, 4)) == 196608)
                {
                    var reply = new List<byte>();
                    reply.AddRange(Message('R', new byte[4]));
                    foreach (var (name, value) in new[] { ("server_version", "16.0"), ("client_encoding", "UTF8"), ("integer_datetimes", "on"),
                        ("standard_conforming_strings", "on"), ("DateStyle", "ISO, MDY"), ("TimeZone", "UTC") })
                    {
                        reply.AddRange(ParameterStatus(name, value));
                    }

                    reply.AddRange(Message('K', new byte[] { 0, 0, 0, 1, 0, 0, 0, 1 }));
                    reply.AddRange(Message('Z', new byte[] { (byte)'I' }));
                    await stream.WriteAsync(reply.ToArray(), _stop.Token);
                }

                while (await stream.ReadAsync(buffer, _stop.Token) > 0)
                {
                    /* never answers */
                }
            }
            catch (Exception)
            {
                /* the probe gave up, or the store stopped */
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _stop.Dispose();
        }
    }

    private static string QuietConnectionString(QuietStore store) =>
        $"Host=127.0.0.1;Port={store.Port};Username=dashboard;Password=dash-pw;Database=darling;SSL Mode=Disable;"
        + "Server Compatibility Mode=NoTypeLoading;Cancellation Timeout=100";

    /// <summary>A store that never answers the connect, and one that connects and never answers the command, are each a failed probe within the
    /// probe's own bound, never an exception.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AProbeThatTimesOut_IsAFailedProbe_AndNeverThrows(bool answerStartup)
    {
        using var store = new QuietStore(answerStartup);
        var postgres = OwnStore(Owner, webConnectionString: QuietConnectionString(store));

        var result = await DarlingStoreLogins.ProbeSurfaceLoginAsync(
            DarlingStoreLogins.Surface.Web, postgres, TestContext.Current.CancellationToken, timeoutSeconds: 1);

        Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Failed, result.Verdict);
        Assert.False(string.IsNullOrEmpty(result.ErrorClass));
    }

    /// <summary>The stop token cancelling the probe is the one cancellation that propagates: the service is stopping, so no verdict is made.</summary>
    [Fact]
    public async Task AStopToken_AlreadyCancelled_PropagatesTheCancellation_NotAVerdict()
    {
        using var store = new QuietStore(answerStartup: false);
        var postgres = OwnStore(Owner, webConnectionString: QuietConnectionString(store));
        using var stop = new CancellationTokenSource();
        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            DarlingStoreLogins.ProbeSurfaceLoginAsync(DarlingStoreLogins.Surface.Web, postgres, stop.Token));
    }

    /* ---- A container: two strings that name no user run as the same user ---- */

    [Theory]
    [MemberData(nameof(Surfaces))]
    public void InAContainer_ASurfaceStringAndAnOwnerStringThatBothNameNoUser_RunAsTheOwner(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        const string noUser = "Host=pg.example;Database=darling";

        void Report(string configured, string owner, CapturingTestLogger logger)
        {
            DarlingStoreLogins.ResetContainerExposureLogForTests();
            DarlingStoreLogins.ReportContainerExposureOnOwnerLogin(
                surface, new DarlingStoreLogins.StoreLogin(configured, DarlingStoreLogins.LoginSource.Configured, null), owner,
                exposed: true, inContainer: true, logger);
        }

        var bothNoUser = new CapturingTestLogger();
        Report(noUser, noUser, bothNoUser);
        Assert.Equal(1, bothNoUser.CountAtLevel(LogLevel.Critical));
        Assert.NotNull(DarlingStoreLogins.ConfiguredLoginWarning(surface, noUser, noUser, verdict: null));

        /* A string that names a user is not the owner's, and a string that names none beside an owner that does is not known to be. */
        var named = new CapturingTestLogger();
        Report(Dedicated, noUser, named);
        Assert.Equal(0, named.CountAtLevel(LogLevel.Critical));
        var unnamed = new CapturingTestLogger();
        Report(noUser, Owner, unnamed);
        Assert.Equal(0, unnamed.CountAtLevel(LogLevel.Critical));
        Assert.Null(DarlingStoreLogins.ConfiguredLoginWarning(surface, noUser, Owner, verdict: null));

        /* The same user is still the owner. */
        var same = new CapturingTestLogger();
        Report(Owner, Owner, same);
        Assert.Equal(1, same.CountAtLevel(LogLevel.Critical));
    }

    /* ---- Item 2 against a real store ---- */

    /// <summary>The probe against a real server: a superuser and a member of the owner role are refused whatever their names, and a login that
    /// is neither passes, through the same decision a host start takes. A login the store refuses is a failed probe.</summary>
    [Fact]
    public async Task TheProbe_RefusesASuperuserAndAnOwnerRoleMember_AndPassesALeastPrivilegeLogin()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live login probe test (it mints its own scratch database and logins).");

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var superName = "probe_super_" + suffix;
        var memberName = "probe_member_" + suffix;
        var webName = "probe_web_" + suffix;
        var password = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var ownerNameCommand = owner.CreateCommand("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = 'config'"))
            {
                var ownerName = (string)(await ownerNameCommand.ExecuteScalarAsync(ct))!;
                await using var create = owner.CreateCommand(
                    $"CREATE ROLE {superName} LOGIN SUPERUSER PASSWORD '{password}'; "
                    + $"CREATE ROLE {memberName} LOGIN NOSUPERUSER PASSWORD '{password}'; GRANT \"{ownerName}\" TO {memberName}; "
                    + $"CREATE ROLE {webName} LOGIN NOSUPERUSER PASSWORD '{password}'");
                await create.ExecuteNonQueryAsync(ct);
            }

            string As(string user, string pw) => new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = user,
                Password = pw,
                Pooling = false,
            }.ConnectionString;

            async Task<(DarlingHostBinding.BindDecision Bind, DarlingStoreLogins.LoginProbeResult? Probe)> HostStartAsync(string surfaceLogin)
            {
                var postgres = OwnStore(Owner, webConnectionString: surfaceLogin);
                return await DarlingHostBinding.ResolveBindWithLoginProbeAsync(
                    DarlingStoreLogins.Surface.Web, postgres, inContainer: false,
                    ResolveFor(DarlingStoreLogins.Surface.Web, postgres, inContainer: false), ct);
            }

            var superuser = await HostStartAsync(As(superName, password));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Privileged, superuser.Probe?.Verdict);
            Assert.Equal(DarlingHostBinding.BindReason.LoginPrivileged, superuser.Bind.Reason);
            Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, superuser.Bind.Mode);

            var member = await HostStartAsync(As(memberName, password));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Privileged, member.Probe?.Verdict);
            Assert.Equal(DarlingHostBinding.BindReason.LoginPrivileged, member.Bind.Reason);

            var web = await HostStartAsync(As(webName, password));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Unprivileged, web.Probe?.Verdict);
            Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, web.Bind.Reason);
            Assert.Equal(DarlingHostBinding.BindMode.NetworkAndLoopback, web.Bind.Mode);

            /* A login the store refuses (no such role) is a failed probe with the SQLSTATE class, never the password. */
            var refused = await HostStartAsync(As("probe_missing_" + suffix, password));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Failed, refused.Probe?.Verdict);
            Assert.Equal(DarlingHostBinding.BindReason.LoginProbeFailed, refused.Bind.Reason);
            Assert.False(string.IsNullOrEmpty(refused.Probe?.ErrorClass));
            Assert.DoesNotContain(password, refused.Probe?.ErrorClass ?? "", StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {superName}; DROP ROLE IF EXISTS {memberName}; DROP ROLE IF EXISTS {webName}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>The probe against a real server, for the logins the first live test does not reach: a login whose role is switched at connect (the
    /// session login is a superuser while the connected role is not), a member of the role that owns a store table when another role owns the
    /// schema, and a store with no config schema or none of its tables.</summary>
    [Fact]
    public async Task TheProbe_RefusesASwitchedSuperuserAndATableOwnerMember_AndFailsWithoutTheConfigTables()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live login probe test (it mints its own scratch database and logins).");

        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var switchedName = "probe_switched_" + suffix;
        var limitedName = "probe_limited_" + suffix;
        var tableOwnerName = "probe_tblown_" + suffix;
        var tableMemberName = "probe_tblmem_" + suffix;
        var plainName = "probe_plain_" + suffix;
        var password = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var empty = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var create = owner.CreateCommand(
                $"CREATE ROLE {limitedName} NOLOGIN NOSUPERUSER; "
                + $"CREATE ROLE {switchedName} LOGIN SUPERUSER PASSWORD '{password}'; ALTER ROLE {switchedName} SET role = {limitedName}; "
                + $"CREATE ROLE {tableOwnerName} NOLOGIN; GRANT {tableOwnerName} TO CURRENT_USER; "
                + $"CREATE ROLE {tableMemberName} LOGIN NOSUPERUSER PASSWORD '{password}'; GRANT {tableOwnerName} TO {tableMemberName}; "
                + $"CREATE ROLE {plainName} LOGIN NOSUPERUSER PASSWORD '{password}'"))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            string As(NpgsqlConnectionStringBuilder from, string user) => new NpgsqlConnectionStringBuilder(from.ConnectionString)
            {
                Username = user,
                Password = password,
                Pooling = false,
            }.ConnectionString;
            var storeBuilder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString);

            Task<DarlingStoreLogins.LoginProbeResult> ProbeAsync(string surfaceLogin) =>
                DarlingStoreLogins.ProbeSurfaceLoginAsync(
                    DarlingStoreLogins.Surface.Web, OwnStore(Owner, webConnectionString: surfaceLogin), ct);

            /* A login that connects as a limited role but started as a superuser (it can SET ROLE back). */
            var switched = await ProbeAsync(As(storeBuilder, switchedName));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Privileged, switched.Verdict);

            /* A login that is neither passes, so the refusals above are not a probe that refuses everything. */
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Unprivileged, (await ProbeAsync(As(storeBuilder, plainName))).Verdict);

            /* A member of the role that owns a store table, while another role owns the config schema. */
            await using (var move = owner.CreateCommand($"ALTER TABLE config.config_monitored_servers OWNER TO {tableOwnerName}"))
            {
                await move.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Privileged, (await ProbeAsync(As(storeBuilder, tableMemberName))).Verdict);
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Unprivileged, (await ProbeAsync(As(storeBuilder, plainName))).Verdict);

            /* No config schema (a store the migrations never reached), and a config schema without the table: no row, so a failed probe. */
            var noSchema = await ProbeAsync(As(new NpgsqlConnectionStringBuilder(empty.ConnectionString), plainName));
            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Failed, noSchema.Verdict);
            await using (var drop = owner.CreateCommand("DROP TABLE config.config_monitored_servers CASCADE"))
            {
                await drop.ExecuteNonQueryAsync(ct);
            }

            Assert.Equal(DarlingStoreLogins.LoginProbeVerdict.Failed, (await ProbeAsync(As(storeBuilder, plainName))).Verdict);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand(
                    $"DROP OWNED BY {tableOwnerName}; DROP ROLE IF EXISTS {switchedName}; DROP ROLE IF EXISTS {tableMemberName}; DROP ROLE IF EXISTS {plainName}; "
                    + $"DROP ROLE IF EXISTS {tableOwnerName}; DROP ROLE IF EXISTS {limitedName}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /// <summary>A started host keeps no network listener for a superuser login, and binds one for a least-privilege login, on both hosts, against a
    /// real store (the second half is the control: the check that finds no listener must be able to find one).</summary>
    [Theory]
    [MemberData(nameof(Surfaces))]
    public async Task AHostStart_BindsNoNetworkListenerForASuperuserLogin_AndOneForALeastPrivilegeLogin(bool web)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live login probe test (it mints its own scratch database and logins).");

        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var superName = "probe_hsuper_" + suffix;
        var plainName = "probe_hplain_" + suffix;
        var password = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await using (var create = owner.CreateCommand(
                $"CREATE ROLE {superName} LOGIN SUPERUSER PASSWORD '{password}'; CREATE ROLE {plainName} LOGIN NOSUPERUSER PASSWORD '{password}'"))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            PostgresConfig Store(string user)
            {
                var login = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { Username = user, Password = password, Pooling = false }.ConnectionString;
                return new PostgresConfig
                {
                    Managed = false,
                    ConnectionString = scratch.ConnectionString,
                    WebConnectionString = web ? login : null,
                    McpConnectionString = web ? null : login,
                };
            }

            var privileged = await StartHostAsync(web, Store(superName));
            Assert.True(privileged.LoopbackReachable, "the loopback listener must be up");
            Assert.False(privileged.NetworkReachable, "a superuser login must leave no network listener");
            Assert.Contains(privileged.Log.Lines, l => l.StartsWith("Critical:", StringComparison.Ordinal)
                && l.Contains(DarlingStoreLogins.PrivilegedLoginText(surface), StringComparison.Ordinal));

            var unprivileged = await StartHostAsync(web, Store(plainName));
            Assert.True(unprivileged.NetworkReachable, "a least-privilege login is exposed, and this check must see the listener: " + unprivileged.Log.Joined);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {superName}; DROP ROLE IF EXISTS {plainName}", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
