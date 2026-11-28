/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9894: a store the service does not manage (<c>postgres.managed = false</c>), outside a container, exposes the web dashboard or the MCP
/// server to the network only when that surface's own connection string (<c>postgres.webConnectionString</c> /
/// <c>postgres.mcpConnectionString</c>) names a login other than the owner's. The two surfaces are judged separately. The bundled store and a
/// container keep today's behavior, and a container says so once at Critical where an exposed surface's login resolves to the owner's.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingOwnStoreExposureTests : IDisposable
{
    private const string Owner = "Host=pg.example;Database=darling;Username=postgres;Password=owner-secret";
    private const string WebRole = "Host=pg.example;Database=darling;Username=web;Password=web-secret";
    private const string McpRole = "Host=pg.example;Database=darling;Username=mcp;Password=mcp-secret";
    private const string Dedicated = "Host=pg.example;Database=darling;Username=dashboard;Password=d-secret";
    private const string Token = "a-long-token-value-0123456789";

    public DarlingOwnStoreExposureTests() => DarlingStoreLogins.ResetContainerExposureLogForTests();

    public void Dispose() => DarlingStoreLogins.ResetContainerExposureLogForTests();

    private static PostgresConfig OwnStore(string? webConnectionString = null, string? mcpConnectionString = null, bool managed = false) => new()
    {
        Managed = managed,
        ConnectionString = Owner,
        WebConnectionString = webConnectionString,
        McpConnectionString = mcpConnectionString,
    };

    private static McpConfig Mcp() => new()
    {
        Enabled = true,
        Network = new McpNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = Token },
    };

    private static WebConfig Web() => new()
    {
        Enabled = true,
        Network = new WebNetworkConfig { Listen = "192.168.1.205", AllowFrom = "192.168.1.0/24", Token = Token },
    };

    private static bool McpExposed(PostgresConfig postgres, bool inContainer = false)
        => DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres, inContainer).Mode == DarlingMcpHostService.McpBindMode.NetworkAndLoopback;

    private static bool WebExposed(PostgresConfig postgres, bool inContainer = false)
        => DarlingWebHostService.ResolveWebBind(Web(), postgres, inContainer).Mode == DarlingHostBinding.BindMode.NetworkAndLoopback;

    [Fact]
    public void OwnStore_WithDedicatedNonOwnerLogins_ExposesBothSurfaces()
    {
        var postgres = OwnStore(WebRole, McpRole);

        var web = DarlingWebHostService.ResolveWebBind(Web(), postgres, inContainer: false);
        var mcp = DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindReason.NetworkExposed, web.Reason);
        Assert.Equal(DarlingMcpHostService.McpBindReason.NetworkExposed, mcp.Reason);
        Assert.True(WebExposed(postgres));
        Assert.True(McpExposed(postgres));
    }

    [Fact]
    public void OwnStore_WithoutDedicatedLogins_StaysLoopback_DedicatedLoginRequired_AtCritical()
    {
        var postgres = OwnStore();

        var web = DarlingWebHostService.ResolveWebBind(Web(), postgres, inContainer: false);
        var mcp = DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres, inContainer: false);

        Assert.Equal(DarlingHostBinding.BindMode.LoopbackOnly, web.Mode);
        Assert.Equal(DarlingHostBinding.BindReason.DedicatedLoginRequired, web.Reason);
        Assert.Equal(DarlingMcpHostService.McpBindMode.LoopbackOnly, mcp.Mode);
        Assert.Equal(DarlingMcpHostService.McpBindReason.DedicatedLoginRequired, mcp.Reason);
        Assert.Equal(LogLevel.Critical, DarlingHostBinding.MapBindReasonSeverity(web.Reason));
        Assert.Equal(LogLevel.Critical, DarlingMcpHostService.MapBindReasonSeverity(mcp.Reason));
    }

    [Fact]
    public void OwnStore_ConnectionStringsNamingTheOwner_StayLoopback()
    {
        /* Another password, another database, or another host does not make the owner a different login: the user name decides. */
        var postgres = OwnStore(
            webConnectionString: "Host=other.example;Database=darling;Username=postgres;Password=other",
            mcpConnectionString: Owner);

        Assert.False(WebExposed(postgres));
        Assert.False(McpExposed(postgres));
        Assert.Equal(DarlingHostBinding.BindReason.DedicatedLoginRequired, DarlingWebHostService.ResolveWebBind(Web(), postgres, inContainer: false).Reason);
        Assert.Equal(DarlingMcpHostService.McpBindReason.DedicatedLoginRequired, DarlingMcpHostService.ResolveMcpBind(Mcp(), postgres, inContainer: false).Reason);
    }

    [Fact]
    public void OwnStore_WebAndMcpAreJudgedSeparately()
    {
        var onlyWeb = OwnStore(webConnectionString: WebRole);
        Assert.True(WebExposed(onlyWeb));
        Assert.False(McpExposed(onlyWeb));

        var onlyMcp = OwnStore(mcpConnectionString: McpRole);
        Assert.False(WebExposed(onlyMcp));
        Assert.True(McpExposed(onlyMcp));

        /* The other surface's login does not stand in for this one's. */
        var webStringIsTheOwner = OwnStore(webConnectionString: Owner, mcpConnectionString: McpRole);
        Assert.False(WebExposed(webStringIsTheOwner));
        Assert.True(McpExposed(webStringIsTheOwner));
    }

    [Fact]
    public void OwnStore_DedicatedLoginGiven_AsAnEnvReference_IsReadTheWayTheHostReadsIt()
    {
        var name = "DARLING_TEST_9894_WEB_" + Guid.NewGuid().ToString("N");
        Environment.SetEnvironmentVariable(name, WebRole);
        try
        {
            Assert.True(WebExposed(OwnStore(webConnectionString: "env:" + name)));
        }
        finally
        {
            Environment.SetEnvironmentVariable(name, null);
        }
    }

    [Fact]
    public void OwnStore_DedicatedLoginThatCannotBeRead_StaysLoopback()
    {
        /* An unset variable, and a string Npgsql cannot parse: neither can be shown to be a login other than the owner's. */
        Assert.False(WebExposed(OwnStore(webConnectionString: "env:DARLING_TEST_9894_NEVER_SET_" + Guid.NewGuid().ToString("N"))));
        Assert.False(WebExposed(OwnStore(webConnectionString: "this is not a connection string")));
        Assert.False(McpExposed(OwnStore(mcpConnectionString: "Host=pg.example;Database=darling")));
    }

    [Fact]
    public void ManagedStore_ExposesWithoutAnyDedicatedConnectionString()
    {
        var postgres = OwnStore(managed: true);

        Assert.True(WebExposed(postgres));
        Assert.True(McpExposed(postgres));
    }

    [Fact]
    public void Container_OwnStore_StillExposesWithoutADedicatedLogin()
    {
        var postgres = OwnStore();

        Assert.True(WebExposed(postgres, inContainer: true));
        Assert.True(McpExposed(postgres, inContainer: true));
    }

    /* The surface's login is resolved the way the hosts resolve it (the container marker is the ambient one), with the bind result passed in. */
    private static async Task<string?> ResolveInContainerAsync(
        DarlingStoreLogins.Surface surface, PostgresConfig postgres, ILogger logger, bool exposed,
        DarlingStoreLogins.ComposeStoreVerdict? verdict)
    {
        var previousContainer = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER");
        try
        {
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", "true");
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            if (verdict is not null)
            {
                DarlingStoreLogins.PublishComposeStoreVerdict(verdict);
            }

            return await DarlingStoreLogins.ResolveUnmanagedAsync(surface, postgres, logger, TestContext.Current.CancellationToken, exposed);
        }
        finally
        {
            DarlingStoreLogins.ResetComposeStoreVerdictForTests();
            Environment.SetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER", previousContainer);
        }
    }

    private static DarlingStoreLogins.ComposeStoreVerdict Provisioned() =>
        DarlingStoreLogins.ComposeStoreVerdict.ProvisionedWith(
            "Host=pg.example;Database=darling;Username=viewer;Password=v", McpRole, 15, WebRole);

    /// <summary>A not-provisioned verdict whose credentials directory is an empty one, so no earlier start's credential stands in.</summary>
    private static DarlingStoreLogins.ComposeStoreVerdict NotProvisionedWithNoEarlierCredential(string directory) =>
        DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("refused", directory);

    /// <summary>
    /// The compose store provisioned the web and mcp roles, so an exposed surface with no login of its own runs as that role:
    /// nothing logs. Before #9894's move this logged a Critical at bind time for every compose start with exposure.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Container_ComposeRolesProvisioned_ExposedSurface_LogsNothing(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();

        var login = await ResolveInContainerAsync(surface, OwnStore(), logger, exposed: true, Provisioned());

        Assert.Equal(web ? WebRole : McpRole, login);
        Assert.Empty(logger.Lines);
    }

    /// <summary>
    /// A surface on the role credential an earlier start left runs as that role, not the owner: the resolved login is not the owner's, so
    /// the exposed surface logs no Critical.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Container_EarlierStartRoleCredential_ExposedSurface_LogsNoCritical(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();
        var verdict = DarlingStoreLogins.ComposeStoreVerdict.NotProvisioned("refused");

        var login = DarlingStoreLogins.Resolve(
            surface, Owner, null, inContainer: true, verdict, EarlierComposeCredential.Trusted("earlier-secret"));
        DarlingStoreLogins.ReportContainerExposureOnOwnerLogin(surface, login, Owner, exposed: true, inContainer: true, logger);

        Assert.Equal(DarlingStoreLogins.LoginSource.ComposeStoreRoleFromEarlierStart, login.Source);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Critical));
    }

    /// <summary>The owner fallback on an exposed surface says so once at Critical, however many times the host start retries.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Container_OwnerFallback_ExposedSurface_LogsCriticalOncePerProcess(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();
        var directory = Path.Combine(Path.GetTempPath(), "darling-9894-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var first = await ResolveInContainerAsync(surface, OwnStore(), logger, exposed: true, NotProvisionedWithNoEarlierCredential(directory));
            var second = await ResolveInContainerAsync(surface, OwnStore(), logger, exposed: true, NotProvisionedWithNoEarlierCredential(directory));

            Assert.Equal(Owner, first);
            Assert.Equal(Owner, second);
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Not exposed, the owner fallback keeps today's Warning and logs no Critical.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Container_OwnerFallback_SurfaceNotExposed_KeepsTheWarning_NoCritical(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();
        var directory = Path.Combine(Path.GetTempPath(), "darling-9894-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var login = await ResolveInContainerAsync(surface, OwnStore(), logger, exposed: false, NotProvisionedWithNoEarlierCredential(directory));

            Assert.Equal(Owner, login);
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Critical));
            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A configured login whose user is the owner's runs as the owner after all: exposed, that is the Critical.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Container_ConfiguredLoginThatIsTheOwner_ExposedSurface_LogsCritical(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();
        var postgres = web ? OwnStore(webConnectionString: Owner) : OwnStore(mcpConnectionString: Owner);

        var login = await ResolveInContainerAsync(surface, postgres, logger, exposed: true, Provisioned());

        Assert.Equal(Owner, login);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
    }

    /// <summary>A configured login that is not the owner's, exposed, is the dedicated login the Critical asks for: nothing logs.</summary>
    [Fact]
    public async Task Container_ConfiguredDedicatedLogin_ExposedSurface_LogsNothing()
    {
        var logger = new CapturingTestLogger();

        var login = await ResolveInContainerAsync(
            DarlingStoreLogins.Surface.Web, OwnStore(webConnectionString: Dedicated), logger, exposed: true, Provisioned());

        Assert.Equal(Dedicated, login);
        Assert.Empty(logger.Lines);
    }

    /// <summary>The two surfaces are judged and logged separately.</summary>
    [Fact]
    public void Container_OwnerLogin_SurfacesAreLoggedSeparately()
    {
        var logger = new CapturingTestLogger();
        var owner = new DarlingStoreLogins.StoreLogin(Owner, DarlingStoreLogins.LoginSource.Owner, null);

        DarlingStoreLogins.ReportContainerExposureOnOwnerLogin(DarlingStoreLogins.Surface.Web, owner, Owner, exposed: true, inContainer: true, logger);
        DarlingStoreLogins.ReportContainerExposureOnOwnerLogin(DarlingStoreLogins.Surface.Mcp, owner, Owner, exposed: true, inContainer: true, logger);

        Assert.Equal(2, logger.CountAtLevel(LogLevel.Critical));
    }

    /// <summary>Outside a container the bind decision refuses and logs the line itself, so the owner login logs nothing more here.</summary>
    [Fact]
    public void OutsideAContainer_OwnerLogin_ExposedSurface_LogsNothing()
    {
        var logger = new CapturingTestLogger();
        var owner = new DarlingStoreLogins.StoreLogin(Owner, DarlingStoreLogins.LoginSource.Owner, null);

        DarlingStoreLogins.ReportContainerExposureOnOwnerLogin(DarlingStoreLogins.Surface.Web, owner, Owner, exposed: true, inContainer: false, logger);

        Assert.Empty(logger.Lines);
    }
}
