/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// container keep today's behavior, and a container whose exposed surface has no login of its own says so once at Critical.
/// </summary>
public sealed class DarlingOwnStoreExposureTests : IDisposable
{
    private const string Owner = "Host=pg.example;Database=darling;Username=postgres;Password=owner-secret";
    private const string WebRole = "Host=pg.example;Database=darling;Username=web;Password=web-secret";
    private const string McpRole = "Host=pg.example;Database=darling;Username=mcp;Password=mcp-secret";
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Container_OwnStore_ExposedWithoutADedicatedLogin_LogsCriticalOncePerStart(bool web)
    {
        var surface = web ? DarlingStoreLogins.Surface.Web : DarlingStoreLogins.Surface.Mcp;
        var logger = new CapturingTestLogger();

        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(surface, OwnStore(), exposed: true, logger, inContainer: true);
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(surface, OwnStore(), exposed: true, logger, inContainer: true);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
        Assert.Single(logger.Lines);
    }

    [Fact]
    public void Container_OwnStore_ExposedWithTheOwnersLogin_LogsCritical()
    {
        var logger = new CapturingTestLogger();

        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(
            DarlingStoreLogins.Surface.Web, OwnStore(webConnectionString: Owner), exposed: true, logger, inContainer: true);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Critical));
    }

    [Fact]
    public void Container_OwnStore_SurfacesAreLoggedSeparately()
    {
        var logger = new CapturingTestLogger();
        var postgres = OwnStore();

        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Web, postgres, exposed: true, logger, inContainer: true);
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Mcp, postgres, exposed: true, logger, inContainer: true);

        Assert.Equal(2, logger.CountAtLevel(LogLevel.Critical));
    }

    [Fact]
    public void Container_OwnStore_ExposedWithADedicatedLogin_LogsNothing()
    {
        var logger = new CapturingTestLogger();
        var postgres = OwnStore(WebRole, McpRole);

        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Web, postgres, exposed: true, logger, inContainer: true);
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Mcp, postgres, exposed: true, logger, inContainer: true);

        Assert.Empty(logger.Lines);
    }

    [Fact]
    public void Container_Report_IsSilent_WhenNotExposed_Managed_OrOutsideAContainer()
    {
        var logger = new CapturingTestLogger();

        /* Not exposed: nothing was asked for. */
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Web, OwnStore(), exposed: false, logger, inContainer: true);
        /* The bundled store derives its own logins. */
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Web, OwnStore(managed: true), exposed: true, logger, inContainer: true);
        /* Outside a container the bind decision refuses and logs the same line itself, so this report stays out of it. */
        DarlingStoreLogins.ReportContainerExposureWithoutDedicatedLogin(DarlingStoreLogins.Surface.Web, OwnStore(), exposed: true, logger, inContainer: false);

        Assert.Empty(logger.Lines);
    }
}
