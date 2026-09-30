/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>audit_config</c> named an EngineEdition with a private switch that disagreed with every other surface
/// (6 is Azure Synapse Analytics, 8 is Managed Instance, and there is no "HADR" edition), and its empty answer
/// told every engine "the config collector may not have run yet" - false on Azure SQL Database, whose engine
/// never collects <c>server_config</c> at all, and where <c>get_server_config</c> already answers
/// <c>not_collected</c>.
///
/// <para>The naming is a pure function, pinned directly. The empty answer needs a registry row to read the
/// engine from, so its end-to-end half is in <c>EngineCapabilityMissLivePostgresTests</c> (gated on
/// <c>DARLING_TEST_PG</c>); what runs everywhere is the source pin that the tool's empty branch asks the same
/// engine question <c>get_server_config</c> does, for the same collector.</para>
/// </summary>
public sealed class AuditConfigEditionNameTests
{
    [Theory]
    [InlineData(8, "Azure SQL Managed Instance")]
    [InlineData(6, "Azure Synapse Analytics")]
    [InlineData(5, "Azure SQL Database")]
    [InlineData(3, "Enterprise")]
    [InlineData(2, "Standard")]
    [InlineData(0, "Unknown")]
    public void AuditEditionName_UsesTheSharedEditionTable_AndKeepsUnknownForAnUnprobedEdition(int engineEdition, string expected)
    {
        Assert.Equal(expected, DarlingMcpTools.AuditEditionName(engineEdition));
    }

    [Fact]
    public void AuditEditionName_NeverDisagreesWithTheSharedTable_ForAnEditionThatIsProbed()
    {
        for (var edition = 1; edition <= 12; edition++)
        {
            Assert.Equal(CollectorEngineCapability.DescribeEngineEdition(edition), DarlingMcpTools.AuditEditionName(edition));
        }
    }

    [Fact]
    public void AuditConfig_AsksTheEngineQuestion_BeforeItSaysTheConfigCollectorMayNotHaveRun()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var start = source.IndexOf("Name = \"audit_config\"", StringComparison.Ordinal);
        var body = source[start..source.IndexOf("FormatError(\"audit_config\"", StringComparison.Ordinal)];

        /* Exactly one gate, for the collector get_server_config gates on (see DarlingMcpConfigTools). */
        var gate = body.IndexOf("DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, \"server_config\"", StringComparison.Ordinal);
        Assert.True(gate >= 0, "audit_config's empty answer does not ask the engine question for server_config");
        Assert.Equal(body.IndexOf("NotCollectedStatusAsync(", StringComparison.Ordinal), body.LastIndexOf("NotCollectedStatusAsync(", StringComparison.Ordinal));

        /* And it comes before the SQL Server arm's own "no_config_data" (the PostgreSQL arm's precedes it). */
        Assert.True(gate < body.LastIndexOf("\"no_config_data\"", StringComparison.Ordinal));

        /* The private edition switch is gone: the tool names an edition through the shared table only. */
        Assert.Contains("AuditEditionName(edition)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("(HADR)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Azure Synapse serverless\"", body, StringComparison.Ordinal);
    }
}
