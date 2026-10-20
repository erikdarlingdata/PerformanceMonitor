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
}
