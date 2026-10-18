/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling's half of the cross-app investigation tool contract (#5820): its four tools keep the parameters and status words in
/// <see cref="InvestigationToolContract"/>, the table Lite's tools are held to by the same reflection read, so the two
/// products cannot drift apart on a name, a type, a default or a status word.
/// </summary>
public sealed class InvestigationToolContractTests
{
    [Fact]
    public void DarlingsInvestigationTools_KeepTheSharedContract() =>
        Assert.Empty(InvestigationToolContract.Differences(typeof(DarlingMcpInvestigationTools), InvestigationToolContract.DarlingOnlyParameters));
}
