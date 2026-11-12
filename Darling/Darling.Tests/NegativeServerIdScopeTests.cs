/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common.KnownPatterns;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7975: a Darling server id is a 32-bit hash of the storage name, so about half of all servers have a negative one. A server scope
/// (an environment profile, a known pattern) must accept any nonzero id and leave the registry check to say whether the server exists.
/// Id 0 stays refused: the stores match a scope on <c>coalesce(scope_server_id, 0)</c>, so a profile for server 0 would stand in the fleet
/// profile's place, and 0 is the fleet-scope sentinel elsewhere.
/// </summary>
public sealed class NegativeServerIdScopeTests
{
    [Theory]
    [InlineData(-1)]
    [InlineData(-1_714_925_380)]
    [InlineData(int.MinValue)]
    [InlineData(1)]
    [InlineData(int.MaxValue)]
    public void AProfileForAnyNonzeroServerId_PassesValidate(int serverId) =>
        Assert.Null(InvestigationProfiles.Validate(serverId, null, "payroll runs 01:00-03:00"));

    [Fact]
    public void AProfileForServer0_IsStillRefused() =>
        Assert.Contains("Server id 0", InvestigationProfiles.Validate(0, null, "text"), StringComparison.Ordinal);

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ATagIdBelow1_IsStillRefused(int tagId) =>
        Assert.Contains("tag ids start at 1", InvestigationProfiles.Validate(null, tagId, "text"), StringComparison.Ordinal);

    [Theory]
    [InlineData(-1)]
    [InlineData(-1_714_925_380)]
    [InlineData(int.MinValue)]
    public void AKnownPatternForAnyNonzeroServerId_PassesValidate(int serverId) =>
        Assert.Null(new KnownPatternInput(serverId, null, null, null, null, "a note", null).Validate());

    [Fact]
    public void AKnownPatternForServer0_IsStillRefused() =>
        Assert.Contains("server_id must be a registered server's id",
            new KnownPatternInput(0, null, null, null, null, "a note", null).Validate(), StringComparison.Ordinal);
}
