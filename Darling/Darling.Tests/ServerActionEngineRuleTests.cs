/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6727: the rule a server action switch is checked against. An Azure SQL Database has no plan handle eviction and no plan guides, and an
/// Azure SQL Managed Instance has no plan guides: the tools refuse those every time, so the switch must not turn on for them. A SQL Server
/// target with no probed edition answers as before.
/// </summary>
public sealed class ServerActionEngineRuleTests
{
    [Theory]
    [InlineData("force_plan", 5, true)]
    [InlineData("evict_plan", 5, false)]
    [InlineData("kill_session", 5, true)]
    [InlineData("recompile_module", 5, true)]
    [InlineData("create_plan_guide", 5, false)]
    [InlineData("drop_plan_guide", 5, false)]
    [InlineData("force_plan", 8, true)]
    [InlineData("evict_plan", 8, true)]
    [InlineData("kill_session", 8, true)]
    [InlineData("recompile_module", 8, true)]
    [InlineData("create_plan_guide", 8, false)]
    [InlineData("drop_plan_guide", 8, false)]
    [InlineData("force_plan", 3, true)]
    [InlineData("evict_plan", 3, true)]
    [InlineData("kill_session", 3, true)]
    [InlineData("recompile_module", 3, true)]
    [InlineData("create_plan_guide", 3, true)]
    [InlineData("drop_plan_guide", 3, true)]
    public void ASqlServerTargetsEdition_DecidesWhichActionsItCanTake(string action, int edition, bool expected)
    {
        Assert.Equal(expected, ServerActionNames.IsValidForEngine(action, postgres: false, edition));
    }

    [Theory]
    [InlineData("force_plan", true)]
    [InlineData("evict_plan", true)]
    [InlineData("kill_session", true)]
    [InlineData("recompile_module", true)]
    [InlineData("create_plan_guide", true)]
    [InlineData("drop_plan_guide", true)]
    [InlineData("not_an_action", false)]
    [InlineData(null, false)]
    public void ANullEdition_AnswersAsAnOrdinarySqlServer_AndAnUnknownNameIsNeverValid(string? action, bool expected)
    {
        Assert.Equal(expected, ServerActionNames.IsValidForEngine(action, postgres: false, sqlEngineEdition: null));
    }

    [Theory]
    [InlineData("force_plan", false)]
    [InlineData("evict_plan", false)]
    [InlineData("kill_session", true)]
    [InlineData("recompile_module", false)]
    [InlineData("create_plan_guide", false)]
    [InlineData("drop_plan_guide", false)]
    public void APostgreSqlTarget_TakesOnlyKillSession_WhateverEditionTheRegistryHolds(string action, bool expected)
    {
        Assert.Equal(expected, ServerActionNames.IsValidForEngine(action, postgres: true, sqlEngineEdition: null));
        Assert.Equal(expected, ServerActionNames.IsValidForEngine(action, postgres: true, sqlEngineEdition: 5));
    }
}
