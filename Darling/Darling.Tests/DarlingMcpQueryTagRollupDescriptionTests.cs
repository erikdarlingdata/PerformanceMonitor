/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6305: <c>get_resource_use_by_tag</c> answered the query_store section <c>unavailable</c> for hours_back 4 because the
/// section is only read for windows of <see cref="DarlingQueryTagRollupReader.QueryStoreMinWindow"/> or more, and neither the
/// tool's opening description nor its hours_back argument said so. An agent reads those when it picks a value.
/// </summary>
public sealed class DarlingMcpQueryTagRollupDescriptionTests
{
    private static MethodInfo Tool => typeof(DarlingMcpQueryTagRollupTools).GetMethod(nameof(DarlingMcpQueryTagRollupTools.GetResourceUseByTag))!;

    [Fact]
    public void TheHoursBackArgument_NamesTheQueryStoreMinimumTheReaderEnforces()
    {
        var hoursBack = Tool.GetParameters().Single(p => p.Name == "hours_back");
        var text = hoursBack.GetCustomAttribute<DescriptionAttribute>()!.Description;

        Assert.Contains("query_store", text, StringComparison.Ordinal);
        Assert.Contains($"{DarlingQueryTagRollupReader.QueryStoreMinWindow.TotalHours:0} or more", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheToolDescription_SaysTheQueryStoreSectionNeedsTheMinimum_BeforeTheGuide()
    {
        var text = Tool.GetCustomAttribute<DescriptionAttribute>()!.Description;
        var head = text[..text.IndexOf("<<GUIDE>>", StringComparison.Ordinal)];

        Assert.Contains($"query_store section needs hours_back {DarlingQueryTagRollupReader.QueryStoreMinWindow.TotalHours:0} or more", head, StringComparison.Ordinal);
    }
}
