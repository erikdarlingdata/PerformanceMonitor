/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#7386: the body of <c>/api/server-data-start</c>. The page parses <c>data_start_utc</c> with <c>Date.parse</c>, so it
/// must be an ISO instant ending in Z whatever kind the store's timestamp came back as, and null when the server has no row.</summary>
public sealed class ServerDataStartEndpointTests
{
    [Theory]
    [InlineData(DateTimeKind.Utc)]
    [InlineData(DateTimeKind.Unspecified)]
    public void TheBody_CarriesTheDataStartAsAnIsoInstantInUtc(DateTimeKind kind)
    {
        var start = new DateTime(2026, 11, 3, 8, 56, 24, 123, kind);

        using var doc = JsonDocument.Parse(DarlingServerDataStartEndpoint.Render("SRV1", start));

        Assert.Equal("SRV1", doc.RootElement.GetProperty("server").GetString());
        Assert.Equal("2026-11-03T08:56:24.123Z", doc.RootElement.GetProperty("data_start_utc").GetString());
    }

    [Fact]
    public void AServerWithNoRowYet_HasANullDataStart()
    {
        using var doc = JsonDocument.Parse(DarlingServerDataStartEndpoint.Render("SRV1", null));

        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("data_start_utc").ValueKind);
    }
}
