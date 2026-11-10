/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

public sealed class ManagedWebGroupCookieTests
{
    [Fact]
    public void IdentityCookiesCarryOnlyImmutableIdentifiersAndNeverAStoredGrant()
    {
        var id=Guid.NewGuid();var generation=Guid.NewGuid();
        Assert.True(DarlingWebSeat.TryResolveSeat(DarlingWebSeat.EncodeGroupCookieSubject(id,generation),out var seat));
        Assert.Equal(id,seat.GroupId);Assert.Equal(generation,seat.GroupGeneration);
        Assert.False(seat.CanEdit);Assert.Null(seat.OwnerKey);Assert.Null(seat.TokenName);
        Assert.Null(seat.AllowedTagIds);
        foreach(var bad in new[]{"g1:","g1:wrong:wrong","g1:"+Guid.Empty.ToString("N")+":"+generation.ToString("N"),
            "g1:"+id.ToString("N")+":"+Guid.Empty.ToString("N"),"g1:"+id.ToString("N")+":"+generation.ToString("N")+":admin"})
            Assert.False(DarlingWebSeat.TryResolveSeat(bad,out _));
    }
}
