/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7988: the part of a scoped web token's denial that needs no store. Under a grant the resolver reaches a server only inside
/// the allow-list, and under a pinned server only that server; every other name gets the one fixed refusal, which names no
/// server. The reads that filter rows by the grant are proven against rows in <see cref="ScopedWebTokenLiveTests"/> and
/// <see cref="ScopedSweepBatchLiveTests"/>.
/// </summary>
public sealed class ScopedServerResolverTests
{
    private static readonly RegisteredServer[] Registry =
    [
        new RegisteredServer(41, "finance-server", "finance-server"),
        new RegisteredServer(42, "operations-server", "operations-server"),
        new RegisteredServer(43, "prod-only-server", "prod-only-server"),
        new RegisteredServer(44, "untagged-server", "untagged-server"),
    ];

    private const string Denied = "Server data is unavailable to this token.";

    [Fact]
    public async Task UnderAGrant_TheResolverAnswersOnlyInsideTheAllowList_AndRefusesTheRestTheSameWay()
    {
        await DarlingServerResolver.ReadWebScopeAsync([41], [7], () =>
        {
            var inScope = DarlingServerResolver.ResolveOrError(Registry, "finance-server");
            Assert.Null(inScope.error);
            Assert.Equal(41, inScope.resolved.ServerId);

            // A partial that matches every registered name resolves to the one server inside the grant, never to an ambiguity.
            var partial = DarlingServerResolver.ResolveOrError(Registry, "server");
            Assert.Null(partial.error);
            Assert.Equal(41, partial.resolved.ServerId);

            foreach (var outside in new[] { "operations-server", "operations", "prod-only-server", "untagged-server", "no-such-server" })
            {
                var refused = DarlingServerResolver.ResolveOrError(Registry, outside);
                Assert.NotNull(refused.error);
                Assert.Contains(Denied, refused.error, StringComparison.Ordinal);
                Assert.DoesNotContain("operations-server", refused.error, StringComparison.Ordinal);
                Assert.DoesNotContain("prod-only-server", refused.error, StringComparison.Ordinal);
            }

            return Task.FromResult(0);
        });

        // The same registry with no grant: the hidden server resolves, and a partial over four servers is the ordinary ambiguity.
        Assert.Equal(42, DarlingServerResolver.ResolveOrError(Registry, "operations-server").resolved.ServerId);
        Assert.NotNull(DarlingServerResolver.ResolveOrError(Registry, "server").error);
        Assert.DoesNotContain(Denied, DarlingServerResolver.ResolveOrError(Registry, "server").error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnderAPinnedServer_TheResolverRefusesEveryOtherServer_EvenInsideTheAllowList()
    {
        await DarlingServerResolver.ReadWebScopeAsync([41, 42], [7], () =>
            DarlingServerResolver.ReadWebServerAsync(41, () =>
            {
                Assert.Equal(41, DarlingServerResolver.ResolveOrError(Registry, "finance-server").resolved.ServerId);
                var other = DarlingServerResolver.ResolveOrError(Registry, "operations-server");
                Assert.NotNull(other.error);
                Assert.Contains(Denied, other.error, StringComparison.Ordinal);
                return Task.FromResult(0);
            }));
    }
}
