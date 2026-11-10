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
/// #7988: the pieces of a scoped web token's row-level denial that need no store, pinned so a dropped guard goes red on a pull
/// request. The resolver's allow-list and pin decide which server a name reaches; the SQL constants carry the grant's
/// <c>($N::integer[] IS NULL OR server_id = ANY($N))</c> clause, bound NULL for every unscoped caller; the job-history builder
/// and the compose compiler emit the grant's predicate from their own inputs.
/// </summary>
public sealed class ScopedServerResolverAndSqlPinTests
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

    [Fact]
    public void TheAlertHistoryStatements_CarryTheGrantClause_BoundNullForAnUnscopedCaller()
    {
        Assert.Contains("AND   ($7::integer[] IS NULL OR a.server_id = ANY($7))", DarlingAlertReader.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("AND   ($6::integer[] IS NULL OR a.server_id = ANY($6))", DarlingAlertReader.AlertHistoryAllServersSql, StringComparison.Ordinal);
        Assert.Contains("AND   ($5::integer[] IS NULL OR server_id = ANY($5))", DarlingAlertReader.DismissedAlertCountSql, StringComparison.Ordinal);
        Assert.Contains("AND   ($4::integer[] IS NULL OR server_id = ANY($4))", DarlingAlertReader.DismissedAlertCountAllServersSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheInvestigationStatements_CarryTheGrantClause()
    {
        Assert.Contains("AND ($6::integer[] IS NULL OR r.server_id = ANY($6))", InvestigationRequestStore.ListSql, StringComparison.Ordinal);
        Assert.Contains("AND ($6::integer[] IS NULL OR r.server_id = ANY($6))", InvestigationRequestStore.ReportsRequestsSql, StringComparison.Ordinal);
        Assert.Contains("AND ($3::integer[] IS NULL OR r.server_id = ANY($3))", InvestigationFeedbackStore.FamilyCountsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheJobHistoryStatement_BindsTheGrantAheadOfTheOptionalFilters_AndOnlyWhenOneIsSet()
    {
        var fleet = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, new JobHistoryFilter { AllowedServerIds = [41] });
        Assert.Contains("AND   jh.server_id = ANY($4)", fleet, StringComparison.Ordinal);
        var oneServer = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: true, new JobHistoryFilter { AllowedServerIds = [41] });
        Assert.Contains("AND   jh.server_id = ANY($5)", oneServer, StringComparison.Ordinal);
        var withFilters = DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, new JobHistoryFilter(JobName: "nightly", MinDurationSeconds: 5) { AllowedServerIds = [41] });
        Assert.Contains("AND   jh.server_id = ANY($4)", withFilters, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id = ANY(", DarlingJobHistoryReader.FleetSql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id = ANY(", DarlingJobHistoryReader.BuildJobHistorySql(scopedToServer: false, new JobHistoryFilter(JobName: "nightly")), StringComparison.Ordinal);
    }

    [Fact]
    public void TheComposeScopePredicate_AndsTheGrantWithAnyRequestedNames()
    {
        Assert.Equal("f.server_id = ANY($3)", ComposeCompiler.ServerScope("f.", null, null, "$3"));
        var both = ComposeCompiler.ServerScope("f.", "$2", null, "$3");
        Assert.StartsWith("(", both, StringComparison.Ordinal);
        Assert.EndsWith(" AND f.server_id = ANY($3))", both, StringComparison.Ordinal);
        Assert.DoesNotContain("ANY($3)", ComposeCompiler.ServerScope("f.", "$2", null), StringComparison.Ordinal);
    }
}
