/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common.QueryTags;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6020: <see cref="QueryTagResolver.Precedence"/> is what the query-tag tools show as <c>precedence</c> and
/// <c>precedence_rank</c>. This runs the real resolver over every step so a step moved in the resolver but not in the list fails here.
/// </summary>
public sealed class QueryTagResolverPrecedenceTests
{
    private const int Server = 7;

    [Fact]
    public void EachStep_WinsOverEveryStepAfterIt_InTheResolver()
    {
        var steps = QueryTagResolver.Precedence;
        Assert.Equal(Enumerable.Range(1, steps.Count), steps.Select(s => s.Rank));
        var obj = new QueryTagObject("Sales", "dbo", "usp_Report");
        var sighting = new QueryTagSighting(QueryTagKeyKind.Hash, "0xAB", ObjectRef: obj, Application: "ReportServer");

        for (var i = 0; i < steps.Count; i++)
        {
            var assignments = new List<QueryTagHandAssignment>();
            var rules = new List<QueryTagRule>();
            foreach (var step in steps.Skip(i))
            {
                long tag = step.Rank;
                switch (step.Source)
                {
                    case QueryTagSource.HandHash:
                        assignments.Add(new QueryTagHandAssignment(tag, QueryTagKeyKind.Hash, "0xab", null, step.LimitedToOneServer == true ? Server : null));
                        break;
                    case QueryTagSource.HandObject:
                        assignments.Add(new QueryTagHandAssignment(tag, QueryTagKeyKind.Object, null, new QueryTagObject("SALES", "DBO", "USP_REPORT"), step.LimitedToOneServer == true ? Server : null));
                        break;
                    default:
                        rules.Add(new QueryTagRule(1, QueryTagRuleField.Application, "Report*", 0, true, tag));
                        break;
                }
            }

            /* Listed worst-first, so only the resolver's own order can pick the step. */
            assignments.Reverse();
            var resolution = QueryTagResolver.Resolve(Server, new[] { sighting }, assignments, rules, new Dictionary<string, long>());

            Assert.Equal(steps[i].Rank, resolution.TagId);
            Assert.Equal(steps[i].Source, resolution.Source);
        }
    }

    [Theory]
    [InlineData(QueryTagSource.HandHash, true, 1)]
    [InlineData(QueryTagSource.HandHash, false, 2)]
    [InlineData(QueryTagSource.HandObject, true, 3)]
    [InlineData(QueryTagSource.HandObject, false, 4)]
    public void TheRankOfAnAssignment_IsItsStepInThatOrder(QueryTagSource source, bool oneServer, int rank)
    {
        Assert.Equal(rank, QueryTagResolver.RankOfAssignment(source, oneServer));
    }
}
