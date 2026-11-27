/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="QueryTagRebuildMerge"/> (#9705): a rebuild hands it a server's sighting rows in heap order, one at a time, and
/// it must give each key the tag <see cref="QueryTagResolver"/> gives over the key's rows in order, or say the key needs
/// its rows in order. Pure: no store.
/// </summary>
public sealed class QueryTagRebuildMergeTests
{
    private const int Server = 7;
    private const int OtherServer = 8;

    private static readonly Dictionary<string, long> Tags = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Alpha"] = 1, ["Beta"] = 2, ["Gamma"] = 3, ["Delta"] = 4, ["Eps"] = 5, ["Zeta"] = 6,
    };

    private sealed record World(List<QueryTagRule> Rules, List<QueryTagHandAssignment> Assignments, List<List<QueryTagSighting>> Keys);

    private static T Pick<T>(Random random, params T[] from) => from[random.Next(from.Length)];

    /* Rules over several fields with patterns that match several values, text and text_tag rules, per-server and all-server,
       some disabled, some scoped to another server; hand assignments by hash and by object, scoped and not; and keys of every
       kind with 1 to 6 rows drawn from small pools, so rows of one key often match one rule with different values. */
    private static World NewWorld(int seed)
    {
        var random = new Random(seed);
        var patterns = new (QueryTagRuleField Field, string Pattern)[]
        {
            (QueryTagRuleField.Application, "Rpt*"), (QueryTagRuleField.Application, "AppA"), (QueryTagRuleField.Application, "App*"),
            (QueryTagRuleField.Login, "svc*"), (QueryTagRuleField.Host, "H*"), (QueryTagRuleField.Host, "H1"),
            (QueryTagRuleField.Database, "Arch*"), (QueryTagRuleField.Database, "Sales"), (QueryTagRuleField.Object, "*.dbo.p*"),
            (QueryTagRuleField.Text, "*orders*"), (QueryTagRuleField.Text, "*from *"), (QueryTagRuleField.TextTag, "*"),
        };

        var rules = new List<QueryTagRule>();
        int ruleCount = random.Next(3, 9);
        for (int id = 1; id <= ruleCount; id++)
        {
            var (field, pattern) = Pick(random, patterns);
            rules.Add(new QueryTagRule(
                id, field, pattern, Priority: random.Next(1, 4), Enabled: random.Next(8) != 0,
                TagId: field == QueryTagRuleField.TextTag ? null : random.Next(1, 7),
                ServerId: Pick<int?>(random, null, null, Server, OtherServer)));
        }

        var assignments = new List<QueryTagHandAssignment>();
        int assignmentCount = random.Next(0, 5);
        for (int i = 0; i < assignmentCount; i++)
        {
            int? scope = Pick<int?>(random, null, Server, OtherServer);
            assignments.Add(random.Next(2) == 0
                ? new QueryTagHandAssignment(random.Next(1, 7), QueryTagKeyKind.Hash, Key: "K" + random.Next(0, 12), ServerId: scope)
                : new QueryTagHandAssignment(random.Next(1, 7), QueryTagKeyKind.Object, ObjectRef: new QueryTagObject(Pick(random, "Sales", "Arch1"), "dbo", Pick(random, "p1", "p2")), ServerId: scope));
        }

        var keys = new List<List<QueryTagSighting>>();
        for (int k = 0; k < 60; k++)
        {
            var kind = Pick(random, QueryTagKeyKind.Hash, QueryTagKeyKind.Hash, QueryTagKeyKind.Hash, QueryTagKeyKind.Object, QueryTagKeyKind.Caller);
            var rows = new List<QueryTagSighting>();
            int rowCount = random.Next(1, 7);
            for (int r = 0; r < rowCount; r++)
            {
                string? database = Pick(random, "Sales", "Arch1", "Arch2", null);
                rows.Add(new QueryTagSighting(
                    kind, "K" + k, database,
                    Pick(random, null, new QueryTagObject(database, "dbo", "p1"), new QueryTagObject(database, "dbo", "p2")),
                    Pick(random, "AppA", "AppB", "RptA", "RptB", null),
                    Pick(random, "svc1", "svc2", null),
                    Pick(random, "H1", "H2", null),
                    Pick(random, null, "select * from orders", "/* tag:Alpha */ select 1 from t", "-- tag:Beta\nselect x from orders", "/* tag:nope */ select 2")));
            }

            keys.Add(rows);
        }

        return new World(rules, assignments, keys);
    }

    private static QueryTagRebuildMergeResult Merge(World world, IEnumerable<QueryTagSighting> rows)
    {
        var merge = new QueryTagRebuildMerge(Server, world.Assignments, world.Rules, Tags);
        foreach (var row in rows)
        {
            merge.Add(row);
        }

        return merge.Finish();
    }

    /* The orders a key's rows are tried in: every one for up to four rows, and the given order, its reverse and 30 shuffles above that. */
    private static IEnumerable<List<QueryTagSighting>> Orders(List<QueryTagSighting> rows, Random random)
    {
        if (rows.Count <= 4)
        {
            foreach (var order in Permutations(rows, 0))
            {
                yield return order;
            }

            yield break;
        }

        yield return rows;
        yield return Enumerable.Reverse(rows).ToList();
        for (int i = 0; i < 30; i++)
        {
            yield return rows.OrderBy(_ => random.Next()).ToList();
        }
    }

    private static IEnumerable<List<QueryTagSighting>> Permutations(List<QueryTagSighting> rows, int from)
    {
        if (from >= rows.Count - 1)
        {
            yield return rows.ToList();
            yield break;
        }

        for (int i = from; i < rows.Count; i++)
        {
            (rows[from], rows[i]) = (rows[i], rows[from]);
            foreach (var order in Permutations(rows, from + 1))
            {
                yield return order;
            }

            (rows[from], rows[i]) = (rows[i], rows[from]);
        }
    }

    private static string Text(QueryTagRebuildMergeResult result) =>
        string.Join(";", result.Rows.Select(r => r.ToString()).OrderBy(r => r, StringComparer.Ordinal))
        + " / " + string.Join(";", result.KeysNeedingOrder.Select(k => k.KeyKind + ":" + k.Key).OrderBy(k => k, StringComparer.Ordinal));

    [Fact]
    public void AKeyTheMergeSettles_GetsTheTagTheResolverGivesInEveryRowOrder_AndTheMergeIsTheSameInAnyRowOrder()
    {
        int settledWithATag = 0, settledFromSeveralRows = 0, needingOrder = 0;
        for (int seed = 1; seed <= 40; seed++)
        {
            var world = NewWorld(seed);
            var random = new Random(seed * 31);
            var all = world.Keys.SelectMany(rows => rows).ToList();
            var merged = Merge(world, all);

            /* The rows of every key, shuffled together as a heap scan gives them: the same rows and the same keys needing order. */
            string expected = Text(merged);
            Assert.Equal(expected, Text(Merge(world, Enumerable.Reverse(all))));
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(expected, Text(Merge(world, all.OrderBy(_ => random.Next()))));
            }

            var rowByKey = merged.Rows.ToDictionary(r => r.Key);
            var orderKeys = merged.KeysNeedingOrder.Select(k => k.Key).ToHashSet();
            needingOrder += orderKeys.Count;
            foreach (var rows in world.Keys)
            {
                string key = rows[0].Key;
                if (orderKeys.Contains(key))
                {
                    Assert.False(rowByKey.ContainsKey(key), $"seed {seed}: {key} is both settled and needing order");
                    continue;
                }

                rowByKey.TryGetValue(key, out var settled);
                foreach (var order in Orders(rows, random))
                {
                    var resolution = QueryTagResolver.Resolve(Server, order, world.Assignments, world.Rules, Tags);
                    var inOrder = resolution.HasTag ? QueryTagMapStore.MapRow(rows[0].KeyKind, key, resolution) : null;
                    Assert.True(Equals(settled, inOrder), $"seed {seed}: {key} merged to {settled} but resolves to {inOrder} in one row order");
                }

                if (settled is not null)
                {
                    settledWithATag++;
                    settledFromSeveralRows += rows.Count > 1 ? 1 : 0;
                }
            }
        }

        /* The worlds really exercise all three outcomes. */
        Assert.True(settledWithATag > 200, $"only {settledWithATag} keys settled with a tag");
        Assert.True(settledFromSeveralRows > 100, $"only {settledFromSeveralRows} keys settled from several rows");
        Assert.True(needingOrder > 50, $"only {needingOrder} keys needed order");
    }

    [Fact]
    public void AKeyNeedsOrder_OnlyWhenItsRowsAtTheWinningStepGiveDifferentResolutions()
    {
        var rules = new List<QueryTagRule>
        {
            new(1, QueryTagRuleField.Application, "Rpt*", 1, true, 1),
            new(2, QueryTagRuleField.Database, "Arch*", 2, true, 2),
        };
        var assignments = new List<QueryTagHandAssignment> { new(3, QueryTagKeyKind.Hash, Key: "0xHAND") };
        var world = new World(rules, assignments, new List<List<QueryTagSighting>>());
        static QueryTagSighting Row(string key, string application, string database, string host = "H1") =>
            new(QueryTagKeyKind.Hash, key, database, null, application, "svc", host);

        var merged = Merge(world, new[]
        {
            /* Two rows match rule 1 with different values: which is first in order decides the matched value. */
            Row("0xTWO", "RptB", "Sales"), Row("0xTWO", "RptA", "Sales"),
            /* Two rows match rule 1 with one value. */
            Row("0xSAME", "RptA", "Sales", "H1"), Row("0xSAME", "RptA", "Sales", "H2"),
            /* The rows differ only at rule 2, and one row matches rule 1, which wins. */
            Row("0xLOW", "App", "Arch1"), Row("0xLOW", "App", "Arch2"), Row("0xLOW", "RptA", "Sales"),
            /* The rows differ at rule 1, and the hand assignment wins the key. */
            Row("0xHAND", "RptA", "Sales"), Row("0xHAND", "RptB", "Sales"),
            /* No row matches anything. */
            Row("0xNONE", "App", "Sales"),
        });

        Assert.Equal(new[] { "hash:0xTWO" }, merged.KeysNeedingOrder.Select(k => k.KeyKind + ":" + k.Key));
        Assert.Equal(
            new[] { "0xHAND|3|hand_hash||0xHAND", "0xLOW|1|rule|1|RptA", "0xSAME|1|rule|1|RptA" },
            merged.Rows.Select(r => $"{r.Key}|{r.TagId}|{r.Source}|{r.RuleId}|{r.MatchedValue}").OrderBy(r => r, StringComparer.Ordinal));
    }

    [Fact]
    public void ResolveWithARank_GivesTheResolutionOfResolve_AndTheRankOfAListIsTheLowestRankOfItsRows()
    {
        int tagged = 0;
        for (int seed = 1; seed <= 40; seed++)
        {
            var world = NewWorld(seed);
            foreach (var rows in world.Keys)
            {
                var plain = QueryTagResolver.Resolve(Server, rows, world.Assignments, world.Rules, Tags);
                var ranked = QueryTagResolver.Resolve(Server, rows, world.Assignments, world.Rules, Tags, out int rank);
                Assert.Equal(plain, ranked);
                Assert.Equal(plain.HasTag, rank != int.MaxValue);

                int lowest = int.MaxValue;
                foreach (var row in rows)
                {
                    QueryTagResolver.Resolve(Server, new[] { row }, world.Assignments, world.Rules, Tags, out int own);
                    lowest = Math.Min(lowest, own);
                }

                Assert.Equal(lowest, rank);
                tagged += plain.HasTag ? 1 : 0;
            }
        }

        Assert.True(tagged > 500, $"only {tagged} keys got a tag");
        Assert.Equal(QueryTagResolution.None, QueryTagResolver.Resolve(Server, Array.Empty<QueryTagSighting>(), Array.Empty<QueryTagHandAssignment>(), Array.Empty<QueryTagRule>(), Tags, out int none));
        Assert.Equal(int.MaxValue, none);
    }
}
