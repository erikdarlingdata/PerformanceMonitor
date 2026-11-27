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
/// <see cref="QueryTagRebuildMerge"/> (#9705): a rebuild hands it a server's slim rows and then its sightings in heap order, one
/// at a time. A key that has a sighting must get the tag <see cref="QueryTagResolver"/> gives over the key's sightings in
/// order, or be named as needing them in order, whatever its slim rows say. A key with no sighting must get the tag the
/// resolver gives over its slim rows in <see cref="QueryTagRebuildMerge.SlimRowOrder"/>. Pure: no store.
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

    private static readonly Comparer<QueryTagSighting> InSlimRowOrder = Comparer<QueryTagSighting>.Create(QueryTagRebuildMerge.SlimRowOrder);

    /* A slim row has no text sample. */
    private static QueryTagSighting Slim(QueryTagSighting row) => row with { TextSample = null };

    private static QueryTagRebuildMergeResult Merge(World world, IEnumerable<QueryTagSighting> slimRows, IEnumerable<QueryTagSighting> sightings)
    {
        var merge = new QueryTagRebuildMerge(Server, world.Assignments, world.Rules, Tags);
        foreach (var row in slimRows)
        {
            merge.AddSlim(row);
        }

        foreach (var row in sightings)
        {
            merge.Add(row);
        }

        return merge.Finish();
    }

    private static string RowText(QueryTagMapRow row) => $"{row.Key}|{row.TagId}|{row.Source}|{row.RuleId}|{row.MatchedValue}";

    /* An application rule that wins over a database rule, so a row can be given either tag, the earlier one or none. */
    private static World TwoRuleWorld() => new(
        new List<QueryTagRule>
        {
            new(1, QueryTagRuleField.Application, "Rpt*", 1, true, 1),
            new(2, QueryTagRuleField.Database, "Arch*", 2, true, 2),
        },
        new List<QueryTagHandAssignment>(),
        new List<List<QueryTagSighting>>());

    private static QueryTagSighting HashRow(string key, string? application, string? database, string? host = "H1", string? login = "svc") =>
        new(QueryTagKeyKind.Hash, key, database, null, application, login, host);

    [Fact]
    public void AKeyWithSlimRowsAndATaggedSighting_TakesTheSightingsTag_EvenWhenAnEarlierRuleTagsTheSlimRow()
    {
        var world = TwoRuleWorld();

        /* 0xBOTH: the slim row matches rule 1, which wins over rule 2 when both rows count. The sighting matches only rule 2.
           0xSAME: the slim row and the sighting match rule 1 with different values. */
        var merged = Merge(
            world,
            new[] { HashRow("0xBOTH", "RptA", "Sales"), HashRow("0xSAME", "RptA", "Sales") },
            new[] { HashRow("0xBOTH", "App", "Arch1"), HashRow("0xSAME", "RptZ", "Sales") });

        Assert.Empty(merged.KeysNeedingOrder);
        Assert.Equal(new[] { "0xBOTH|2|rule|2|Arch1", "0xSAME|1|rule|1|RptZ" }, merged.Rows.Select(RowText).OrderBy(r => r, StringComparer.Ordinal));
    }

    [Fact]
    public void AKeyWithTaggedSlimRowsAndASightingNoRuleTags_GetsNoMapRow_AndAKeyWithOnlySlimRowsIsTaggedFromThem()
    {
        var world = TwoRuleWorld();
        var slim = new[] { HashRow("0xSEEN", "RptA", "Arch1"), HashRow("0xGONE", "RptA", "Sales"), HashRow("0xARCH", "App", "Arch1"), HashRow("0xNONE", "App", "Sales") };

        var merged = Merge(world, slim, new[] { HashRow("0xSEEN", "App", "Sales") });

        Assert.Empty(merged.KeysNeedingOrder);
        Assert.Equal(new[] { "0xARCH|2|rule|2|Arch1", "0xGONE|1|rule|1|RptA" }, merged.Rows.Select(RowText).OrderBy(r => r, StringComparer.Ordinal));

        /* The same slim rows with no sighting at all: 0xSEEN is tagged from its slim row. */
        var slimOnly = Merge(world, slim, Array.Empty<QueryTagSighting>());
        Assert.Equal(new[] { "0xARCH|2|rule|2|Arch1", "0xGONE|1|rule|1|RptA", "0xSEEN|1|rule|1|RptA" }, slimOnly.Rows.Select(RowText).OrderBy(r => r, StringComparer.Ordinal));
    }

    [Fact]
    public void SlimRowsOfOneKeyAtTheWinningStep_GiveTheFirstInSlimRowOrder_InEveryArrivalOrder()
    {
        var world = TwoRuleWorld();

        /* Four rows match rule 1 with three values, in two databases, and one row matches only rule 2. First in order: the
           database Arch1 before Sales, then the application RptB before RptC. RptA comes first by application alone. */
        var rows = new List<QueryTagSighting>
        {
            HashRow("0xK", "RptA", "Sales"), HashRow("0xK", "RptC", "Arch1"), HashRow("0xK", "RptB", "Arch1", host: "H2"), HashRow("0xK", "RptB", "Arch1", host: "H1"),
            HashRow("0xK", "App", "Arch0"),
        };

        var inOrder = QueryTagResolver.Resolve(Server, rows.OrderBy(row => row, InSlimRowOrder).ToList(), world.Assignments, world.Rules, Tags);
        Assert.Equal("RptB", inOrder.MatchedValue);

        int orders = 0;
        foreach (var order in Permutations(rows, 0))
        {
            var merged = Merge(world, order, Array.Empty<QueryTagSighting>());
            Assert.Empty(merged.KeysNeedingOrder);
            Assert.Equal(new[] { QueryTagMapStore.MapRow(QueryTagKeyKind.Hash, "0xK", inOrder) }, merged.Rows);
            orders++;
        }

        Assert.Equal(120, orders);
    }

    [Fact]
    public void AMergeOfBothTables_ResolvesASightedKeyFromItsSightingsAlone_AndAKeyWithNoneFromItsSlimRowsInSlimRowOrder()
    {
        int slimOnlyTagged = 0, slimOnlyOrderDecides = 0, sightedWithSlimRows = 0, sightedWhereSlimRowsWouldChangeTheTag = 0;
        for (int seed = 1; seed <= 40; seed++)
        {
            var world = NewWorld(seed);
            var random = new Random(seed * 17);

            /* Each key's rows are dealt to the two tables: about a third of the keys keep only slim rows, a third only
               sightings, and a third have both. */
            var slimByKey = new Dictionary<string, List<QueryTagSighting>>();
            var sightingsByKey = new Dictionary<string, List<QueryTagSighting>>();
            foreach (var rows in world.Keys)
            {
                int deal = random.Next(3);
                foreach (var row in rows)
                {
                    bool toSlim = deal == 0 || (deal == 2 && random.Next(2) == 0);
                    var table = toSlim ? slimByKey : sightingsByKey;
                    if (!table.TryGetValue(row.Key, out var list))
                    {
                        table[row.Key] = list = new List<QueryTagSighting>();
                    }

                    list.Add(toSlim ? Slim(row) : row);
                }
            }

            var allSlim = slimByKey.Values.SelectMany(rows => rows).ToList();
            var allSightings = sightingsByKey.Values.SelectMany(rows => rows).ToList();
            var merged = Merge(world, allSlim, allSightings);

            /* Any arrival order inside each table gives the same rows and the same keys needing order. */
            string expected = Text(merged);
            Assert.Equal(expected, Text(Merge(world, Enumerable.Reverse(allSlim), Enumerable.Reverse(allSightings))));
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(expected, Text(Merge(world, allSlim.OrderBy(_ => random.Next()), allSightings.OrderBy(_ => random.Next()))));
            }

            /* A sighted key comes out exactly as it does with no slim table. */
            var sightingsAlone = Merge(world, allSightings);
            var aloneByKey = sightingsAlone.Rows.ToDictionary(r => r.Key);
            var rowByKey = merged.Rows.ToDictionary(r => r.Key);
            Assert.Equal(
                sightingsAlone.KeysNeedingOrder.Select(k => k.KeyKind + ":" + k.Key).OrderBy(k => k, StringComparer.Ordinal),
                merged.KeysNeedingOrder.Select(k => k.KeyKind + ":" + k.Key).OrderBy(k => k, StringComparer.Ordinal));
            foreach (var (key, sightings) in sightingsByKey)
            {
                aloneByKey.TryGetValue(key, out var alone);
                rowByKey.TryGetValue(key, out var both);
                Assert.True(Equals(alone, both), $"seed {seed}: {key} merged to {both} with its slim rows and to {alone} from its sightings alone");
                if (slimByKey.TryGetValue(key, out var slimRows))
                {
                    sightedWithSlimRows++;
                    var together = QueryTagResolver.Resolve(Server, slimRows.Concat(sightings).ToList(), world.Assignments, world.Rules, Tags);
                    var apart = QueryTagResolver.Resolve(Server, sightings, world.Assignments, world.Rules, Tags);
                    sightedWhereSlimRowsWouldChangeTheTag += together.TagId != apart.TagId ? 1 : 0;
                }
            }

            /* A key with no sighting gets what the resolver gives over its slim rows in SlimRowOrder. */
            foreach (var (key, slimRows) in slimByKey)
            {
                if (sightingsByKey.ContainsKey(key))
                {
                    continue;
                }

                var inOrder = slimRows.OrderBy(row => row, InSlimRowOrder).ToList();
                var resolution = QueryTagResolver.Resolve(Server, inOrder, world.Assignments, world.Rules, Tags);
                var expectedRow = resolution.HasTag ? QueryTagMapStore.MapRow(slimRows[0].KeyKind, key, resolution) : null;
                rowByKey.TryGetValue(key, out var settled);
                Assert.True(Equals(settled, expectedRow), $"seed {seed}: {key} merged to {settled} but its slim rows in order resolve to {expectedRow}");
                slimOnlyTagged += settled is not null ? 1 : 0;

                var reversed = QueryTagResolver.Resolve(Server, Enumerable.Reverse(inOrder).ToList(), world.Assignments, world.Rules, Tags);
                slimOnlyOrderDecides += reversed.Equals(resolution) ? 0 : 1;
            }
        }

        /* The worlds really exercise each case. */
        Assert.True(slimOnlyTagged > 150, $"only {slimOnlyTagged} keys were tagged from slim rows alone");
        Assert.True(slimOnlyOrderDecides > 20, $"the order of the slim rows decided only {slimOnlyOrderDecides} keys");
        Assert.True(sightedWithSlimRows > 300, $"only {sightedWithSlimRows} sighted keys had slim rows");
        Assert.True(sightedWhereSlimRowsWouldChangeTheTag > 20, $"the slim rows would have changed the tag of only {sightedWhereSlimRowsWouldChangeTheTag} sighted keys");
    }

    [Fact]
    public void ASlimRowAddedTwice_ChangesNothing()
    {
        for (int seed = 1; seed <= 10; seed++)
        {
            var world = NewWorld(seed);
            var slim = world.Keys.SelectMany(rows => rows).Select(Slim).ToList();
            var once = Merge(world, slim, Array.Empty<QueryTagSighting>());
            Assert.NotEmpty(once.Rows);
            Assert.Equal(Text(once), Text(Merge(world, slim.Concat(slim), Array.Empty<QueryTagSighting>())));
            Assert.Equal(Text(once), Text(Merge(world, slim.Concat(Enumerable.Reverse(slim)), Array.Empty<QueryTagSighting>())));
        }
    }

    [Fact]
    public void ASlimRowAfterASighting_Throws()
    {
        var world = TwoRuleWorld();
        var merge = new QueryTagRebuildMerge(Server, world.Assignments, world.Rules, Tags);
        merge.AddSlim(HashRow("0xA", "RptA", "Sales"));
        merge.Add(HashRow("0xB", "App", "Sales"));

        Assert.Throws<InvalidOperationException>(() => merge.AddSlim(HashRow("0xC", "RptA", "Sales")));
    }

    /* U+FFFD is one UTF-16 unit, 0xFFFD. U+1F600 is two, 0xD83D and 0xDE00: the higher code point, whose first unit is the lower. */
    private const string LastOfThePlane = "�";
    private const string AboveThePlane = "\U0001F600";

    [Fact]
    public void CompareCodePoints_PutsACharacterAboveUFFFFAfterEveryCharacterBelowIt_AndTakesNoneAsEmpty()
    {
        Assert.True(QueryTagRebuildMerge.CompareCodePoints(LastOfThePlane, AboveThePlane) < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints(AboveThePlane, LastOfThePlane) > 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("Rpt" + LastOfThePlane + "z", "Rpt" + AboveThePlane + "a") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("", "\U00010000") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("\U00010000", AboveThePlane) < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints(AboveThePlane, "\U0001F601") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("z", AboveThePlane) < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("B", "a") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("App", "App1") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("App1", "App") > 0);
        Assert.Equal(0, QueryTagRebuildMerge.CompareCodePoints("Rpt" + AboveThePlane, "Rpt" + AboveThePlane));

        Assert.Equal(0, QueryTagRebuildMerge.CompareCodePoints(null, null));
        Assert.Equal(0, QueryTagRebuildMerge.CompareCodePoints(null, string.Empty));
        Assert.Equal(0, QueryTagRebuildMerge.CompareCodePoints(string.Empty, null));
        Assert.True(QueryTagRebuildMerge.CompareCodePoints(null, "a") < 0);
        Assert.True(QueryTagRebuildMerge.CompareCodePoints("a", null) > 0);
    }

    [Fact]
    public void CompareCodePoints_GivesTheByteOrderOfTheUtf8Form_ForTextsWithCharactersOnBothSidesOfUFFFF()
    {
        string[] characters = { "A", "a", "z", "é", "Ж", "中", "퟿", "", "�", "￿", "\U00010000", "\U0001F600", "\U0010FFFF" };
        var random = new Random(9705);
        string NewText() => string.Concat(Enumerable.Range(0, random.Next(0, 5)).Select(_ => Pick(random, characters)));

        int otherThanByCodeUnit = 0;
        for (int i = 0; i < 5000; i++)
        {
            string a = NewText();
            string b = NewText();
            int byUtf8 = Math.Sign(System.Text.Encoding.UTF8.GetBytes(a).AsSpan().SequenceCompareTo(System.Text.Encoding.UTF8.GetBytes(b)));

            Assert.True(byUtf8 == Math.Sign(QueryTagRebuildMerge.CompareCodePoints(a, b)), $"pair {i}: the UTF-8 bytes order the texts {byUtf8}");
            Assert.True(-byUtf8 == Math.Sign(QueryTagRebuildMerge.CompareCodePoints(b, a)), $"pair {i}: the UTF-8 bytes order the texts {-byUtf8} the other way round");
            otherThanByCodeUnit += Math.Sign(string.CompareOrdinal(a, b)) != byUtf8 ? 1 : 0;
        }

        /* The texts really hold pairs that an order by UTF-16 code unit gets wrong. */
        Assert.True(otherThanByCodeUnit > 100, $"only {otherThanByCodeUnit} pairs order differently by code unit");
    }

    [Fact]
    public void SlimRowOrder_OrdersEachFieldByCodePoint_WhereTheOrderByCodeUnitIsTheOtherWayRound_AndNoneEqualsEmpty()
    {
        /* By UTF-16 code unit the two texts order the other way round: 0xD83D, the pair's first unit, is below 0xFFFD. */
        Assert.True(string.CompareOrdinal(LastOfThePlane, AboveThePlane) > 0);

        static QueryTagSighting Row(string? database = "Sales", string? application = "App", string? login = "svc", string? host = "H1", string? objectName = null) =>
            new(QueryTagKeyKind.Hash, "0xK", database, objectName is null ? null : new QueryTagObject("Sales", "dbo", objectName), application, login, host);

        var fields = new (string Field, QueryTagSighting First, QueryTagSighting Second)[]
        {
            ("database", Row(database: LastOfThePlane), Row(database: AboveThePlane)),
            ("application", Row(application: LastOfThePlane), Row(application: AboveThePlane)),
            ("login", Row(login: LastOfThePlane), Row(login: AboveThePlane)),
            ("host", Row(host: LastOfThePlane), Row(host: AboveThePlane)),
            ("object", Row(objectName: LastOfThePlane), Row(objectName: AboveThePlane)),
        };
        foreach (var (field, first, second) in fields)
        {
            Assert.True(QueryTagRebuildMerge.SlimRowOrder(first, second) < 0, $"U+FFFD does not come first in the {field}");
            Assert.True(QueryTagRebuildMerge.SlimRowOrder(second, first) > 0, $"U+1F600 does not come last in the {field}");
            Assert.Equal(0, QueryTagRebuildMerge.SlimRowOrder(first, first with { TextSample = "SELECT 1;" }));
        }

        /* An earlier field decides before a later one that orders the other way. */
        Assert.True(QueryTagRebuildMerge.SlimRowOrder(Row(database: LastOfThePlane, application: "RptB"), Row(database: AboveThePlane, application: "RptA")) < 0);
        Assert.True(QueryTagRebuildMerge.SlimRowOrder(Row(application: LastOfThePlane, host: "H2"), Row(application: AboveThePlane, host: "H1")) < 0);

        /* None is the empty text, in every field that can be none or empty. */
        Assert.Equal(0, QueryTagRebuildMerge.SlimRowOrder(Row(database: null), Row(database: string.Empty)));
        Assert.Equal(0, QueryTagRebuildMerge.SlimRowOrder(Row(application: null), Row(application: string.Empty)));
        Assert.Equal(0, QueryTagRebuildMerge.SlimRowOrder(Row(login: string.Empty), Row(login: null)));
        Assert.Equal(0, QueryTagRebuildMerge.SlimRowOrder(Row(host: string.Empty), Row(host: null)));
        Assert.True(QueryTagRebuildMerge.SlimRowOrder(Row(application: null), Row(application: "A")) < 0);
        Assert.True(QueryTagRebuildMerge.SlimRowOrder(Row(objectName: "p1"), Row()) > 0);
    }

    [Fact]
    public void TwoSlimRowsThatUFFFDAndACharacterAboveItTellApart_GiveTheUFFFDRowsMatchedValue_InBothArrivalOrders()
    {
        var world = TwoRuleWorld();

        /* 0xAPP: the application, which is the matched value, decides. 0xDB: the database decides which application is the
           matched value, and by application alone RptA would come first. An order by code unit gives the other row of each. */
        var first = new[] { HashRow("0xAPP", "Rpt" + LastOfThePlane, "Sales"), HashRow("0xDB", "RptB", LastOfThePlane) };
        var second = new[] { HashRow("0xAPP", "Rpt" + AboveThePlane, "Sales"), HashRow("0xDB", "RptA", AboveThePlane) };
        string[] expected = { "0xAPP|1|rule|1|Rpt" + LastOfThePlane, "0xDB|1|rule|1|RptB" };

        foreach (var arrival in new[] { first.Concat(second).ToList(), second.Concat(first).ToList() })
        {
            var merged = Merge(world, arrival, Array.Empty<QueryTagSighting>());
            Assert.Empty(merged.KeysNeedingOrder);
            Assert.Equal(expected, merged.Rows.Select(RowText).OrderBy(r => r, StringComparer.Ordinal));
        }
    }

    [Fact]
    public void TwoSlimRowsOfAKeyThatTwoNeighbouringOrderFieldsPutInOppositeOrders_GiveTheRowTheEarlierFieldPutsFirst_ForEachPairOfFields_InBothArrivalOrders()
    {
        /* One rule per field, each matching only values that start with x, so each key below is tagged by the rule on the
           later field of its pair, and its two rows give that rule different matched values. */
        var world = new World(
            new List<QueryTagRule>
            {
                new(1, QueryTagRuleField.Application, "x*", 1, true, 1),
                new(2, QueryTagRuleField.Login, "x*", 2, true, 2),
                new(3, QueryTagRuleField.Host, "x*", 3, true, 3),
                new(4, QueryTagRuleField.Object, "*.dbo.x*", 4, true, 4),
            },
            new List<QueryTagHandAssignment>(),
            new List<List<QueryTagSighting>>());
        static QueryTagSighting Row(string key, string database, string application, string login, string host, string objectName) =>
            new(QueryTagKeyKind.Hash, key, database, new QueryTagObject("D", "dbo", objectName), application, login, host);

        /* Each key's first row comes first by the earlier field of its pair (a before b) and second by the later one (xz after
           xy). The same few values stand in different fields of different keys. */
        var first = new[]
        {
            Row("0xDATABASE", "a", "xz", "m", "m", "m"), Row("0xAPPLICATION", "m", "a", "xz", "m", "m"),
            Row("0xLOGIN", "m", "m", "a", "xz", "m"), Row("0xHOST", "m", "m", "m", "a", "xz"),
        };
        var second = new[]
        {
            Row("0xDATABASE", "b", "xy", "m", "m", "m"), Row("0xAPPLICATION", "m", "b", "xy", "m", "m"),
            Row("0xLOGIN", "m", "m", "b", "xy", "m"), Row("0xHOST", "m", "m", "m", "b", "xy"),
        };
        string[] expected = { "0xAPPLICATION|2|rule|2|xz", "0xDATABASE|1|rule|1|xz", "0xHOST|4|rule|4|D.dbo.xz", "0xLOGIN|3|rule|3|xz" };

        foreach (var arrival in new[] { first.Concat(second).ToList(), second.Concat(first).ToList() })
        {
            var merged = Merge(world, arrival, Array.Empty<QueryTagSighting>());
            Assert.Empty(merged.KeysNeedingOrder);
            Assert.Equal(expected, merged.Rows.Select(RowText).OrderBy(r => r, StringComparer.Ordinal));
        }
    }

    private static List<string> RowTexts(IEnumerable<QueryTagMapRow> rows) =>
        rows.Select(r => r.ToString()).OrderBy(r => r, StringComparer.Ordinal).ToList();

    [Fact]
    public void MapRowsAfterSetOrdered_AreTheRowsTheResolverGivesOverEachKeysSightingsInSlimRowOrder_AndAKeySetToNoTagHasNoRow()
    {
        int ordered = 0, orderDecides = 0;
        for (int seed = 1; seed <= 40; seed++)
        {
            var world = NewWorld(seed);
            var random = new Random(seed * 13);
            var arrival = world.Keys.SelectMany(rows => rows).OrderBy(_ => random.Next()).ToList();
            var inOrderByKey = world.Keys.ToDictionary(rows => rows[0].Key, rows => rows.OrderBy(row => row, InSlimRowOrder).ToList());
            QueryTagResolution InOrder(string key) => QueryTagResolver.Resolve(Server, inOrderByKey[key], world.Assignments, world.Rules, Tags);

            QueryTagRebuildMerge NewMerge()
            {
                var merge = new QueryTagRebuildMerge(Server, world.Assignments, world.Rules, Tags);
                foreach (var row in arrival)
                {
                    merge.Add(row);
                }

                return merge;
            }

            /* As the builder does: each key that needs order is settled with the resolution of its sightings in order. */
            var merge = NewMerge();
            var settledByTheMerge = RowTexts(merge.MapRows());
            var needOrder = merge.KeysNeedingOrder();
            foreach (var key in needOrder)
            {
                var rows = inOrderByKey[key.Key];
                merge.SetOrdered(rows[0].KeyKind, key.Key, InOrder(key.Key));
                ordered++;
                orderDecides += InOrder(key.Key).Equals(QueryTagResolver.Resolve(Server, Enumerable.Reverse(rows).ToList(), world.Assignments, world.Rules, Tags)) ? 0 : 1;
            }

            var expected = new List<QueryTagMapRow>();
            foreach (var (key, rows) in inOrderByKey)
            {
                var resolution = InOrder(key);
                if (resolution.HasTag)
                {
                    expected.Add(QueryTagMapStore.MapRow(rows[0].KeyKind, key, resolution));
                }
            }

            Assert.Equal(RowTexts(expected), RowTexts(merge.MapRows()));
            Assert.Empty(merge.KeysNeedingOrder());
            Assert.Equal(settledByTheMerge.Count + needOrder.Count, expected.Count);

            /* The same keys set to no tag: the map rows are the ones the merge settled alone, and no key is left needing order. */
            var dropped = NewMerge();
            foreach (var key in dropped.KeysNeedingOrder())
            {
                dropped.SetOrdered(inOrderByKey[key.Key][0].KeyKind, key.Key, QueryTagResolution.None);
            }

            Assert.Equal(settledByTheMerge, RowTexts(dropped.MapRows()));
            Assert.Empty(dropped.KeysNeedingOrder());
            Assert.Equal(settledByTheMerge, RowTexts(dropped.Finish().Rows));
        }

        /* The worlds really have keys that need order, and keys whose tag the order decides. */
        Assert.True(ordered > 50, $"only {ordered} keys needed order");
        Assert.True(orderDecides > 20, $"the order decided only {orderDecides} keys");
    }

    [Fact]
    public void AKeyThatNeedsOrder_HasNoMapRowUntilSetOrdered_ThenTheRowOfTheResolutionItWasGiven_OrNoneWhenGivenNoTag()
    {
        var world = TwoRuleWorld();
        QueryTagRebuildMerge NewMerge()
        {
            var merge = new QueryTagRebuildMerge(Server, world.Assignments, world.Rules, Tags);
            merge.Add(HashRow("0xTWO", "RptB", "Sales"));
            merge.Add(HashRow("0xTWO", "RptA", "Sales"));
            merge.Add(HashRow("0xONE", "RptC", "Sales"));
            return merge;
        }

        var merge = NewMerge();
        Assert.Equal(new[] { "hash:0xTWO" }, merge.KeysNeedingOrder().Select(k => k.KeyKind + ":" + k.Key));
        Assert.Equal(new[] { "0xONE|1|rule|1|RptC" }, merge.MapRows().Select(RowText));

        var inOrder = new[] { HashRow("0xTWO", "RptB", "Sales"), HashRow("0xTWO", "RptA", "Sales") }.OrderBy(row => row, InSlimRowOrder).ToList();
        merge.SetOrdered(QueryTagKeyKind.Hash, "0xTWO", QueryTagResolver.Resolve(Server, inOrder, world.Assignments, world.Rules, Tags));
        Assert.Empty(merge.KeysNeedingOrder());
        Assert.Equal(new[] { "0xONE|1|rule|1|RptC", "0xTWO|1|rule|1|RptA" }, merge.MapRows().Select(RowText).OrderBy(r => r, StringComparer.Ordinal));

        var dropped = NewMerge();
        dropped.SetOrdered(QueryTagKeyKind.Hash, "0xTWO", QueryTagResolution.None);
        Assert.Empty(dropped.KeysNeedingOrder());
        Assert.Equal(new[] { "0xONE|1|rule|1|RptC" }, dropped.MapRows().Select(RowText));
        Assert.Equal(new[] { "0xONE|1|rule|1|RptC" }, dropped.Finish().Rows.Select(RowText));
    }
}
