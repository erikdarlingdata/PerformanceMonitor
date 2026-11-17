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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2119: pins against the generated-rung replay hazard. Several migration rungs are assembled at
/// RUNTIME from the live schema generators (14, 38, 51, 54 today), so their SQL is whatever the
/// generator emits on the CURRENT build — not what it emitted when the rung shipped. When a later
/// rung teaches a generator a new column, every earlier generator-built rung silently re-emits SQL
/// referencing it, and a store old enough to replay that rung fails the whole ladder against a
/// table that does not have the column yet. The field failure: rung 51 re-emitted the resolving
/// view with V54's <c>query_plan_gz</c>, and every 3.3.0→3.4.0 upgrade died with 42703 at service
/// start. The dogfood box never sees this class — it walks each rung in the era it shipped — so
/// only a pin can.
/// </summary>
public sealed class MigrationLadderPins
{
    [Fact]
    public void EveryRungReferencingTheGzColumn_PreAddsItBeforeTheViewUsesIt()
    {
        var column = PayloadDimensions.CompressedContentColumn;
        var offenders = PgMigrations.Scripts
            .Where(m => m.Sql.Contains(column, StringComparison.Ordinal))
            .Where(m =>
            {
                /* Both guard forms establish the column ahead of use: the ALTER pre-add
                   ("ADD COLUMN IF NOT EXISTS query_plan_gz bytea") and V38's generated CREATE
                   ("query_plan_gz bytea NULL,"). The resolving view's reference is qualified
                   ("qpd.query_plan_gz"), so the guard index is found by the bare "<column> bytea"
                   shape and must come FIRST. */
                var guard = m.Sql.IndexOf(column + " bytea", StringComparison.Ordinal);
                var use = m.Sql.IndexOf("." + column, StringComparison.Ordinal);
                return use >= 0 && (guard < 0 || guard > use);
            })
            .Select(m => $"V{m.Version} ({m.Name})")
            .ToList();

        Assert.True(offenders.Count == 0,
            $"Migration rung(s) reference {column} before anything establishes it — a store replaying " +
            "that rung on current code fails the whole ladder with 42703 (#2119's field failure, which " +
            "broke every 3.3.0→3.4.0 upgrade). Pre-add the column in the rung (prepend V54Sql) or move " +
            $"the generated SQL to a later rung:\n{string.Join("\n", offenders)}");
    }

    /// <summary>
    /// The GENERAL form of the fact above, which the gz-only version did not cover.
    ///
    /// <para>The gz pin guards ONE column. The hazard is not about that column: any rung that re-emits the
    /// payload-resolving view carries whatever <c>query_stats</c> payload columns the collector has TODAY,
    /// and a store old enough to replay that rung runs the view against a table that predates the later ones
    /// — 42703, at service start, on every upgrade from below it. V38 pre-added every payload column for
    /// exactly this reason and said so ("permanently, for every future payload column too"); V51 and V54
    /// re-emitted the same view pre-adding only their own, so the guarantee was V38's alone.</para>
    ///
    /// <para>Measured, not theorised: #3392 appended <c>query_plan_xml_bytes</c>, and the 3.3.0 fixture
    /// store failed climbing the ladder on <c>f.query_plan_xml_bytes</c> at V51. The live upgrade test
    /// caught it, which is the right backstop and the wrong place to learn it — that test needs a
    /// container and a fixture, so it is the slowest signal in the build. This is the fast one.</para>
    /// </summary>
    [Fact]
    public void EveryRungReEmittingTheResolvingView_PreAddsEveryPayloadColumnItNames()
    {
        var definers = PgMigrations.Scripts
            .Where(m => m.Sql.Contains("VIEW v_query_stats AS", StringComparison.Ordinal))
            .ToList();

        /* A scan that found no definers would pass while asserting nothing, and the whole file is about
           generated rungs quietly changing shape. */
        Assert.NotEmpty(definers);

        var offenders = new List<string>();

        foreach (var rung in definers)
        {
            foreach (var column in QueryStatsCollector.Instance.PayloadColumns)
            {
                var use = rung.Sql.IndexOf("f." + column.Name, StringComparison.Ordinal);
                if (use < 0)
                {
                    continue;
                }

                /* PRESENCE before ORDER: IndexOf answers -1 for an absent guard, and -1 is less than every
                   real offset, so an ordering comparison alone passes LOUDEST when the pre-add is missing
                   entirely — which is the failure. */
                var guard = rung.Sql.IndexOf(
                    "ADD COLUMN IF NOT EXISTS " + column.Name + " ", StringComparison.Ordinal);

                if (guard < 0)
                {
                    offenders.Add($"V{rung.Version} ({rung.Name}) names f.{column.Name} and never adds it");
                }
                else if (guard > use)
                {
                    offenders.Add($"V{rung.Version} ({rung.Name}) adds {column.Name} AFTER the view uses it");
                }
            }
        }

        Assert.True(offenders.Count == 0,
            "Migration rung(s) re-emit the payload-resolving view naming a query_stats payload column the "
            + "same rung does not establish first. A store replaying that rung fails the whole ladder with "
            + "42703 at service start, permanently, on every upgrade from below it. Concatenate "
            + "PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds() ahead of the view in the rung:\n"
            + string.Join("\n", offenders));
    }

    [Fact]
    public void TheLadder_IsStrictlyOrdered_WithNoDuplicates()
    {
        /* The replay math above only holds when versions are strictly increasing and unique — a
           duplicated or out-of-order version would let two rungs disagree about what "already ran"
           means. The historical V45 hole stays sanctioned (a gap NOBODY fills is harmless — the
           stamp comparison is >, not sequence arithmetic); new gaps are the next pin's job. Cheap
           to pin, catastrophic to debug from a half-migrated field store. */
        var versions = PgMigrations.Scripts.Select(m => m.Version).ToList();
        Assert.Equal(versions.OrderBy(v => v).ToList(), versions);
        Assert.Equal(versions.Count, versions.Distinct().Count());
    }

    [Fact]
    public void TheLadder_IsDenseAboveTheHistoricalGap()
    {
        /* #2226: a NEW gap is a rung some other branch intends to fill later — and the applier
           ascends with `version <= currentVersion ? skip`, so a rung filled AFTER a store stamped
           past it is skipped silently and forever: its objects never exist, readers of them fail
           permanently, and no upgrade can repair the store. That exact window nearly opened between
           two in-flight branches (one at V61, one at V62 while dev's MAX was 60; nightlies ship
           from dev, so the window is real). Density makes the hazard fail at AUTHORING time, in the
           author's own test run: every branch must take max(dev)+1, and two branches that both do
           so collide loudly at rebase instead of coexisting into a field incident. V45 is the one
           sanctioned hole, vacant for many releases and filled by nobody. */
        var above = PgMigrations.Scripts.Select(m => m.Version).Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count).ToList(), above);
    }
}
