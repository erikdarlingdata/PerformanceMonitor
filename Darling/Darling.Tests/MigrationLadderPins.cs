/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
