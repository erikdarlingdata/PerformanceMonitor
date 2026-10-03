/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The rule that says whether a stored install id row belongs to the store it is read from (#4961), as a truth table.
/// A managed major upgrade makes a new cluster id, keeps the database's OID and the install id table's OID, and raises the
/// server's major version. So a row that has its table OID keeps its id through a changed cluster id only when both OIDs
/// match and the major rose: a copy of the row into a fresh install of the same version also matches both OIDs (a new
/// cluster numbers its objects from the same start) and has a new cluster id, but the major did not rise, so it gets a new
/// id. A row made before the table OID was kept has none, and is compared as it was before: by the database's OID, and by
/// the cluster id when the row and the current read both have one.
/// </summary>
public sealed class StoreInstallIdBindingRuleTests
{
    // stored cluster id, database OID, table OID, major; current cluster id, database OID, table OID, major; same store?
    [Theory]
    /* The same id: nothing changed. */
    [InlineData(1L, 5L, 7L, 16, 1L, 5L, 7L, 16, true)]
    /* A major upgrade: new cluster id, both OIDs kept, the major rose. */
    [InlineData(1L, 5L, 7L, 16, 2L, 5L, 7L, 17, true)]
    [InlineData(1L, 5L, 7L, 15, 2L, 5L, 7L, 17, true)]     // two majors in one step is still a rise
    /* A copy of the row into a fresh install: new cluster id, both OIDs match, but the major did not rise. */
    [InlineData(1L, 5L, 7L, 17, 2L, 5L, 7L, 17, false)]    // the same major
    [InlineData(1L, 5L, 7L, 17, 2L, 5L, 7L, 16, false)]    // a lower major
    /* The OIDs always count, with or without a changed cluster id. */
    [InlineData(1L, 5L, 7L, 16, 1L, 5L, 8L, 16, false)]    // another table OID
    [InlineData(1L, 5L, 7L, 16, 1L, 6L, 7L, 16, false)]    // another database OID
    [InlineData(1L, 5L, 7L, 16, 2L, 5L, 8L, 17, false)]    // shaped like an upgrade, but the table OID changed
    [InlineData(1L, 5L, 7L, 16, 2L, 6L, 7L, 17, false)]    // shaped like an upgrade, but the database OID changed
    /* A row with its table OID and no major (the column was added to a store that already had rows): the cluster id must match. */
    [InlineData(1L, 5L, 7L, null, 1L, 5L, 7L, 17, true)]
    [InlineData(1L, 5L, 7L, null, 2L, 5L, 7L, 17, false)]  // a changed cluster id with no stored major cannot be shown to be an upgrade
    /* A row made before the table OID was kept (no table OID, no major): the old rule, the cluster id counting when both sides have one. */
    [InlineData(1L, 5L, null, null, 1L, 5L, 7L, 17, true)]
    [InlineData(1L, 5L, null, null, 2L, 5L, 7L, 17, false)] // another cluster id is another store, as before
    [InlineData(1L, 5L, null, null, 2L, 5L, 7L, 16, false)]
    [InlineData(1L, 5L, null, null, 1L, 6L, 7L, 17, false)] // the database's OID always counts
    [InlineData(1L, 5L, null, 16, 2L, 5L, 7L, 17, false)]   // the table OID decides which rule applies, not the major
    /* An unknown cluster id, on either side or both: the two OIDs decide, so a grant that changes later never makes a new id. */
    [InlineData(null, 5L, 7L, 16, 2L, 5L, 7L, 16, true)]
    [InlineData(1L, 5L, 7L, 16, null, 5L, 7L, 16, true)]
    [InlineData(null, 5L, 7L, 16, null, 5L, 7L, 16, true)]
    [InlineData(null, 5L, 7L, null, 2L, 5L, 7L, 17, true)]
    [InlineData(1L, 5L, 7L, null, null, 5L, 7L, 17, true)]
    [InlineData(null, 5L, 7L, 17, null, 5L, 7L, 16, true)]  // no cluster id to have changed, so the major is no reason
    [InlineData(null, 5L, 7L, 16, null, 5L, 8L, 16, false)] // another table OID, and no cluster id to say otherwise
    [InlineData(null, 5L, 7L, 16, null, 6L, 7L, 16, false)] // another database OID
    [InlineData(null, 5L, null, null, 2L, 5L, 7L, 17, true)]
    [InlineData(1L, 5L, null, null, null, 5L, 7L, 17, true)]
    [InlineData(null, 5L, null, null, null, 5L, 7L, 17, true)]
    [InlineData(null, 5L, null, null, null, 6L, 7L, 17, false)]
    public void TheRuleIsTheTwoOidsAndThenTheMajor_AndTheOldRuleForARowWithoutATableOid(
        long? storedCluster, long storedDatabase, long? storedTable, int? storedMajor,
        long? currentCluster, long currentDatabase, long currentTable, int currentMajor,
        bool expectedSame)
    {
        Assert.Equal(expectedSame, StoreInstallId.IsSameStore(
            storedCluster, storedDatabase, storedTable, storedMajor,
            currentCluster, currentDatabase, currentTable, currentMajor));
    }
}
