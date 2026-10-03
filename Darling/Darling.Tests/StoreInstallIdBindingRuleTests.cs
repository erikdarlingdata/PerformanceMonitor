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
/// A managed major upgrade makes a new cluster id and keeps the database's OID and the install id table's OID, so a row
/// that has its table OID is decided by the two OIDs alone, and a changed cluster id is never a different store. A row
/// made before the table OID was kept has none, and is compared as it was before: by the database's OID, and by the
/// cluster id when the row and the current read both have one.
/// </summary>
public sealed class StoreInstallIdBindingRuleTests
{
    // stored cluster id, stored database OID, stored table OID, current cluster id, current database OID, current table OID, same store?
    [Theory]
    /* A row with its table OID: the two OIDs decide. */
    [InlineData(1L, 5L, 7L, 1L, 5L, 7L, true)]      // nothing changed
    [InlineData(1L, 5L, 7L, 2L, 5L, 7L, true)]      // a major upgrade: new cluster id, both OIDs kept
    [InlineData(null, 5L, 7L, 2L, 5L, 7L, true)]    // stored without a cluster id, now readable
    [InlineData(1L, 5L, 7L, null, 5L, 7L, true)]    // the cluster id is refused now
    [InlineData(null, 5L, 7L, null, 5L, 7L, true)]  // refused both times
    [InlineData(1L, 5L, 7L, 1L, 5L, 8L, false)]     // another table OID: a restored copy
    [InlineData(null, 5L, 7L, null, 5L, 8L, false)] // another table OID, and no cluster id to say otherwise
    [InlineData(1L, 5L, 7L, 2L, 5L, 8L, false)]     // another table OID and another cluster id
    [InlineData(1L, 5L, 7L, 1L, 6L, 7L, false)]     // another database OID
    [InlineData(1L, 5L, 7L, 2L, 6L, 7L, false)]     // another database OID and another cluster id
    /* A row made before the table OID was kept: the old rule, the cluster id counting when both sides have one. */
    [InlineData(1L, 5L, null, 1L, 5L, 7L, true)]
    [InlineData(1L, 5L, null, 2L, 5L, 7L, false)]   // the old rule: another cluster id is another store
    [InlineData(null, 5L, null, 2L, 5L, 7L, true)]
    [InlineData(1L, 5L, null, null, 5L, 7L, true)]
    [InlineData(null, 5L, null, null, 5L, 7L, true)]
    [InlineData(1L, 5L, null, 1L, 6L, 7L, false)]   // the database's OID always counts
    [InlineData(null, 5L, null, null, 6L, 7L, false)]
    public void TheRuleIsTheTwoOidsOnceARowHasTheTableOid_AndTheOldRuleUntilThen(
        long? storedCluster, long storedDatabase, long? storedTable,
        long? currentCluster, long currentDatabase, long currentTable,
        bool expectedSame)
    {
        Assert.Equal(expectedSame, StoreInstallId.IsSameStore(
            storedCluster, storedDatabase, storedTable, currentCluster, currentDatabase, currentTable));
    }
}
