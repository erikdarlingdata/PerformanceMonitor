/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2220: which databases one Azure SQL DB registration's per-database sweep covers.
///
/// <para><b>The field report.</b> A single real deadlock in one Azure SQL Database produced near-identical
/// "Deadlocks Detected" alerts on every OTHER monitored database sharing the same logical server — and the
/// stored data matched: byte-identical deadlock graphs and the same top query, with counters within ~1%,
/// under six unrelated <c>server_id</c>s. Azure SQL DB engines are isolated per database, so one database's
/// sessions cannot block another's; the rows were not cross-talk, they were the same rows collected six
/// times.</para>
///
/// <para><b>The cause, and why it is not a typo.</b> The enumeration read <c>master</c> unconditionally and
/// swept every online database on the logical server, storing all of it under whichever registration ran the
/// sweep. Two parts of the product hold incompatible ideas of what a registration IS, both deliberate: the
/// enumeration assumes one registration = one LOGICAL SERVER (#857's shape), while identity assumes one
/// registration = one DATABASE (<c>server_id</c> hashes <c>host[:database][:RO]</c>, and the Azure
/// query_store path needs a per-database connection anyway, #1836). The second shape silently behaved like
/// the first, N times over — N registrations of N databases is N² collection.</para>
///
/// <para>Pinned in BOTH suites against the shared implementation — this is Lite's half. A scoping rule that
/// disagrees between Lite and Darling is the same class of defect as the one being fixed, and both runners
/// previously carried their own private copy of it.</para>
/// </summary>
public sealed class AzureSweepScopeTests
{
    /// <summary>
    /// THE FIX. A registration naming a database sweeps exactly that database — the reported case, where
    /// fifteen registrations on one logical server each swept all fifteen.
    /// </summary>
    [Theory]
    [InlineData("db1")]
    [InlineData("AdventureWorks")]
    [InlineData("Sibling-A")]
    public void ARegistrationThatNamesADatabase_SweepsOnlyThatDatabase(string catalog)
    {
        Assert.Equal(new[] { catalog }, AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>
    /// A registration naming NO database is a registration of the logical server, so it must enumerate —
    /// signalled by the empty list rather than by a separate flag, because the caller's next step is a list
    /// either way. This is the behaviour #857 was written for and it is deliberately unchanged.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ARegistrationThatNamesNoDatabase_StillEnumeratesTheServer(string? catalog)
    {
        Assert.Empty(AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>
    /// <c>master</c> counts as naming none, in any casing. A connection string with no
    /// <c>Initial Catalog</c> lands in <c>master</c> on Azure SQL DB, so treating it as a named database
    /// would scope such a registration to the one database holding none of the user's data — collecting
    /// nothing and looking healthy while doing it.
    /// </summary>
    [Theory]
    [InlineData("master")]
    [InlineData("MASTER")]
    [InlineData("Master")]
    public void MasterIsNotADatabaseAnyoneRegisteredFor(string catalog)
    {
        Assert.Empty(AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>
    /// A database that merely CONTAINS "master" is a real database and is swept. Guards the obvious
    /// over-match, which would silently stop collecting from it.
    /// </summary>
    [Theory]
    [InlineData("mastermind")]
    [InlineData("paymaster")]
    [InlineData("master_archive")]
    public void ADatabaseNamedLikeMasterIsStillItsOwnDatabase(string catalog)
    {
        Assert.Equal(new[] { catalog }, AzureSweepScope.OwnDatabaseOrEmpty(catalog));
    }

    /// <summary>
    /// The returned list is the caller's to keep: both runners hand it straight to their per-database loop,
    /// and a shared or cached instance would let one sweep's mutation reach another's.
    /// </summary>
    [Fact]
    public void EachCallReturnsItsOwnList()
    {
        var first = AzureSweepScope.OwnDatabaseOrEmpty("db1");
        var second = AzureSweepScope.OwnDatabaseOrEmpty("db1");

        Assert.NotSame(first, second);
        first.Add("mutated");
        Assert.Single(second);
    }
}
