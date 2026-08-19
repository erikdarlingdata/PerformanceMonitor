/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2218: the storage name carries engine and port, without re-keying anything that already exists.
///
/// <para><b>The defect.</b> <c>server_id</c> was derived from host, database and read-only intent only. So a SQL
/// Server and a PostgreSQL instance on ONE host collided into a single identity and interleaved their histories,
/// and so did two PostgreSQL instances distinguished only by port — both of which #2213 made first-class
/// configuration.</para>
///
/// <para><b>Why the new discriminators are OPTIONAL, which is the whole reason this is safe.</b> Lite derives
/// <c>server_id</c> FRESH at runtime from this function, everywhere, and has no stored-id fallback:
/// <c>RemoteCollectorService.GetServerNameForStorage</c> hashes it on every read. So any change to what this
/// returns for an existing server re-keys it in Lite and orphans all of its collected history, silently — which
/// is the same class of harm as #2158, arrived at from the other direction. Appending nothing at the parameter
/// defaults is what keeps Lite's three-argument call byte-identical.</para>
///
/// <para>These tests are therefore mostly about what did NOT change. This file lives in Lite.Tests deliberately:
/// Lite is the SKU with no stored-id safety net, so it is the one whose invariant needs guarding.</para>
/// </summary>
public sealed class ServerIdentityEngineAndPortTests
{
    /// <summary>
    /// The pre-#2218 implementation, verbatim, as the oracle. Comparing against a re-statement of the rule
    /// rather than against hard-coded strings is what makes "nothing re-keyed" checkable for any input, not
    /// just the handful someone thought to write down.
    /// </summary>
    private static string PreviousImplementation(string serverName, string? databaseName, bool readOnlyIntent)
    {
        var name = string.IsNullOrWhiteSpace(databaseName) ? serverName : serverName + ":" + databaseName;
        return readOnlyIntent ? name + ":RO" : name;
    }

    /// <summary>
    /// THE INVARIANT: Lite's three-argument call is byte-identical to what it produced before, so no Lite
    /// server re-keys and no Lite history is orphaned. Asserted on the derived <c>server_id</c> too, because
    /// that — not the string — is what the stored rows are keyed by.
    /// </summary>
    [Theory]
    [InlineData("SQLPROD01", null, false)]
    [InlineData("SQLPROD01", "SalesDB", false)]
    [InlineData("SQLPROD01", null, true)]
    [InlineData("SQLPROD01", "SalesDB", true)]
    [InlineData("host.contoso.com,1433", null, false)]
    [InlineData("host,49152", "db", true)]
    [InlineData("azure.database.windows.net", "AdventureWorks", false)]
    public void TheThreeArgumentCallIsUnchanged_SoNothingReKeys(string host, string? database, bool readOnlyIntent)
    {
        var expected = PreviousImplementation(host, database, readOnlyIntent);

        Assert.Equal(expected, ServerIdHelper.BuildStorageName(host, database, readOnlyIntent));
        Assert.Equal(
            ServerIdHelper.GetDeterministicHashCode(expected),
            ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, database, readOnlyIntent)));
    }

    /// <summary>
    /// And a SQL Server entry that DOES pass the new arguments is also unchanged — which is what lets Darling
    /// pass <c>Engine</c> and <c>Port</c> unconditionally instead of branching at the call site. <c>Port</c> is
    /// a PostgreSQL-only field (SQL Server carries a non-default port inside the host as <c>host,1433</c>, so it
    /// is already discriminated there), so 0 is the SQL Server case.
    /// </summary>
    [Theory]
    [InlineData("SQLPROD01", null, false)]
    [InlineData("SQLPROD01", "SalesDB", true)]
    [InlineData("host.contoso.com,1433", null, false)]
    public void ASqlServerEntryPassingEngineAndPortIsAlsoUnchanged(string host, string? database, bool readOnlyIntent)
    {
        Assert.Equal(
            PreviousImplementation(host, database, readOnlyIntent),
            ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, "sqlserver", 0));
    }

    /// <summary>
    /// An engine that is blank, SQL Server, or unrecognized appends NOTHING. The unrecognized case matters as
    /// much as the others: a typo in <c>engine</c> must not mint a fresh identity for a server that already has
    /// one, which is exactly what interpolating the raw value would do.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("sqlserver")]
    [InlineData("SQLSERVER")]
    [InlineData("SqlServer")]
    [InlineData("mysql")]
    [InlineData("postgre")]
    public void AnEngineThatIsNotPostgresAppendsNothing(string? engine)
    {
        Assert.Equal("H:D", ServerIdHelper.BuildStorageName("H", "D", false, engine, 0));
    }

    /// <summary>THE FIX, half one: PostgreSQL and SQL Server on one host are now two identities.</summary>
    [Fact]
    public void PostgresAndSqlServerOnOneHostNoLongerCollide()
    {
        var sqlServer = ServerIdHelper.BuildStorageName("box01", null, false, "sqlserver", 0);
        var postgres = ServerIdHelper.BuildStorageName("box01", null, false, "postgres", 0);

        Assert.Equal("box01", sqlServer);
        Assert.Equal("box01:pg", postgres);
        Assert.NotEqual(
            ServerIdHelper.GetDeterministicHashCode(sqlServer),
            ServerIdHelper.GetDeterministicHashCode(postgres));
    }

    /// <summary>THE FIX, half two: two PostgreSQL instances on one host, told apart by port.</summary>
    [Fact]
    public void TwoPostgresInstancesOnOneHostNoLongerCollide()
    {
        var first = ServerIdHelper.BuildStorageName("box01", null, false, "postgres", 5432);
        var second = ServerIdHelper.BuildStorageName("box01", null, false, "postgres", 5433);

        Assert.NotEqual(first, second);
        Assert.NotEqual(
            ServerIdHelper.GetDeterministicHashCode(first),
            ServerIdHelper.GetDeterministicHashCode(second));
    }

    /// <summary>
    /// Every spelling of the engine folds to ONE token, so an operator writing <c>"PostgreSQL"</c> where a
    /// colleague wrote <c>"postgres"</c> does not get a second identity for the same instance — a split history
    /// caused by capitalisation, which nothing downstream could diagnose.
    /// </summary>
    [Theory]
    [InlineData("postgres")]
    [InlineData("PostgreSQL")]
    [InlineData("Postgres")]
    [InlineData("POSTGRESQL")]
    [InlineData("postgresql")]
    [InlineData("pg")]
    [InlineData("PG")]
    [InlineData("  postgres  ")]
    public void EverySpellingOfPostgresFoldsToOneIdentity(string engine)
    {
        Assert.Equal("box01:pg", ServerIdHelper.BuildStorageName("box01", null, false, engine, 0));
    }

    /// <summary>
    /// The suffix order is fixed — engine, then port, then <c>:RO</c>. Two callers supplying the same facts
    /// must not be able to produce two names, and the read-only marker stays last so the existing convention
    /// (and anything that reads a name by eye) is preserved.
    /// </summary>
    [Theory]
    [InlineData("h", "d", true, "postgres", 5433, "h:d:pg:5433:RO")]
    [InlineData("h", null, true, "postgres", 0, "h:pg:RO")]
    [InlineData("h", null, false, null, 5433, "h:5433")]
    [InlineData("h", "d", false, "postgres", 0, "h:d:pg")]
    public void TheSuffixOrderIsEngineThenPortThenReadOnly(
        string host, string? database, bool readOnlyIntent, string? engine, int port, string expected)
    {
        Assert.Equal(expected, ServerIdHelper.BuildStorageName(host, database, readOnlyIntent, engine, port));
    }

    /// <summary>
    /// A zero or negative port appends nothing. Zero is the real default (the field means "the driver's
    /// default"), and a negative value is nonsense that must not become part of an identity.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void AnAbsentOrNonsensePortAppendsNothing(int port)
    {
        Assert.Equal("h", ServerIdHelper.BuildStorageName("h", null, false, null, port));
    }
}
