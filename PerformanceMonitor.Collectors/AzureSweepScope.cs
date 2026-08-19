/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which databases one Azure SQL DB registration's per-database sweep covers (#2220).
///
/// <para><b>The defect this decides.</b> A database-scoped collector on Azure SQL DB used to enumerate
/// <c>master</c> unconditionally and sweep EVERY online database on the logical server, storing all of it
/// under the one <c>server_id</c> of whichever registration ran the sweep. On a logical server holding N
/// separately-registered databases that is N registrations × N databases: N² collection, and every
/// registration's stored history contaminated with its siblings'. The field report is byte-identical
/// deadlock graphs and the same top query appearing under six unrelated identities, which is exactly what
/// reading one database's rows six times looks like.</para>
///
/// <para><b>Why it happened rather than being an oversight.</b> Two parts of the product hold incompatible
/// ideas of what an Azure SQL DB registration IS, and both are deliberate. The enumeration assumes one
/// registration = one LOGICAL SERVER, which is the shape #857 was written for. Identity assumes one
/// registration = one DATABASE: <c>server_id</c> is the hash of <c>host[:database][:RO]</c>, so registering
/// two databases on one server is the supported way to get two identities — and the Azure query_store path
/// (#1836) needs a per-database connection anyway. Nothing reconciled the two, so the second shape silently
/// behaved like the first, N times over.</para>
///
/// <para><b>The rule.</b> A registration that NAMES a database is a registration OF that database, and its
/// sweep covers exactly that one. Only a registration that names none — or names <c>master</c>, which on
/// Azure SQL DB is where a catalog-less connection lands — is a registration of the logical SERVER, and only
/// that one enumerates.</para>
///
/// <para>Shared rather than duplicated per host on purpose: both runners had their own private copy of this
/// predicate, and a scoping rule that disagrees between Lite and Darling is the same class of bug as the one
/// being fixed. One implementation, pinned by both test suites.</para>
/// </summary>
public static class AzureSweepScope
{
    /// <summary>
    /// The single database this registration names, or an EMPTY list when it names none.
    ///
    /// <para>Empty is not "no databases" — it means "this registration is of the logical server, so the
    /// caller must enumerate". The two are told apart by the count, which is why this returns a list rather
    /// than a nullable string: the caller's next step is a list either way.</para>
    ///
    /// <para><c>master</c> counts as naming none. A connection string with no <c>Initial Catalog</c> lands in
    /// <c>master</c> on Azure SQL DB, so treating it as a named database would scope every catalog-less
    /// registration to a database that holds none of the user's data.</para>
    /// </summary>
    public static List<string> OwnDatabaseOrEmpty(string? initialCatalog)
    {
        if (string.IsNullOrEmpty(initialCatalog)
            || string.Equals(initialCatalog, "master", StringComparison.OrdinalIgnoreCase))
        {
            return new List<string>();
        }

        return new List<string> { initialCatalog };
    }
}
