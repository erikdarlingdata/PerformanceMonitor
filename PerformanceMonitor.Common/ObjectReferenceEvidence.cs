/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// One name that Query Store statements named as a table source over the window (#8784 part 2), with the executions of those statements.
/// The name is as the statements wrote it, after the parse: <paramref name="SchemaName"/> is empty where a statement left the schema out, and
/// such a name binds through the caller's default schema, so it is matched against the name under any schema.
/// </summary>
/// <param name="SchemaName">The schema as written, or empty when the statement gave none.</param>
/// <param name="ObjectName">The object name as written.</param>
/// <param name="Executions">The executions, summed over the window, of every statement that names it. A statement that names it twice counts once.</param>
/// <param name="LastExecutionUtc">The newest execution of any such statement.</param>
/// <param name="TextMatched">True where at least one statement could not be parsed and the name was found by a whole-word search of its text
/// instead, which can also match a name inside a comment or a string.</param>
/// <param name="IsFunction">True where the statements named it as a table-valued function (<c>FROM dbo.f(1)</c>), not as a table or view.</param>
public sealed record ObjectReferenceTotals(
    string SchemaName, string ObjectName, long Executions, DateTime? LastExecutionUtc, bool TextMatched = false, bool IsFunction = false);

/// <summary>What the stored Query Store statement text says about the names it references over the window (#8784 part 2).</summary>
/// <param name="CoveredFromUtc">The oldest day of statement text the read found for this database in the window, or null with none.</param>
/// <param name="CoveredToUtc">The end of the newest such day.</param>
/// <param name="DaysWithData">How many days of the window hold statement text for this database, or null where the read cannot say.</param>
/// <param name="Source">Where the references came from, a sentence the reply carries.</param>
/// <param name="References">One row per referenced name.</param>
/// <param name="Statements">How many statements were looked at (a statement on two days counts twice on Darling, where each day is summarised
/// on its own).</param>
/// <param name="StatementsWithoutText">How many of them had no stored text, so could not be looked at.</param>
/// <param name="StatementsUnparsed">How many of them did not parse: searched as text for the database's views where an object list is kept,
/// and otherwise not looked into.</param>
public sealed record ObjectReferenceWindow(
    DateTime? CoveredFromUtc,
    DateTime? CoveredToUtc,
    int? DaysWithData,
    string Source,
    IReadOnlyList<ObjectReferenceTotals> References,
    long Statements = 0,
    long StatementsWithoutText = 0,
    long StatementsUnparsed = 0);

/// <summary>
/// One edge of the database's dependency list (<c>sys.sql_expression_dependencies</c>, #8784 part 2), already filtered of the rows that are
/// never a reference to a user object (see <c>ObjectDependencyFilter</c>): a module, view or trigger that names another object in its
/// definition.
/// </summary>
/// <param name="ReferencingSchema">The schema of the object whose definition holds the reference.</param>
/// <param name="ReferencingName">Its name.</param>
/// <param name="ReferencingType">Its <c>sys.objects.type</c>, trimmed (e.g. <c>V</c>, <c>P</c>, <c>IF</c>, <c>TR</c>).</param>
/// <param name="ReferencedSchema">The referenced object's schema: the bound object's own where SQL Server bound the name, otherwise as
/// written, empty when the definition gave none.</param>
/// <param name="ReferencedName">The referenced object's name, the same way.</param>
/// <param name="IsBound">True where SQL Server bound the name to an object (<c>referenced_id</c> is not null), so the schema is exact.</param>
/// <param name="ReferencingDatabase">Null for an edge of the database's own list. For an edge from another database's list into this one
/// (<c>A.dbo.vReport</c> reading <c>B.dbo.vCustomer</c>, read for B), the database the referencing object is in: such an edge's target is
/// taken as used, since the read does not weigh the other database's own evidence.</param>
public sealed record ObjectDependency(
    string ReferencingSchema,
    string ReferencingName,
    string ReferencingType,
    string ReferencedSchema,
    string ReferencedName,
    bool IsBound,
    string? ReferencingDatabase = null);

/// <summary>The stored dependency list of one database, as of its newest capture (#8784 part 2).</summary>
/// <param name="CapturedAtUtc">When the list was captured.</param>
/// <param name="Dependencies">The edges.</param>
public sealed record ObjectDependencyList(DateTime CapturedAtUtc, IReadOnlyList<ObjectDependency> Dependencies);
