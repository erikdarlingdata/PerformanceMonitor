/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;

namespace PerformanceMonitor.Common;

/// <summary>
/// SQL Server's own housekeeping statements (#6548): work the engine or a system job issues, that is not application code and
/// cannot usefully be forced or tuned. Change tracking cleanup (<c>delete top(@batch_size) from sys.change_tracking_&lt;id&gt;</c>,
/// <c>sp_flush_commit_table</c>) and change data capture cleanup (<c>sp_cdc_cleanup_change_table</c>) are the known ones. A
/// regression of one of these (a Query Store plan change on a cleanup delete reads as thousands of times slower) is real but
/// is not the cause an investigation is looking for, and it used to lead the regression targets and the plan-regression findings.
/// <para>Query Store marks such queries <c>is_internal_query = 1</c>, but the collectors do not collect that column, and adding it
/// is a schema change. So the rule is a text match on the statement, the same way the collectors already leave out their own
/// queries by text (the <c>PerformanceMonitorLite</c> marker) and the readers leave out <c>WAITFOR</c>. The wording is the engine's
/// own, so a user query is not caught by accident: a DMV read such as <c>sys.change_tracking_tables</c> does not match.</para>
/// </summary>
public static class InternalStatements
{
    /// <summary>
    /// One pattern for the lower-cased statement text. The same text works as a PostgreSQL regex (<c>lower(text) ~ pattern</c>)
    /// and a DuckDB one (<c>regexp_matches(lower(text), pattern)</c>), so the SQL sites in Darling and Lite share one list. A
    /// constant, so it can be spliced into the SQL constants. The <c>@</c> of <c>@batch_size</c> is written <c>\W</c> (any
    /// non-word character: the same match in .NET, PostgreSQL and DuckDB) so the pattern carries no <c>@name</c> token: the
    /// PostgreSQL SQL has no named parameters (only <c>$N</c>), and the dialect guard in <c>PgFactCollectorTests</c> rejects
    /// an <c>@</c> anywhere in that SQL. SQL Server records the cleanup with a bracketed side-table name, as in <c>sys.[change_tracking_1228027148]</c>,
    /// so the <c>[</c> is optional (<c>\[?</c>): the unbracketed form is valid T-SQL too. The test fixtures copy the bracketed form from stored text.
    /// </summary>
    public const string LowerTextPattern =
        @"delete\s+top\s*\(\s*\Wbatch_size\s*\)\s+from\s+sys\.\[?change_tracking_|sp_flush_commit_table|sp_cdc_cleanup_change_table";

    private static readonly Regex Matcher = new(LowerTextPattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>True when <paramref name="statementText"/> is one of SQL Server's own housekeeping statements.</summary>
    public static bool IsInternal(string? statementText) =>
        !string.IsNullOrEmpty(statementText) && Matcher.IsMatch(statementText);
}
