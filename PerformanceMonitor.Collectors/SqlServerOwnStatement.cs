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
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7730: the monitor's own statements that builds before #7463 sent to a user database WITHOUT the marker
/// (<see cref="QueryStoreCollector.SelfQueryMarker"/>), as one closed list. #7463 made each statement carry the marker as an alias,
/// so a text sent after it is recognized by the marker alone. A text recorded before the upgrade, or sent by an older build that
/// still polls the server, has no marker, so every rule that hides the monitor's own statements reads this list as well: the
/// collector's drop (<see cref="QueryStoreCollector"/>), Darling's key trigger and backfill, and Lite.
///
/// <para><b>The match is the whole text, with every whitespace character removed, equal to an entry.</b> Nothing looser: a person's
/// query that only resembles one of these stays listed. The statement text SQL Server keeps starts at the statement (a comment above
/// it in the batch is not part of it) and ends before its semicolon, and a database with PARAMETERIZATION FORCED stores a normalized
/// text that is not on this list. Whitespace is the six ASCII characters, so the Postgres test (<see cref="WhitespaceClassForPostgres"/>)
/// and this one cannot disagree.</para>
///
/// <para><b>Exact texts.</b> Each entry is the text of a statement as <c>git show 661e6bf47^</c> sends it, with the values the build
/// resolved at run time written out (the forced-plans row cap, the forcing-type column of each of its two shapes). A statement
/// whose text carries a run-time list (an id list, a row cap that changes) is not a closed text and is not here. The list covers the
/// statements that stayed listed on a store in the field: the two eligibility gates, the forced-plans read and its object-id update.</para>
/// </summary>
public static class SqlServerOwnStatement
{
    /// <summary>The plan-correction collector's readable-secondary gate (a leading batch comment is not part of the statement).</summary>
    private const string PlanCorrectionReplicaGate = @"
IF EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE readonly_reason & 8 = 8
)";

    /// <summary>The Query Store and forced-plans collectors' eligibility gate.</summary>
    private const string EligibilityGate = @"
IF NOT EXISTS
(
    SELECT
        1
    FROM sys.database_query_store_options
    WHERE actual_state IN (1, 2, 4)
    AND   readonly_reason & 8 = 0
)";

    private const string ForcedPlansReadHead = @"
SELECT TOP (500)
    plan_id = qsp.plan_id,
    query_id = qsp.query_id,
    object_id = CONVERT(bigint, NULL),
    query_plan_hash = CONVERT(varchar(64), qsp.query_plan_hash, 1),
";

    private const string ForcedPlansReadTail = @"
    force_failure_count = CONVERT(bigint, qsp.force_failure_count),
    last_force_failure_reason = NULLIF(qsp.last_force_failure_reason_desc, N'NONE'),
    last_execution_time = CONVERT(datetime2, qsp.last_execution_time AT TIME ZONE 'UTC'),
    last_compile_start_time = CONVERT(datetime2, qsp.last_compile_start_time AT TIME ZONE 'UTC')
INTO #pm_forced_plans
FROM sys.query_store_plan AS qsp
WHERE qsp.is_forced_plan = 1
ORDER BY qsp.plan_id
OPTION(RECOMPILE)";

    private const string ForcedPlansObjectIdUpdate = @"
UPDATE f
SET f.object_id = CONVERT(bigint, qsq.object_id)
FROM #pm_forced_plans AS f
JOIN sys.query_store_query AS qsq
    ON qsq.query_id = f.query_id
OPTION(RECOMPILE)";

    /// <summary>The closed list: the old texts exactly as the builds before #7463 sent them.</summary>
    public static IReadOnlyList<string> OldTexts { get; } =
    [
        PlanCorrectionReplicaGate,
        EligibilityGate,
        ForcedPlansReadHead + "    plan_forcing_type = qsp.plan_forcing_type_desc," + ForcedPlansReadTail,
        ForcedPlansReadHead + "    plan_forcing_type = CONVERT(nvarchar(60), NULL)," + ForcedPlansReadTail,
        ForcedPlansObjectIdUpdate,
    ];

    /// <summary>The Postgres regex class that matches exactly the characters <see cref="StripWhitespace"/> removes.</summary>
    public const string WhitespaceClassForPostgres = @"[ \t\n\r\f\v]";

    private static readonly string[] s_stripped = OldTexts.Select(StripWhitespace).ToArray();

    /// <summary>Each entry with its whitespace removed.</summary>
    public static IReadOnlyList<string> StrippedTexts => s_stripped;

    /// <summary>The longest entry, whitespace removed, in characters.</summary>
    public static int LongestStrippedLength { get; } = s_stripped.Max(t => t.Length);

    /// <summary>
    /// A text longer than this cannot be an old statement: it would hold more than seven times its own length in whitespace. The
    /// Postgres test applies the same bound first, so the stored-text trigger and the backfill do not run a regular expression over
    /// every large text, and this test agrees with it.
    /// </summary>
    public static int MaxRawLength => LongestStrippedLength * 8;

    /// <summary>The lowercase hex SHA-256 of each stripped entry (UTF-8). The Postgres test compares digests, so the statements that
    /// carry it stay a few hundred bytes (<c>encode(sha256(convert_to(...)), 'hex')</c>), while the match is still of the whole text.</summary>
    public static IReadOnlyList<string> StrippedDigests { get; } = s_stripped.Select(Digest).ToArray();

    /// <summary>The text with every ASCII whitespace character removed.</summary>
    public static string StripWhitespace(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var builder = new StringBuilder(text.Length);
        foreach (var c in text)
        {
            if (!IsWhitespace(c))
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }

    /// <summary>True when <paramref name="text"/> is, whitespace aside, one of <see cref="OldTexts"/>.</summary>
    public static bool IsOldStatement(string? text)
    {
        if (string.IsNullOrEmpty(text) || text.Length > MaxRawLength)
        {
            return false;
        }

        var stripped = StripWhitespace(text);
        foreach (var entry in s_stripped)
        {
            if (string.Equals(stripped, entry, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True for a text that carries the marker, or is an old statement.</summary>
    public static bool IsOwnStatement(string? text) =>
        text is not null && (text.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal) || IsOldStatement(text));

    private static bool IsWhitespace(char c) => c is ' ' or '\t' or '\n' or '\r' or '\f' or '\v';

    private static string Digest(string stripped) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(stripped))).ToLowerInvariant();
}
