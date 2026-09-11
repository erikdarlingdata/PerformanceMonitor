/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>The database and object a <c>Proc [Database Id = N Object Id = M]</c> placeholder names.</summary>
public readonly record struct ProcPlaceholderId(int DatabaseId, int ObjectId);

/// <summary>
/// Resolution of SQL Server's <c>Proc [Database Id = N Object Id = M]</c> placeholder to
/// <c>database.schema.object</c> (#3307).
///
/// <para>SQL Server writes that placeholder into a process's <c>&lt;inputbuf&gt;</c> whenever the batch it
/// is running is a stored procedure invoked as an RPC rather than submitted as text — so a deadlock or a
/// blocked-process report whose statement sits inside a procedure names an object id where an on-call
/// engineer needs a procedure name. Both surfaces shred <c>&lt;inputbuf&gt;</c> CLIENT-side
/// (<c>DeadlocksCollector.ExtractVictimFields</c>, <c>BlockedProcessReportCollector.ParseReportXml</c>), so
/// the parse lives here in C# and only the id-to-name lookup goes to the target, batched once per cycle
/// through the definitions' supplemental-query seam.</para>
///
/// <para><b>The technique is ported from <c>sp_HumanEventsBlockViewer</c> and <c>sp_BlitzLock</c></b>, which
/// resolve it identically: match the placeholder, take the two ids out of the text, and project
/// <c>OBJECT_SCHEMA_NAME</c> + <c>'.'</c> + <c>OBJECT_NAME</c>. Three deliberate departures:</para>
///
/// <para><b>The name is three-part, not two.</b> Those two procedures each report on the database they
/// run in, where a database prefix would be noise. This reports on a fleet, and everything else in the
/// same alert names an object in three parts: the incident's Involved Objects field
/// (<c>AlertIncidentRenderTests</c> pins <c>SalesDB.dbo.Orders</c>), the deadlock graph's own
/// <c>keylock/@objectname</c>, and — on a live production graph for exactly this defect — the graph's own
/// <c>frame/@procname</c>. So two parts is what would make this field the odd one out, which is the thing
/// the references' qualification was for.</para>
///
/// <para>The references' <c>LIKE N'Proc |[Database Id = %' ESCAPE N'|'</c> needs the <c>ESCAPE</c> because
/// <c>[</c> opens a character class in <c>LIKE</c> — without it the predicate is a range expression that
/// matches nothing and does not error. A C# ordinal comparison has no such metacharacter, so that failure
/// mode does not exist on this path; the bracket is load-bearing only as a literal, which
/// <c>ProcPlaceholderResolutionTests</c> pins by mutating it out.</para>
///
/// <para>The references concatenate the two names server-side, so a procedure the monitoring login cannot
/// see resolves as <c>NULL + N'.' + NULL</c> — NULL — and BLANKS the field. Here the target projects the
/// two names separately and <see cref="Resolve"/> keeps the raw placeholder unless BOTH came back. An
/// object id is a worse answer than a name and a much better one than nothing: it is still resolvable by
/// hand.</para>
/// </summary>
public static class ProcPlaceholder
{
    /// <summary>
    /// The placeholder's literal opening, matched ordinally. Exactly the references' <c>LIKE</c> pattern
    /// with the <c>ESCAPE</c> removed, because nothing here treats <c>[</c> as a metacharacter.
    /// </summary>
    private const string Prefix = "Proc [Database Id = ";

    /// <summary>What separates the two ids. The references' <c>CHARINDEX(N'Object Id = ', …) + 12</c>.</summary>
    private const string ObjectIdMarker = " Object Id = ";

    /// <summary>
    /// Distinct pairs one cycle will ask the target to resolve. A cycle whose events name more procedures
    /// than this resolves the first <see cref="MaxLookupsPerCycle"/> and leaves the rest showing their
    /// object ids — the same degradation as a failed lookup, and it keeps one pathological sweep from
    /// building an unbounded <c>VALUES</c> list. blocked_process_report is the surface that can produce
    /// hundreds of events in a cycle; the DISTINCT procedures behind them are normally a handful.
    /// </summary>
    public const int MaxLookupsPerCycle = 500;

    /// <summary>
    /// One batched lookup for every pair a cycle turned up. Text held apart from
    /// <see cref="BuildResolutionQuery"/> so the T-SQL is one literal a reader (and the convention guard)
    /// can see whole; the pairs splice in at the marker, exactly as the collectors' own plan fragments do.
    ///
    /// <para>The ids are bound as literals rather than parameters BECAUSE they are already integers — they
    /// came out of <see cref="TryParse"/> as <c>int</c>, so there is no string to inject through, and a
    /// per-pair parameter list would cap the batch far below what one sweep can produce.
    /// <c>OPTION(RECOMPILE)</c> is what keeps that variable-length literal list from interning a plan per
    /// distinct pair count.</para>
    ///
    /// <para><c>OBJECT_SCHEMA_NAME</c> and <c>OBJECT_NAME</c> return NULL rather than erroring when the
    /// monitoring login has no access to that database or the object has been dropped since the event, so
    /// the whole batch survives a pair it cannot resolve.</para>
    /// </summary>
    private const string ResolutionQueryText = @"
SELECT
    database_id = ids.database_id,
    object_id = ids.object_id,
    database_name = DB_NAME(ids.database_id),
    schema_name = OBJECT_SCHEMA_NAME(ids.object_id, ids.database_id),
    object_name = OBJECT_NAME(ids.object_id, ids.database_id)
FROM
(
    VALUES
        /*PROC_ID_PAIRS*/
) AS ids
(
    database_id,
    object_id
)
OPTION(RECOMPILE);";

    /// <summary>
    /// True when <paramref name="text"/> is the unresolved placeholder, yielding the ids it names.
    ///
    /// <para><b>The leading-noise skip is load-bearing and fails silently without a fixture for it.</b> The
    /// references prepend <c>@inputbuf_bom</c> to their pattern, built as
    /// <c>CONVERT(nvarchar(1), 0x0a00, 0)</c> — UTF-16LE <c>0x000A</c>, a line feed, which is what SQL
    /// Server actually puts in front of <c>&lt;inputbuf&gt;</c> element text. A real byte-order mark
    /// (U+FEFF) can lead it too, and <c>string.Trim()</c> — which both callers already apply — does NOT
    /// remove U+FEFF, because .NET classifies it as a format character and not whitespace. So the BOM
    /// survives trimming and a prefix test without this skip misses every BOM-prefixed row without
    /// complaining. Both arms carry a fixture.</para>
    ///
    /// <para>The object id may be negative (system objects), the database id never is. Anything that is not
    /// the full shape — INCLUDING text that merely opens with it and carries something after the closing
    /// bracket — returns false and leaves the text alone, rather than the references' behaviour of
    /// matching on the prefix and then letting an implicit int conversion decide.</para>
    /// </summary>
    public static bool TryParse(string? text, out ProcPlaceholderId id)
    {
        id = default;

        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var rest = text.AsSpan();
        var lead = 0;
        while (lead < rest.Length && IsIgnorablePadding(rest[lead]))
        {
            lead++;
        }

        rest = rest[lead..];

        if (!rest.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        rest = rest[Prefix.Length..];

        if (!TakeInteger(ref rest, allowSign: false, out var databaseId))
        {
            return false;
        }

        if (!rest.StartsWith(ObjectIdMarker, StringComparison.Ordinal))
        {
            return false;
        }

        rest = rest[ObjectIdMarker.Length..];

        if (!TakeInteger(ref rest, allowSign: true, out var objectId))
        {
            return false;
        }

        /* The closing bracket AND nothing but padding after it, which is what the doc comment's "full
           shape" claim needs to be true. Without this the parse returns true for any text that merely
           OPENS with the placeholder — and Resolve replaces the WHOLE input, so whatever followed would
           be silently discarded rather than declined. SQL Server writes the placeholder as the entire
           inputbuf value (space-padded: "]   "), so requiring it costs no coverage. A value truncated
           before the bracket is still accepted: the ids are the thing being read, and declining would
           throw away the only answer available. */
        if (!rest.IsEmpty)
        {
            if (rest[0] != ']')
            {
                return false;
            }

            rest = rest[1..];

            for (var i = 0; i < rest.Length; i++)
            {
                if (!IsIgnorablePadding(rest[i]))
                {
                    return false;
                }
            }
        }

        id = new ProcPlaceholderId(databaseId, objectId);
        return true;
    }

    /// <summary>
    /// Records the pair <paramref name="text"/> names, if it is a placeholder at all, for the cycle's one
    /// batched lookup. Distinct, capped, and order-preserving so the query text is deterministic.
    /// </summary>
    public static void Register(string? text, IList<ProcPlaceholderId> ids)
    {
        ArgumentNullException.ThrowIfNull(ids);

        if (!TryParse(text, out var id) || ids.Contains(id) || ids.Count >= MaxLookupsPerCycle)
        {
            return;
        }

        ids.Add(id);
    }

    /// <summary>
    /// The cycle's lookup, or null when nothing needs resolving — which is the common case, so a server
    /// whose deadlocks never sit inside a procedure pays no extra round trip at all.
    /// </summary>
    public static CollectorQuery? BuildResolutionQuery(IReadOnlyList<ProcPlaceholderId> ids)
    {
        ArgumentNullException.ThrowIfNull(ids);

        if (ids.Count == 0)
        {
            return null;
        }

        /* One line, separator spelled out rather than Environment.NewLine: the query text a definition
           builds has to be identical on the Linux build and the Windows test job, and a platform newline
           spliced into it is exactly the kind of difference a text pin cannot see but a diff of two runs
           can. */
        var pairs = new StringBuilder();
        for (var i = 0; i < ids.Count; i++)
        {
            if (i > 0)
            {
                pairs.Append(", ");
            }

            pairs.Append(CultureInfo.InvariantCulture, $"({ids[i].DatabaseId}, {ids[i].ObjectId})");
        }

        return new CollectorQuery(
            ResolutionQueryText.Replace("/*PROC_ID_PAIRS*/", pairs.ToString(), StringComparison.Ordinal));
    }

    /// <summary>
    /// Reads the lookup's result set into a pair-to-<c>database.schema.object</c> map, positionally — the
    /// reader contract on this seam is by ordinal, and the collectors' test fake throws from
    /// <c>GetName</c> deliberately.
    ///
    /// <para>A pair missing ANY of the three parts is simply ABSENT from the map, so there is no partial
    /// name for <see cref="Resolve"/> to fall for. This is the one place a NULL could have been
    /// concatenated into a blanked field. All three fail together in practice — naming a database, its
    /// schema and its object all need the same access to it — so the all-or-nothing rule costs no
    /// coverage and leaves exactly one outcome to reason about.</para>
    /// </summary>
    public static async ValueTask<Dictionary<ProcPlaceholderId, string>> ReadResolutionsAsync(
        DbDataReader reader,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var resolved = new Dictionary<ProcPlaceholderId, string>();

        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1)
                || reader.IsDBNull(2) || reader.IsDBNull(3) || reader.IsDBNull(4))
            {
                continue;
            }

            var databaseName = reader.GetString(2);
            var schemaName = reader.GetString(3);
            var objectName = reader.GetString(4);

            if (databaseName.Length == 0 || schemaName.Length == 0 || objectName.Length == 0)
            {
                continue;
            }

            resolved[new ProcPlaceholderId(reader.GetInt32(0), reader.GetInt32(1))] =
                databaseName + "." + schemaName + "." + objectName;
        }

        return resolved;
    }

    /// <summary>
    /// The resolved <c>database.schema.object</c> when <paramref name="text"/> is a placeholder the lookup
    /// answered for, and <paramref name="text"/> UNCHANGED otherwise — not blank, and never "unknown".
    /// Three-part because that is how every other object name in the same alert renders (see the class
    /// remarks), so anything shorter would be the odd field out in its own message.
    /// </summary>
    public static string? Resolve(string? text, IReadOnlyDictionary<ProcPlaceholderId, string> resolved)
    {
        ArgumentNullException.ThrowIfNull(resolved);

        return TryParse(text, out var id) && resolved.TryGetValue(id, out var name)
            ? name
            : text;
    }

    /// <summary>
    /// What may surround the placeholder without changing it: the references' <c>@inputbuf_bom</c> line
    /// feed and any other whitespace, plus a real byte-order mark, which trimming leaves behind. Used on
    /// both ends — SQL Server leads the value with a newline and pads it with spaces.
    /// </summary>
    private static bool IsIgnorablePadding(char c) => c == '\uFEFF' || char.IsWhiteSpace(c);

    /// <summary>Consumes the digits (and optional sign) at the head of <paramref name="rest"/>.</summary>
    private static bool TakeInteger(ref ReadOnlySpan<char> rest, bool allowSign, out int value)
    {
        value = 0;

        var length = 0;
        if (allowSign && rest.Length > 0 && rest[0] == '-')
        {
            length++;
        }

        var digits = 0;
        while (length < rest.Length && char.IsAsciiDigit(rest[length]))
        {
            length++;
            digits++;
        }

        if (digits == 0 || !int.TryParse(rest[..length], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value))
        {
            return false;
        }

        rest = rest[length..];
        return true;
    }
}
