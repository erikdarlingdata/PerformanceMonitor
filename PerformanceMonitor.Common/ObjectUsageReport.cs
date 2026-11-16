/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>One module's executions over the window, summed from Query Store (#8784). <c>LastExecutionUtc</c> is UTC, as Query Store stores it.</summary>
/// <param name="ModuleName">The module as Query Store names it: <c>schema.name</c>.</param>
/// <param name="Executions">Executions summed over the window.</param>
/// <param name="LastExecutionUtc">The newest execution seen.</param>
public sealed record ObjectExecutionTotals(string ModuleName, long Executions, DateTime? LastExecutionUtc);

/// <summary>What the stored Query Store data says about module executions over the window (#8784).</summary>
/// <param name="CoveredFromUtc">The oldest Query Store data the read found in the window. Null when there was none.</param>
/// <param name="CoveredToUtc">The newest.</param>
/// <param name="Source">Where the counts came from, a sentence the reply carries.</param>
/// <param name="Modules">One row per module that ran.</param>
/// <param name="DaysWithData">How many days of the window hold Query Store data for THIS database, or null where the read cannot say. The
/// covered range is the oldest and newest such day, so a hole between them shows here and not there, and the classification goes by this
/// count where it is known.</param>
/// <param name="QueryCaptureMode">The database's Query Store capture mode (<c>query_capture_mode_desc</c> verbatim: <c>ALL</c>, <c>AUTO</c>,
/// <c>CUSTOM</c> or <c>NONE</c>) from the stored health snapshots in the window: the newest one that is not <c>ALL</c> where there is one,
/// otherwise the newest. Null where none was stored.</param>
public sealed record ObjectExecutionWindow(
    DateTime? CoveredFromUtc,
    DateTime? CoveredToUtc,
    string Source,
    IReadOnlyList<ObjectExecutionTotals> Modules,
    int? DaysWithData = null,
    string? QueryCaptureMode = null);

/// <summary>One procedure, function or view in the stored object list (Darling's <c>object_inventory</c>, #8784).</summary>
/// <param name="SchemaName">The schema.</param>
/// <param name="ObjectName">The name.</param>
/// <param name="TypeDesc">The <c>sys.objects.type_desc</c>, e.g. <c>SQL_STORED_PROCEDURE</c>.</param>
/// <param name="CreateDate">When it was created (server-local, as stored).</param>
/// <param name="ModifyDate">When it was last altered (server-local, as stored).</param>
/// <param name="IsNativelyCompiled">A natively compiled module, which Query Store does not record unless its statistics collection has been
/// turned on for it.</param>
public sealed record ObjectInventoryEntry(
    string SchemaName, string ObjectName, string TypeDesc, DateTime? CreateDate, DateTime? ModifyDate, bool IsNativelyCompiled = false);

/// <summary>The stored object list of one database, as of its newest capture (#8784).</summary>
/// <param name="CapturedAtUtc">When the list was captured.</param>
/// <param name="Objects">The objects.</param>
public sealed record ObjectInventory(DateTime CapturedAtUtc, IReadOnlyList<ObjectInventoryEntry> Objects);

/// <summary>One registration's evidence: its table and index window, its module executions, and (Darling) its object list.</summary>
/// <param name="ServerDisplayName">The registration's display name.</param>
/// <param name="Tables">The index and object stats window.</param>
/// <param name="ToUtc">Converts a server-local stamp from <paramref name="Tables"/> or <paramref name="Inventory"/> to UTC.</param>
/// <param name="Executions">The module executions, or null when this registration has none to read.</param>
/// <param name="Inventory">The object list, or null where none is kept (Lite, or a store with no capture yet).</param>
public sealed record ObjectUsageSide(
    string ServerDisplayName,
    ObjectUsageWindowResult Tables,
    Func<DateTime, DateTime> ToUtc,
    ObjectExecutionWindow? Executions,
    ObjectInventory? Inventory);

/// <summary>
/// The <c>get_object_usage</c> reply (#8784), built the same way on both products from each side's evidence, so Lite and Darling answer in
/// one shape and only their store reads differ.
///
/// <para><b>Classification.</b> A table or indexed view is <c>Not used in window</c> with no reads, no writes and no last-seen stamp at or
/// after the window start; <c>Write-only</c> with writes (or a write stamp in the window) and no reads; <c>Read</c> otherwise. A procedure or
/// function is <c>Executed</c> with at least one execution in Query Store over the window, and <c>Not executed in window</c> when the object
/// list holds it and Query Store never saw it run. Where there is no object list (Lite), a module that never ran is not listed at all, and the
/// reply says so: an absent row is not evidence of no use.</para>
///
/// <para><b>Not tracked.</b> Some objects leave no trace in the evidence whatever their use, and calling them unused would be wrong. A
/// memory-optimized table has no rows in <c>sys.dm_db_index_usage_stats</c>, and a table whose every stored row predates the collection of
/// that flag cannot be told from one, so with no counted use it is not called unused. An inline table-valued function is expanded into the
/// statement that calls it, a CLR module has no T-SQL statements and a natively compiled module is not recorded by default, so Query Store
/// attributes no executions to any of them. These are <c>Not tracked</c>, ordered last, with the reason in <c>caveat</c>. A scalar function
/// that never ran stays <c>Not executed in window</c> but carries a caveat: SQL Server 2019 and later can inline it into its caller, and
/// then it is not attributed executions either. A procedure that never ran carries one too: Query Store records the statements a
/// procedure runs itself, so one whose body only calls other modules shows nothing however often it runs, and the catalog cannot tell
/// which procedures those are.</para>
///
/// <para><b>No executions is only evidence where Query Store was looked at.</b> A module is <c>Not executed in window</c> because Query
/// Store held data for the database and the module was not in it. With no Query Store data for the database in the window (Query Store
/// off, or the summary not built yet) there is nothing to conclude, and every module that did not run is <c>Not tracked</c>. With data for
/// only part of the window the module stays <c>Not executed in window</c> and its caveat says how much of the window was covered. How much
/// is the count of days that hold data where the read knows it, so a database whose Query Store was off for weeks in the middle of the
/// window says so, and the span from the oldest to the newest day otherwise.</para>
///
/// <para><b>Capture mode.</b> Under a capture mode other than <c>ALL</c> (<c>AUTO</c>, the default from SQL Server 2019, <c>CUSTOM</c> or
/// <c>NONE</c>) Query Store leaves out statements it judges infrequent or cheap, or every new one, so a procedure that runs once a quarter can
/// have no rows while it is plainly in use. A module that did not run where any side's stored mode in the window is not <c>ALL</c> is
/// therefore <c>Not tracked</c>, with a caveat that names the mode.</para>
///
/// <para><b>Created inside the window.</b> A module created after the window start could only have run since then, so a <c>Not executed in
/// window</c> one says how many of the days asked for it existed.</para>
///
/// <para><b>More than one side.</b> Darling stores a ReadOnlyIntent connection as its own registration (<c>X:RO</c>). Index usage counters are
/// per instance, so a table read only by read-only reporting looks unused from the primary: each table's counts are summed across the sides
/// and its stamps are the newest of either. Query Store is different: a database has ONE Query Store, which lives on the primary (a readable
/// secondary shows the primary's, and with Query Store for secondary replicas the secondaries' workload is written to it), so two
/// registrations read the same executions. A module's executions are therefore the LARGER of the sides' reads, never their sum.
/// <c>by_server</c> shows each side's own numbers. Every side's object list is added before any side's executions, so a module is matched to
/// its typed row whichever side holds the list.</para>
///
/// <para><b>Names are matched exactly.</b> A case-sensitive database can hold <c>dbo.Orders</c> and <c>dbo.ORDERS</c> as two objects, so
/// rows are keyed by ordinal comparison. Every name here comes from the catalog (<c>sys.objects</c> for tables and the object list, Query
/// Store's own object id for modules), so the same object is spelled the same way in every source.</para>
///
/// <para>Rows are ordered decommission candidates first (not used, then write-only, then the rest, not tracked last), then by type, schema
/// and name, and held to the shared response budget.</para>
/// </summary>
public static class ObjectUsageReport
{
    /// <summary>The classification of a table or indexed view with no evidence of use in the window.</summary>
    public const string NotUsed = "Not used in window";

    /// <summary>The classification of a table or indexed view written but not read in the window.</summary>
    public const string WriteOnly = "Write-only";

    /// <summary>The classification of a table or indexed view read in the window.</summary>
    public const string Read = "Read";

    /// <summary>The classification of a module that ran in the window.</summary>
    public const string Executed = "Executed";

    /// <summary>The classification of a module in the object list that never ran in the window.</summary>
    public const string NotExecuted = "Not executed in window";

    /// <summary>The classification of an object whose use the evidence cannot show: see <c>caveat</c> on the row.</summary>
    public const string NotTracked = "Not tracked";

    /// <summary>The module types of the object list that the reply lists (views are left to the view usage read).</summary>
    private static readonly HashSet<string> ModuleTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "SQL_STORED_PROCEDURE", "CLR_STORED_PROCEDURE", "SQL_SCALAR_FUNCTION", "SQL_INLINE_TABLE_VALUED_FUNCTION",
        "SQL_TABLE_VALUED_FUNCTION", "CLR_SCALAR_FUNCTION", "CLR_TABLE_VALUED_FUNCTION",
    };

    /// <summary>The module types Query Store attributes no executions to, with why. One of these that never "ran" is not evidence.</summary>
    private static readonly Dictionary<string, string> UntrackedModuleTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        ["SQL_INLINE_TABLE_VALUED_FUNCTION"] = InlineFunctionCaveat,
        ["CLR_STORED_PROCEDURE"] = ClrCaveat,
        ["CLR_SCALAR_FUNCTION"] = ClrCaveat,
        ["CLR_TABLE_VALUED_FUNCTION"] = ClrCaveat,
    };

    internal const string InlineFunctionCaveat =
        "An inline table-valued function is expanded into the statement that calls it, so Query Store attributes no executions to it. "
        + "No executions here is not evidence that it is unused: check what references it.";

    internal const string ClrCaveat =
        "A CLR module has no T-SQL statements of its own, so Query Store attributes no executions to it. No executions here is not "
        + "evidence that it is unused: check what references it.";

    internal const string ScalarFunctionCaveat =
        "A scalar function that SQL Server 2019 or later inlines into its caller is not attributed executions by Query Store, so check what "
        + "references it before treating it as unused.";

    internal const string MemoryOptimizedCaveat =
        "A memory-optimized table has no rows in sys.dm_db_index_usage_stats, so its reads and writes are always zero here whatever its "
        + "use. Check sys.dm_db_xtp_index_stats on the server.";

    internal const string MemoryOptimizedUnknownCaveat =
        "Every stored snapshot of this table is from before memory-optimized tables were marked, so it cannot be told from one, and a "
        + "memory-optimized table reads zero here whatever its use. The next daily collection settles it.";

    internal const string NativelyCompiledCaveat =
        "A natively compiled module is not recorded by Query Store unless its statistics collection has been turned on "
        + "(sys.sp_xtp_control_query_exec_stats). No executions here is not evidence that it is unused: check what references it.";

    internal const string ProcedureCaveat =
        "Query Store records only the statements a procedure runs itself: one whose body only calls other modules shows no executions "
        + "however often it runs. Check what references it before treating it as unused.";

    internal const string NoExecutionEvidenceCaveat =
        "Query Store held no data for this database in the window, so there is no evidence either way about whether this module ran.";

    /// <summary>The caveat of a module that did not run where Query Store captures only some statements.</summary>
    internal static string CaptureModeCaveat(string mode) =>
        $"Query Store's capture mode for this database is {mode}, not ALL, so it leaves out statements it judges infrequent or cheap (or, "
        + "under NONE, every new one): a module that runs rarely can have no executions here while it is in use. No executions here is not "
        + "evidence that it is unused.";

    /// <summary>The caveat of a module that did not run and was created after the window start.</summary>
    internal static string CreatedInWindowCaveat(DateTime createdUtc, double existedDays, int daysBack) =>
        string.Create(CultureInfo.InvariantCulture,
            $"This module was created on {createdUtc:yyyy-MM-dd}, so it existed for only {existedDays:0.#} of the {daysBack} days asked for and may not have been due to run yet.");

    /// <summary>The caveat of a module that did not run where Query Store covered only part of the window.</summary>
    internal static string PartialExecutionEvidenceCaveat(double coveredDays, int daysBack) =>
        string.Create(CultureInfo.InvariantCulture,
            $"Query Store data for this database covers only {coveredDays:0.#} of the {daysBack} days asked for, so this module may have run in the days not covered.");

    /// <summary>The default window: a quarter, which catches quarterly workloads.</summary>
    public const int DefaultDaysBack = 90;

    /// <summary>The default page of object rows.</summary>
    public const int DefaultLimit = 100;

    /// <summary>How many days short of the window the Query Store data may be before a module that did not run says so. Darling's summary
    /// is built after a day ends, so the newest day or two are never in it.</summary>
    internal const int ExecutionCoverageSlackDays = 3;


    /// <summary>Builds the reply.</summary>
    /// <param name="databaseName">The database asked about.</param>
    /// <param name="daysBack">The window asked for, in days.</param>
    /// <param name="windowStartUtc">The window start.</param>
    /// <param name="nowUtc">Now, for the covered-days figures.</param>
    /// <param name="sides">The evidence, the asked-for registration first.</param>
    /// <param name="retentionNote">A sentence on how far back this product can reach, or null.</param>
    /// <param name="limit">The most object rows to return.</param>
    /// <param name="offset">How many object rows to skip.</param>
    public static string Build(
        string databaseName,
        int daysBack,
        DateTime windowStartUtc,
        DateTime nowUtc,
        IReadOnlyList<ObjectUsageSide> sides,
        string? retentionNote,
        int limit,
        int offset)
    {
        ArgumentNullException.ThrowIfNull(sides);
        if (sides.Count == 0)
        {
            throw new ArgumentException("At least one side is required.", nameof(sides));
        }

        /* Ordinal: a case-sensitive database can hold two objects whose names differ only by case. */
        var rows = new Dictionary<string, Row>(StringComparer.Ordinal);
        foreach (var side in sides)
        {
            AddTables(rows, side);
            AddInventory(rows, side);
        }

        /* After every side's object list, so a module is matched to its typed row whichever side holds the list. */
        var modules = new Dictionary<string, Row>(StringComparer.Ordinal);
        foreach (var existing in rows.Values.Where(r => !r.IsTable))
        {
            modules.TryAdd(existing.SchemaName + "|" + existing.ObjectName, existing);
        }

        foreach (var side in sides)
        {
            AddExecutions(rows, modules, side);
        }

        /* The registrations read one Query Store, so the best-covered side is how much of the window was looked at. Any side seeing a mode
           other than ALL is enough to stop calling a module that did not run a candidate. */
        var executionDays = sides.Max(ExecutionDays);
        var partialCaptureMode = sides
            .Select(s => s.Executions?.QueryCaptureMode)
            .FirstOrDefault(m => m is not null && IsPartialCaptureMode(m));
        foreach (var row in rows.Values)
        {
            (row.Classification, row.Caveat) = Classify(row, windowStartUtc, nowUtc, executionDays, daysBack, partialCaptureMode);
        }

        var ordered = rows.Values
            .OrderBy(r => Rank(r.Classification))
            .ThenBy(r => r.ObjectType, StringComparer.Ordinal)
            .ThenBy(r => r.SchemaName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.ObjectName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(r => r.SchemaName, StringComparer.Ordinal)
            .ThenBy(r => r.ObjectName, StringComparer.Ordinal)
            .ToList();

        var paired = sides.Count > 1;
        var page = ordered.Skip(offset).Take(limit).Select(r => ToJson(r, paired)).ToArray();
        var totalObjects = ordered.Count;
        var hasInventory = sides.Any(s => s.Inventory is not null);

        var payload = new JsonObject
        {
            ["server"] = sides[0].ServerDisplayName,
            ["paired_servers"] = paired ? new JsonArray(sides.Select(s => (JsonNode?)JsonValue.Create(s.ServerDisplayName)).ToArray()) : null,
            ["database_name"] = databaseName,
            ["days_back"] = daysBack,
            ["window_start_utc"] = Iso(windowStartUtc),
            ["coverage"] = new JsonArray(sides.Select(s => (JsonNode?)Coverage(s, daysBack, nowUtc)).ToArray()),
            ["summary"] = new JsonObject
            {
                ["objects"] = ordered.Count,
                ["not_used"] = ordered.Count(r => r.Classification == NotUsed),
                ["write_only"] = ordered.Count(r => r.Classification == WriteOnly),
                ["read"] = ordered.Count(r => r.Classification == Read),
                ["executed"] = ordered.Count(r => r.Classification == Executed),
                ["not_executed"] = ordered.Count(r => r.Classification == NotExecuted),
                ["not_tracked"] = ordered.Count(r => r.Classification == NotTracked),
            },
            ["note"] = Note(sides, hasInventory, retentionNote),
            ["offset"] = offset,
        };

        /* Held to the shared response budget, whole rows only. The budget can return fewer rows than the limit, so next_offset is computed
           from the rows that actually fit: paging on from it never skips a row the budget left off. The row nodes are moved in and out of
           one array rather than cloned for each measurement, and the last rendering is kept, so a page that fits is serialised once. */
        var objects = new JsonArray();
        var renderedCount = -1;
        var rendered = "";
        string Render(int n)
        {
            if (n == renderedCount)
            {
                return rendered;
            }

            payload["returned"] = n;
            payload["next_offset"] = offset + n < totalObjects ? offset + n : null;
            payload["budget_truncated"] = n < page.Length;
            payload["budget_note"] = n < page.Length
                ? $"{n:N0} of the {page.Length:N0} rows asked for fit the response budget; ask again from next_offset for the rest."
                : null;
            if (objects.Parent is null)
            {
                payload["objects"] = objects;
            }

            objects.Clear();
            for (var i = 0; i < n; i++)
            {
                objects.Add(page[i]);
            }

            renderedCount = n;
            rendered = payload.ToJsonString(McpHelpers.JsonOptions);
            return rendered;
        }

        var fit = McpResponseBudget.RowsThatFit(page.Length, n => System.Text.Encoding.UTF8.GetByteCount(Render(n)));
        return Render(fit);
    }

    private static void AddTables(Dictionary<string, Row> rows, ObjectUsageSide side)
    {
        var coveredTo = side.Tables.CoveredTo;
        foreach (var t in side.Tables.Objects)
        {
            var type = t.IsIndexedView ? "INDEXED_VIEW" : "USER_TABLE";
            var row = Get(rows, type, t.SchemaName, t.ObjectName);
            var lastRead = t.LastRead is { } r ? side.ToUtc(r) : (DateTime?)null;
            var lastWrite = t.LastWrite is { } w ? side.ToUtc(w) : (DateTime?)null;
            row.Reads += t.Reads;
            row.Writes += t.Writes;

            /* Yes on any side is yes; otherwise no on any side is no; with no side saying, it stays not known. */
            row.IsMemoryOptimized = t.IsMemoryOptimized == true || row.IsMemoryOptimized == true ? true : t.IsMemoryOptimized ?? row.IsMemoryOptimized;
            row.LastReadUtc = Newest(row.LastReadUtc, lastRead);
            row.LastWriteUtc = Newest(row.LastWriteUtc, lastWrite);
            var dropped = coveredTo is { } to && t.LastSnapshot < to;
            row.Dropped = row.Dropped is null ? dropped : row.Dropped.Value && dropped;
            row.Sides.Add(new JsonObject
            {
                ["server"] = side.ServerDisplayName,
                ["reads"] = t.Reads,
                ["writes"] = t.Writes,
                ["last_read_utc"] = Iso(lastRead),
                ["last_write_utc"] = Iso(lastWrite),
            });
        }
    }

    private static void AddInventory(Dictionary<string, Row> rows, ObjectUsageSide side)
    {
        if (side.Inventory is not { } inventory)
        {
            return;
        }

        foreach (var o in inventory.Objects.Where(o => ModuleTypes.Contains(o.TypeDesc)))
        {
            var row = Get(rows, o.TypeDesc.ToUpperInvariant(), o.SchemaName, o.ObjectName);
            row.IsNativelyCompiled |= o.IsNativelyCompiled;
            row.CreatedUtc ??= o.CreateDate is { } c ? side.ToUtc(c) : null;
            row.ModifiedUtc ??= o.ModifyDate is { } m ? side.ToUtc(m) : null;
        }
    }

    /// <summary>Adds one side's executions. Query Store names a module without its type, so it is matched to the object list's row by schema
    /// and name, and gets a row of type <c>MODULE</c> where no side's list holds it. The sides read one Query Store, so a module's
    /// executions are the larger of their reads, not the sum.</summary>
    private static void AddExecutions(Dictionary<string, Row> rows, Dictionary<string, Row> modules, ObjectUsageSide side)
    {
        if (side.Executions is not { } executions)
        {
            return;
        }

        foreach (var m in executions.Modules)
        {
            var (schema, name) = SplitModuleName(m.ModuleName);
            if (!modules.TryGetValue(schema + "|" + name, out var row))
            {
                row = Get(rows, "MODULE", schema, name);
                modules.Add(schema + "|" + name, row);
            }

            row.Executions = Math.Max(row.Executions ?? 0, m.Executions);
            row.LastExecutionUtc = Newest(row.LastExecutionUtc, m.LastExecutionUtc);
            row.Sides.Add(new JsonObject
            {
                ["server"] = side.ServerDisplayName,
                ["executions"] = m.Executions,
                ["last_execution_utc"] = Iso(m.LastExecutionUtc),
            });
        }
    }

    /// <summary>Splits Query Store's <c>schema.name</c> at the first dot. A schema name with a dot in it is split wrongly, and such a module
    /// then shows as its own row beside the object list's entry rather than being merged into it.</summary>
    internal static (string Schema, string Name) SplitModuleName(string moduleName)
    {
        var dot = moduleName.IndexOf('.', StringComparison.Ordinal);
        return dot <= 0 ? ("", moduleName) : (moduleName[..dot], moduleName[(dot + 1)..]);
    }

    private static Row Get(Dictionary<string, Row> rows, string type, string schema, string name)
    {
        var key = type + "|" + schema + "|" + name;
        if (!rows.TryGetValue(key, out var row))
        {
            row = new Row(type, schema, name);
            rows.Add(key, row);
        }

        return row;
    }

    /// <summary>How many days of the window one side's Query Store data covers: zero with no executions read or no data in the window. The
    /// count of days that hold data where the read knows it (never more than the span), so a hole in the middle counts against coverage, and
    /// the span from the oldest to the newest day otherwise.</summary>
    internal static double ExecutionDays(ObjectExecutionWindow? executions)
    {
        if (executions is not { CoveredFromUtc: { } from, CoveredToUtc: { } to } || to <= from)
        {
            return 0;
        }

        var span = (to - from).TotalDays;
        return executions.DaysWithData is { } days ? Math.Min(days, span) : span;
    }

    private static double ExecutionDays(ObjectUsageSide side) => ExecutionDays(side.Executions);

    /// <summary>Whether a stored capture mode leaves statements out: anything but <c>ALL</c>.</summary>
    internal static bool IsPartialCaptureMode(string mode) => !string.Equals(mode.Trim(), "ALL", StringComparison.OrdinalIgnoreCase);

    private static (string Classification, string? Caveat) Classify(
        Row row, DateTime windowStartUtc, DateTime nowUtc, double executionDays, int daysBack, string? partialCaptureMode)
    {
        if (row.IsTable)
        {
            /* A memory-optimized table's counters are always zero, so nothing below would be evidence. */
            if (row.IsMemoryOptimized == true)
            {
                return (NotTracked, MemoryOptimizedCaveat);
            }

            var readInWindow = row.Reads > 0 || row.LastReadUtc >= windowStartUtc;
            var writtenInWindow = row.Writes > 0 || row.LastWriteUtc >= windowStartUtc;

            /* No side says whether the table is memory-optimized (every row predates the flag). Counted use settles it, since such a
               table has none; with no use, "not used" and "cannot be seen" look the same, so it is not called a candidate. */
            if (row.IsMemoryOptimized is null && !readInWindow && !writtenInWindow)
            {
                return (NotTracked, MemoryOptimizedUnknownCaveat);
            }

            return (readInWindow ? Read : writtenInWindow ? WriteOnly : NotUsed, null);
        }

        if (row.Executions is > 0)
        {
            return (Executed, null);
        }

        if (UntrackedModuleTypes.TryGetValue(row.ObjectType, out var why))
        {
            return (NotTracked, why);
        }

        if (row.IsNativelyCompiled)
        {
            return (NotTracked, NativelyCompiledCaveat);
        }

        /* Nothing was looked at, so nothing can be said: not a candidate. */
        if (executionDays <= 0)
        {
            return (NotTracked, NoExecutionEvidenceCaveat);
        }

        /* Query Store leaves out what it judges rare or cheap, which is exactly the module this read is asked about. */
        if (partialCaptureMode is not null)
        {
            return (NotTracked, CaptureModeCaveat(partialCaptureMode.Trim().ToUpperInvariant()));
        }

        var caveats = new List<string>(3);
        if (executionDays < daysBack - ExecutionCoverageSlackDays)
        {
            caveats.Add(PartialExecutionEvidenceCaveat(Math.Round(executionDays, 1), daysBack));
        }

        if (row.CreatedUtc is { } created && created > windowStartUtc)
        {
            caveats.Add(CreatedInWindowCaveat(created, Math.Round(Math.Max(0, (nowUtc - created).TotalDays), 1), daysBack));
        }

        if (string.Equals(row.ObjectType, "SQL_SCALAR_FUNCTION", StringComparison.OrdinalIgnoreCase))
        {
            caveats.Add(ScalarFunctionCaveat);
        }
        else if (string.Equals(row.ObjectType, "SQL_STORED_PROCEDURE", StringComparison.OrdinalIgnoreCase))
        {
            caveats.Add(ProcedureCaveat);
        }

        return (NotExecuted, caveats.Count > 0 ? string.Join(" ", caveats) : null);
    }

    private static int Rank(string classification) => classification switch
    {
        NotUsed or NotExecuted => 0,
        WriteOnly => 1,
        NotTracked => 3,
        _ => 2,
    };

    private static JsonObject ToJson(Row r, bool paired)
    {
        var isTable = r.IsTable;
        return new JsonObject
        {
            ["schema_name"] = r.SchemaName,
            ["object_name"] = r.ObjectName,
            ["object_type"] = r.ObjectType,
            ["classification"] = r.Classification,
            ["caveat"] = r.Caveat,
            ["reads"] = isTable ? r.Reads : null,
            ["writes"] = isTable ? r.Writes : null,
            ["last_read_utc"] = isTable ? Iso(r.LastReadUtc) : null,
            ["last_write_utc"] = isTable ? Iso(r.LastWriteUtc) : null,
            ["executions"] = isTable ? null : r.Executions ?? 0,
            ["last_execution_utc"] = isTable ? null : Iso(r.LastExecutionUtc),
            ["created_utc"] = Iso(r.CreatedUtc),
            ["modified_utc"] = Iso(r.ModifiedUtc),
            ["dropped"] = isTable && r.Dropped == true ? true : null,
            /* The side nodes are built for this reply and have no parent yet, so they are moved in, not copied. */
            ["by_server"] = paired && r.Sides.Count > 0 ? new JsonArray(r.Sides.ToArray<JsonNode?>()) : null,
        };
    }

    private static JsonObject Coverage(ObjectUsageSide side, int daysBack, DateTime nowUtc)
    {
        var t = side.Tables;
        /* The time before each clear of the counters is taken off: use in it was wiped before any snapshot read it, so it is not covered. */
        var tablesDays = t.CoveredFrom is { } from && t.CoveredTo is { } to ? Math.Round(Math.Max(0, (to - from - t.BlindTime).TotalDays), 1) : 0;
        var executionDays = side.Executions is { CoveredFromUtc: { } ef, CoveredToUtc: { } et } ? Math.Round((et - ef).TotalDays, 1) : 0;
        var evidenceDays = Math.Round(ExecutionDays(side.Executions), 1);
        return new JsonObject
        {
            ["server"] = side.ServerDisplayName,
            ["tables"] = new JsonObject
            {
                ["covered_from_utc"] = Iso(t.CoveredFrom),
                ["covered_to_utc"] = Iso(t.CoveredTo),
                ["covered_days"] = tablesDays,
                ["short_of_window"] = tablesDays < daysBack - 1,
                ["snapshots"] = t.SnapshotCount,
                ["largest_gap_hours"] = t.LargestGap is { } gap ? Math.Round(gap.TotalHours, 1) : null,
                ["resets_crossed"] = t.Resets.Count,
                ["blind_hours"] = Math.Round(t.BlindTime.TotalHours, 1),
                ["index_resets"] = t.IndexResets,
                ["resets"] = new JsonArray(t.Resets.Take(20).Select(r => (JsonNode?)new JsonObject
                {
                    ["collection_time_utc"] = Iso(r.CollectionTime),
                    ["reasons"] = r.Reasons.ToString(),
                    ["blind_from_utc"] = Iso(r.BlindFrom),
                    ["blind_hours"] = r.BlindFrom is null ? null : Math.Round(r.BlindTime.TotalHours, 1),
                    ["from_server"] = r.FromServerName,
                    ["to_server"] = r.ToServerName,
                }).ToArray()),
                ["answering_servers"] = new JsonArray(t.AnsweringServerNames.Select(n => (JsonNode?)JsonValue.Create(n)).ToArray()),
            },
            ["executions"] = side.Executions is { } e
                ? new JsonObject
                {
                    ["covered_from_utc"] = Iso(e.CoveredFromUtc),
                    ["covered_to_utc"] = Iso(e.CoveredToUtc),
                    ["covered_days"] = executionDays,
                    ["days_with_data"] = e.DaysWithData,
                    ["short_of_window"] = evidenceDays < daysBack - 1,
                    ["query_capture_mode"] = e.QueryCaptureMode,
                    ["source"] = e.Source,
                }
                : null,
            ["object_list_captured_at_utc"] = side.Inventory is { } inv ? Iso(inv.CapturedAtUtc) : null,
            ["data_age_hours"] = t.CoveredTo is { } newest ? Math.Round((nowUtc - newest).TotalHours, 1) : null,
        };
    }

    private static string Note(IReadOnlyList<ObjectUsageSide> sides, bool hasInventory, string? retentionNote)
    {
        var parts = new List<string>
        {
            "Tables and indexed views: reads and writes are summed from the daily index and object stats snapshots, per answering instance. "
            + "Every counter reset inside the window (restart, database offline and online, restore, change of role) is detected, use "
            + "after it is counted whole and last seen is carried forward across resets. Use between the last snapshot before a reset and "
            + "the reset itself is LOST: the counters that held it were cleared before they were read. coverage.tables.blind_hours is how "
            + "much time that is at most (each entry of resets has its own), and covered_days already leaves it out; a 'Not used in window' "
            + "table may have been used in those hours. A row is a candidate only when it is 'Not used in window' on EVERY replica that "
            + "serves it.",
            "Procedures and functions: executions come from Query Store, so they only cover what Query Store captured (its capture mode, its "
            + "own retention and the databases where it is on). coverage.executions is for THIS database: with no covered days there, a "
            + "module that did not run is 'Not tracked', not a candidate, and with only part of the window covered (counting the days that "
            + "hold data, so a gap in the middle counts) its caveat says how much. Where the capture mode is not ALL, Query Store leaves "
            + "out statements it judges infrequent or cheap, so a module that did not run is 'Not tracked' too. A module created inside "
            + "the window says how many of the days asked for it existed. A procedure whose body only calls other modules has no statements of its own for Query Store to record, so it shows "
            + "no executions however often it runs: check what references a procedure before dropping it.",
            hasInventory
                ? "Modules in the stored object list that Query Store never saw run are listed as 'Not executed in window'."
                : "No object list is kept here, so a module that never ran in the window is NOT listed: its absence is not evidence that it is unused.",
            "'Not tracked' rows (memory-optimized tables, inline table-valued functions, CLR and natively compiled modules, and anything "
            + "the evidence does not cover) leave no trace here whatever their use: they are NOT candidates, and caveat on the row says why.",
            "Views that are not indexed are not covered: their reads land on the base tables.",
        };

        if (sides.Count > 1)
        {
            parts.Add("A database has one Query Store, on the primary, so the registrations read the same executions: a module's executions "
                + "are the larger of their reads, not the sum. Table reads and writes are per instance and are summed.");

            var shared = sides
                .SelectMany(s => s.Tables.AnsweringServerNames.Distinct(StringComparer.OrdinalIgnoreCase))
                .GroupBy(n => n, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();
            if (shared.Count > 0)
            {
                parts.Add($"WARNING: {string.Join(", ", shared)} answered more than one of these registrations (a ReadOnlyIntent connection "
                    + "with no read-only routing lands on the primary), so table reads and writes summed across them can count that instance "
                    + "twice. Classification is not affected; read the counts from by_server.");
            }
        }

        if (sides.Count == 1)
        {
            parts.Add("Only this registration was read. Usage is per replica: an object used only on a readable secondary that is not monitored here looks unused.");
        }

        if (retentionNote is not null)
        {
            parts.Add(retentionNote);
        }

        return string.Join(" ", parts);
    }

    private static string? Iso(DateTime? value) => value?.ToString("o", CultureInfo.InvariantCulture);

    private static DateTime? Newest(DateTime? a, DateTime? b) => ObjectUsageWindow.Newest(a, b);

    private sealed class Row(string objectType, string schemaName, string objectName)
    {
        public string ObjectType { get; } = objectType;
        public string SchemaName { get; } = schemaName;
        public string ObjectName { get; } = objectName;
        public bool IsTable { get; } = objectType is "USER_TABLE" or "INDEXED_VIEW";
        public string Classification { get; set; } = "";
        public string? Caveat { get; set; }
        public bool? IsMemoryOptimized { get; set; }
        public bool IsNativelyCompiled { get; set; }
        public long Reads { get; set; }
        public long Writes { get; set; }
        public long? Executions { get; set; }
        public DateTime? LastReadUtc { get; set; }
        public DateTime? LastWriteUtc { get; set; }
        public DateTime? LastExecutionUtc { get; set; }
        public DateTime? CreatedUtc { get; set; }
        public DateTime? ModifiedUtc { get; set; }
        public bool? Dropped { get; set; }
        public List<JsonObject> Sides { get; } = new();
    }
}
