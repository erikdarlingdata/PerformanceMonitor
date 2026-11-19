using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>One index (or heap) row of the newest daily index snapshot, as the page latch answer needs it (#9055).
/// A heap has a null <see cref="IndexName"/>.</summary>
public sealed record PageLatchIndexStat(
    int DatabaseId, string DatabaseName, string? SchemaName, string? TableName, string? IndexName,
    long PageLatchWaitMs, long PageLatchWaitCount, long PageIoLatchWaitMs, long PageIoLatchWaitCount);

/// <summary>One object the statement's plan names. <see cref="IndexName"/> is null for a heap.</summary>
public sealed record PlanIndexReference(string DatabaseName, string? SchemaName, string TableName, string? IndexName);

/// <summary>One index named in a page latch answer, with the wait totals of the kind the row waited on. A null total means the
/// index snapshot has no row for the index (a Microsoft-shipped object, an excluded database, or an index created after the snapshot),
/// which is not the same as no waits.</summary>
public sealed record PageLatchIndex(string? SchemaName, string? TableName, string? IndexName, long? WaitMs, long? WaitCount);

/// <summary>What a page latch wait is on (#9055): the page kind, and for a data or index page the indexes that
/// may own it. <see cref="Match"/> is <c>exact</c> (the plan names one index in that database), <c>plan</c> (it names several) or
/// <c>inferred</c> (no plan names one, so the snapshot's top waiters are listed) when <see cref="Indexes"/> is set.</summary>
public sealed record PageLatchAnswer(
    int DatabaseId, int FileId, long PageId, string PageKind, bool IsIoLatch,
    string? Match, IReadOnlyList<PageLatchIndex>? Indexes, string? Note, DateTime? IndexSnapshotTime)
{
    /// <summary>The <c>page_latch</c> object both apps' get_active_queries write, so the two outputs cannot drift.</summary>
    public JsonObject ToJson()
    {
        var node = new JsonObject
        {
            ["database_id"] = DatabaseId,
            ["file_id"] = FileId,
            ["page_id"] = PageId,
            ["page_kind"] = PageKind,
        };
        if (Match != null) node["match"] = Match;
        if (Indexes != null)
        {
            node["latch_kind"] = IsIoLatch ? "page_io_latch" : "page_latch";
            var list = new JsonArray();
            foreach (var index in Indexes)
            {
                list.Add(new JsonObject
                {
                    ["schema_name"] = index.SchemaName,
                    ["table_name"] = index.TableName,
                    ["index_name"] = index.IndexName,
                    ["latch_wait_ms"] = index.WaitMs,
                    ["latch_wait_count"] = index.WaitCount,
                });
            }

            node["indexes"] = list;
        }

        if (Note != null) node["note"] = Note;
        if (IndexSnapshotTime is { } at) node["index_snapshot_time"] = DateTime.SpecifyKind(at, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
        return node;
    }
}

/// <summary>
/// Names what a PAGELATCH_ or PAGEIOLATCH_ wait is on, from data the monitor already stores (#9055): the request's own
/// <c>wait_resource</c>, its plan, and the daily per-index snapshot. Nothing is looked up on the monitored server. Both
/// apps' get_active_queries call <see cref="Describe"/>.
/// </summary>
public static class PageLatchResource
{
    public const string Pfs = "PFS";
    public const string Gam = "GAM";
    public const string Sgam = "SGAM";
    public const string Dcm = "DCM";
    public const string Bcm = "BCM";
    public const string BootPage = "boot page";
    public const string DataOrIndexPage = "data or index page";
    public const string FileHeader = "file header";

    public const string MatchExact = "exact";
    public const string MatchPlan = "plan";
    public const string MatchInferred = "inferred";

    public const string TempdbNote = "tempdb data page: temp tables are not in the index snapshot, so no indexes are listed.";

    /// <summary>How many indexes the fallback lists.</summary>
    public const int InferredIndexCount = 3;

    /// <summary>PFS repeats every 8,088 pages from page 1; GAM and SGAM start at pages 2 and 3 and repeat every GAM interval
    /// (63,904 extents, 511,232 pages). Page ids 1, 8088 and 16176 are PFS pages in the Page and Extent Architecture Guide. DCM
    /// (differential changed map) and BCM (bulk changed map, also called the ML map) are the other two per-interval bitmaps: pages 6
    /// and 7 of the file, then 6 and 7 again in every later GAM interval (511,238 and 511,239, ...). The boot page is page 9 of file 1.</summary>
    private const long PfsInterval = 8088;

    private const long GamInterval = 511232;

    /// <summary>True for a PAGELATCH_ or PAGEIOLATCH_ wait type.</summary>
    public static bool IsPageLatchWait(string? waitType) =>
        waitType != null
        && (waitType.StartsWith("PAGELATCH_", StringComparison.OrdinalIgnoreCase)
            || waitType.StartsWith("PAGEIOLATCH_", StringComparison.OrdinalIgnoreCase));

    public static bool IsIoLatchWait(string? waitType) =>
        waitType != null && waitType.StartsWith("PAGEIOLATCH_", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// For a PAGELATCH_ or PAGEIOLATCH_ wait, <c>sys.dm_exec_requests.wait_resource</c> is the page, written
    /// <c>database_id:file_id:page_id</c> (Microsoft's sys.dm_exec_requests documentation describes the column as the resource the request
    /// waits on and names <c>page_resource</c> as its 8-byte form when it holds a page). Exactly three non-negative integers; anything
    /// else, including a lock resource such as <c>KEY: 5:72057594038779904 (...)</c>, is not a page.
    /// </summary>
    public static bool TryParse(string? waitResource, out int databaseId, out int fileId, out long pageId)
    {
        databaseId = 0;
        fileId = 0;
        pageId = 0;
        if (string.IsNullOrWhiteSpace(waitResource)) return false;
        var parts = waitResource.Trim().Split(':');
        return parts.Length == 3
            && int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out databaseId)
            && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out fileId)
            && long.TryParse(parts[2], NumberStyles.None, CultureInfo.InvariantCulture, out pageId);
    }

    /// <summary>
    /// Names the fixed allocation and header pages by their page number: the file header (page 0), PFS, GAM, SGAM, DCM and BCM, and the
    /// boot page (page 9 of file 1). Every other page comes back as <see cref="DataOrIndexPage"/>, and that is a catch-all, not a proof:
    /// an IAM page or a system table page can sit anywhere in a file, so page arithmetic cannot rule them out.
    /// </summary>
    public static string PageKind(int fileId, long pageId)
    {
        if (pageId == 0) return FileHeader;
        if (pageId == 1 || pageId % PfsInterval == 0) return Pfs;
        if (pageId == 2 || pageId % GamInterval == 0) return Gam;
        if (pageId == 3 || (pageId - 1) % GamInterval == 0) return Sgam;
        if (pageId >= 6 && (pageId - 6) % GamInterval == 0) return Dcm;
        if (pageId >= 7 && (pageId - 7) % GamInterval == 0) return Bcm;
        if (fileId == 1 && pageId == 9) return BootPage;
        return DataOrIndexPage;
    }

    /// <summary>
    /// The tables and indexes a plan names, read with <see cref="ShowPlanParser"/>: each operator's object, plus the nonclustered
    /// indexes a modification operator maintains. A heap is named with a null index. Empty for no plan or a plan that cannot be parsed.
    /// </summary>
    public static IReadOnlyList<PlanIndexReference> IndexesInPlan(string? planXml, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(planXml)) return [];
        ParsedPlan plan;
        try
        {
            plan = ShowPlanParser.Parse(planXml, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return [];
        }

        if (plan.ParseError != null) return [];
        var found = new List<PlanIndexReference>();
        foreach (var statement in plan.Batches.SelectMany(b => b.Statements))
        {
            if (statement.RootNode != null) Collect(statement.RootNode, found);
        }

        return found.Distinct(ReferenceComparer.Instance).ToList();
    }

    private static void Collect(PlanNode root, List<PlanIndexReference> found)
    {
        var pending = new Stack<PlanNode>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            var node = pending.Pop();
            foreach (var child in node.Children) pending.Push(child);
            if (string.IsNullOrEmpty(node.DatabaseName) || string.IsNullOrEmpty(node.ObjectName)) continue;
            var schema = string.IsNullOrEmpty(node.SchemaName) ? null : node.SchemaName;
            var table = schema != null && node.ObjectName.StartsWith(schema + ".", StringComparison.Ordinal)
                ? node.ObjectName[(schema.Length + 1)..]
                : node.ObjectName;
            if (!string.IsNullOrEmpty(node.IndexName))
                found.Add(new PlanIndexReference(node.DatabaseName, schema, table, node.IndexName));
            else if (string.Equals(node.IndexKind, "Heap", StringComparison.OrdinalIgnoreCase))
                found.Add(new PlanIndexReference(node.DatabaseName, schema, table, null));
            foreach (var name in node.NonClusteredIndexNames)
                found.Add(new PlanIndexReference(node.DatabaseName, schema, table, name));
        }
    }

    /// <summary>
    /// The answer for one request row, or null when the wait is not a page latch or the wait resource is not a page.
    /// <paramref name="stats"/> are the newest index snapshot's rows for the page's database (every row with a latch wait, plus at
    /// least one row of the database so its id can be named); <paramref name="indexSnapshotTime"/> is that snapshot's time. Null
    /// stats means the server has no index snapshot.
    /// </summary>
    public static PageLatchAnswer? Describe(
        string? waitType, string? waitResource, string? planXml, IReadOnlyList<PageLatchIndexStat>? stats, DateTime? indexSnapshotTime,
        CancellationToken cancellationToken = default) =>
        Describe(waitType, waitResource, stats, indexSnapshotTime, () => IndexesInPlan(planXml, cancellationToken));

    /// <summary>The same answer with the plan's references already read, so a caller that shares one plan across rows parses it once.</summary>
    public static PageLatchAnswer? Describe(
        string? waitType, string? waitResource, IReadOnlyList<PageLatchIndexStat>? stats, DateTime? indexSnapshotTime,
        Func<IReadOnlyList<PlanIndexReference>> planReferences)
    {
        if (!IsPageLatchWait(waitType) || !TryParse(waitResource, out var databaseId, out var fileId, out var pageId)) return null;
        var io = IsIoLatchWait(waitType);
        var kind = PageKind(fileId, pageId);
        PageLatchAnswer Answer(string? match, IReadOnlyList<PageLatchIndex>? indexes, string? note) =>
            new(databaseId, fileId, pageId, kind, io, match, indexes, note, match != null ? indexSnapshotTime : null);

        if (kind != DataOrIndexPage) return Answer(null, null, null);
        if (databaseId == 2) return Answer(null, null, TempdbNote);

        var rows = stats ?? [];
        var databaseName = rows.FirstOrDefault(s => s.DatabaseId == databaseId)?.DatabaseName;
        if (databaseName == null)
            return Answer(null, null, "No index snapshot rows for this database, so no indexes are listed.");

        long Ms(PageLatchIndexStat s) => io ? s.PageIoLatchWaitMs : s.PageLatchWaitMs;
        long Count(PageLatchIndexStat s) => io ? s.PageIoLatchWaitCount : s.PageLatchWaitCount;

        var named = planReferences()
            .Where(r => string.Equals(r.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (named.Count > 0)
        {
            var listed = named.Select(r =>
            {
                var stat = rows.FirstOrDefault(s =>
                    s.DatabaseId == databaseId
                    && string.Equals(s.SchemaName ?? "", r.SchemaName ?? "", StringComparison.OrdinalIgnoreCase)
                    && string.Equals(s.TableName ?? "", r.TableName, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(s.IndexName ?? "", r.IndexName ?? "", StringComparison.OrdinalIgnoreCase));
                return new PageLatchIndex(r.SchemaName, r.TableName, r.IndexName, stat == null ? null : Ms(stat), stat == null ? null : Count(stat));
            }).OrderByDescending(i => i.WaitMs ?? -1).ThenBy(i => i.SchemaName, StringComparer.OrdinalIgnoreCase)
              .ThenBy(i => i.TableName, StringComparer.OrdinalIgnoreCase).ThenBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase).ToList();
            return Answer(listed.Count == 1 ? MatchExact : MatchPlan, listed, null);
        }

        var inferred = rows
            .Where(s => s.DatabaseId == databaseId && Ms(s) > 0)
            .OrderByDescending(Ms)
            .ThenBy(s => s.SchemaName, StringComparer.OrdinalIgnoreCase).ThenBy(s => s.TableName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.IndexName, StringComparer.OrdinalIgnoreCase)
            .Take(InferredIndexCount)
            .Select(s => new PageLatchIndex(s.SchemaName, s.TableName, s.IndexName, Ms(s), Count(s)))
            .ToList();
        return inferred.Count == 0 ? Answer(null, null, null) : Answer(MatchInferred, inferred, null);
    }

    private sealed class ReferenceComparer : IEqualityComparer<PlanIndexReference>
    {
        public static readonly ReferenceComparer Instance = new();

        public bool Equals(PlanIndexReference? x, PlanIndexReference? y) =>
            x != null && y != null
            && string.Equals(x.DatabaseName, y.DatabaseName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.SchemaName, y.SchemaName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.TableName, y.TableName, StringComparison.OrdinalIgnoreCase)
            && string.Equals(x.IndexName, y.IndexName, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode(PlanIndexReference obj) =>
            HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.DatabaseName),
                obj.SchemaName == null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(obj.SchemaName),
                StringComparer.OrdinalIgnoreCase.GetHashCode(obj.TableName),
                obj.IndexName == null ? 0 : StringComparer.OrdinalIgnoreCase.GetHashCode(obj.IndexName));
    }
}
