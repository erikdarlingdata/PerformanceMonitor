/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Analysis.ObjectReferences;

/// <summary>One referenced name's totals over the statements added, per statement database.</summary>
/// <param name="DatabaseName">The database the statements ran in (Query Store's database).</param>
/// <param name="ReferencedDatabase">The database part as written, or empty where the statement named none or named its own database.</param>
/// <param name="SchemaName">The schema as written, or empty where the statement named none.</param>
/// <param name="ObjectName">The object name as written.</param>
/// <param name="IsFunction">True where it was named as a table-valued function.</param>
/// <param name="Executions">The executions of the statements that name it.</param>
/// <param name="LastExecution">The newest execution of any of them.</param>
/// <param name="Statements">How many statements name it.</param>
/// <param name="TextMatched">True where any of them was found by the text search, not the parse.</param>
public sealed record AggregatedReference(
    string DatabaseName,
    string ReferencedDatabase,
    string SchemaName,
    string ObjectName,
    bool IsFunction,
    long Executions,
    DateTime? LastExecution,
    int Statements,
    bool TextMatched)
{
    /// <summary>The referenced database folded by <see cref="ObjectReferenceAggregator.FoldName"/>: the key the aggregator merged on.</summary>
    public string ReferencedDatabaseKey => ObjectReferenceAggregator.FoldName(ReferencedDatabase);

    /// <summary>The schema folded the same way.</summary>
    public string SchemaKey => ObjectReferenceAggregator.FoldName(SchemaName);

    /// <summary>The name folded the same way.</summary>
    public string ObjectNameKey => ObjectReferenceAggregator.FoldName(ObjectName);
}

/// <summary>What was looked at in one statement database.</summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="Statements">Statements added.</param>
/// <param name="StatementsWithoutText">Of them, the ones with no text.</param>
/// <param name="StatementsUnparsed">Of them, the ones whose text did not parse (searched as text where names to search for were given).</param>
/// <param name="Executions">Their executions.</param>
public sealed record AggregatedDatabase(string DatabaseName, long Statements, long StatementsWithoutText, long StatementsUnparsed, long Executions);

/// <summary>
/// A bounded cache of statement scans keyed by a hash of the text (#8784 part 2): the same statement comes back every day it runs, and a
/// parse costs far more than a hash. Thread-safe.
///
/// <para><b>Two generations</b> (#8784 review). New scans go into the current generation; a hit in the previous one is moved up. When the
/// current one holds <c>capacity</c> scans it becomes the previous one and the old previous one is dropped. So a full cache drops only the
/// scans not used for a whole generation, not every scan: a store with many servers and tens of thousands of distinct texts each keeps
/// the texts that recur from day to day, where clearing it whole would parse each day's text again. It holds at most twice
/// <c>capacity</c>, each key the first 128 bits of the text's SHA-256.</para>
/// </summary>
public sealed class QueryTextScanCache
{
    private Dictionary<UInt128, QueryTextScan> _current = new();
    private Dictionary<UInt128, QueryTextScan> _previous = new();
    private readonly object _gate = new();
    private readonly int _capacity;
    private long _scans;

    /// <summary>Creates a cache whose generations hold at most <paramref name="capacity"/> scans each.</summary>
    public QueryTextScanCache(int capacity = 200_000)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _capacity = capacity;
    }

    /// <summary>How many scans are held, in both generations.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _current.Count + _previous.Count;
            }
        }
    }

    /// <summary>How many texts this cache has parsed since it was created: a hit adds none (#9885).</summary>
    public long Scans => Interlocked.Read(ref _scans);

    /// <summary>The scan of <paramref name="text"/>, parsed on a miss.</summary>
    public QueryTextScan GetOrScan(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        Span<byte> hash = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(text), hash);
        var key = BinaryPrimitives.ReadUInt128LittleEndian(hash);
        lock (_gate)
        {
            if (_current.TryGetValue(key, out var cached))
            {
                return cached;
            }

            if (_previous.Remove(key, out cached))
            {
                Store(key, cached);
                return cached;
            }
        }

        Interlocked.Increment(ref _scans);
        var scan = QueryTextReferenceScanner.Scan(text);
        if (!scan.Parsed)
        {
            /* Split once and kept with the scan: the text search is then one set lookup per view, on this day and every later one. */
            scan = scan with { Words = SqlWholeWordMatcher.Words(text) };
        }

        lock (_gate)
        {
            Store(key, scan);
        }

        return scan;
    }

    /// <summary>Adds to the current generation, turning it over first when full. Called under the lock.</summary>
    private void Store(UInt128 key, QueryTextScan scan)
    {
        if (_current.Count >= _capacity && !_current.ContainsKey(key))
        {
            _previous = _current;
            _current = new Dictionary<UInt128, QueryTextScan>();
        }

        _current[key] = scan;
    }
}

/// <summary>
/// Adds up which names Query Store statements reference, and the executions of the statements that reference each (#8784 part 2). Darling
/// runs it over one server's day of statements to build its per-day summary; Lite runs it at read time over the window's statements.
///
/// <para><b>What is kept.</b> Each statement is scanned (<see cref="QueryTextReferenceScanner"/>); a name on a linked server, or in the
/// <c>sys</c> or <c>INFORMATION_SCHEMA</c> schema, is dropped, since it is never a user view of a monitored database. A database part that
/// names the statement's own database is cleared, so <c>Sales.dbo.v</c> run in <c>Sales</c> and <c>dbo.v</c> are one name. A database part
/// that names another database is kept: a view in <c>Reports</c> read only from statements run in <c>Sales</c> is used, and the read for
/// <c>Reports</c> looks for it there.</para>
///
/// <para><b>A statement that does not parse</b> is searched as text for the names <c>fallbackNames</c> returns for its database (Darling
/// passes the database's views from its object list), whole words only (<see cref="SqlWholeWordMatcher"/>). A match is recorded with no
/// schema, so it matches the name under any schema. With no names to search for (Lite has no object list), it is only counted.</para>
///
/// <para>Names are compared ignoring case: within one statement database, <c>dbo.Orders</c> and <c>DBO.ORDERS</c> are added up as one name,
/// spelled the first way seen. The comparison is <see cref="FoldName"/> on each part, the one fold Darling's summary also stores as its key
/// (#8784 review), so two names the aggregator keeps apart can never fold to one stored key and make the upsert touch a row twice.</para>
/// </summary>
public sealed class ObjectReferenceAggregator
{
    private readonly Func<string, IReadOnlyCollection<string>>? _fallbackNames;
    private readonly QueryTextScanCache? _cache;
    /* Keyed by the statement database exactly (Query Store's own name for it) and the parts as written folded by FoldName. */
    private readonly Dictionary<(string Db, string RefDb, string Schema, string Name, bool IsFunction), Accumulator> _references = new();
    private readonly Dictionary<string, long[]> _databases = new(StringComparer.Ordinal);
    /* The fallback names of each statement database, de-duplicated once (ignoring case, the first spelling kept). */
    private readonly Dictionary<string, string[]> _distinctFallbackNames = new(StringComparer.Ordinal);

    /// <summary>The one case fold names are merged and keyed by: <see cref="string.ToLowerInvariant()"/>.</summary>
    public static string FoldName(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        return name.ToLowerInvariant();
    }

    /// <summary>Creates an aggregator.</summary>
    /// <param name="fallbackNames">For a statement database, the names to search an unparsed statement's text for, or null for none.</param>
    /// <param name="cache">A scan cache to share across runs, or null to parse every statement.</param>
    public ObjectReferenceAggregator(Func<string, IReadOnlyCollection<string>>? fallbackNames = null, QueryTextScanCache? cache = null)
    {
        _fallbackNames = fallbackNames;
        _cache = cache;
    }

    /// <summary>Adds one statement.</summary>
    /// <param name="databaseName">The database it ran in.</param>
    /// <param name="text">Its text, or null where none is stored.</param>
    /// <param name="executions">Its executions.</param>
    /// <param name="lastExecution">Its newest execution.</param>
    public void Add(string databaseName, string? text, long executions, DateTime? lastExecution)
    {
        ArgumentNullException.ThrowIfNull(databaseName);

        if (!_databases.TryGetValue(databaseName, out var counts))
        {
            counts = new long[4];
            _databases.Add(databaseName, counts);
        }

        counts[0]++;
        counts[3] += executions;
        if (string.IsNullOrWhiteSpace(text))
        {
            counts[1]++;
            return;
        }

        var scan = _cache is null ? QueryTextReferenceScanner.Scan(text) : _cache.GetOrScan(text);
        if (!scan.Parsed)
        {
            counts[2]++;
            if (DistinctFallbackNames(databaseName) is { Length: > 0 } names)
            {
                /* The text is split into words once (kept by the cache across days), and a view name made of word characters, which is
                   nearly every one, is a set lookup (#8784 review): searching the text once per view cost the text's length times the
                   database's views. A name with any other character (a space, a dot) is still searched for in the text. */
                var words = scan.Words ?? SqlWholeWordMatcher.Words(text);
                foreach (var name in names)
                {
                    if (SqlWholeWordMatcher.IsWord(name) ? words.Contains(name) : SqlWholeWordMatcher.Contains(text, name))
                    {
                        Accumulate(databaseName, "", "", name, false, executions, lastExecution, textMatched: true);
                    }
                }
            }

            return;
        }

        foreach (var r in scan.References)
        {
            if (r.Server is not null || IsSystemSchema(r.Schema))
            {
                continue;
            }

            var referencedDatabase = r.Database is null || string.Equals(r.Database, databaseName, StringComparison.OrdinalIgnoreCase)
                ? ""
                : r.Database;
            Accumulate(databaseName, referencedDatabase, r.Schema ?? "", r.Name, r.Kind == ScannedReferenceKind.Function, executions, lastExecution, false);
        }
    }

    /// <summary>The database's fallback names with a repeated name (the same view in two schemas, or in another case) kept once, built on
    /// the first unparsed statement of the database and reused for the rest, so no statement builds a set of its own.</summary>
    private string[]? DistinctFallbackNames(string databaseName)
    {
        if (_fallbackNames is null)
        {
            return null;
        }

        if (!_distinctFallbackNames.TryGetValue(databaseName, out var distinct))
        {
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var list = new List<string>();
            foreach (var name in _fallbackNames(databaseName) ?? Array.Empty<string>())
            {
                if (seen.Add(name))
                {
                    list.Add(name);
                }
            }

            distinct = list.ToArray();
            _distinctFallbackNames.Add(databaseName, distinct);
        }

        return distinct;
    }

    /// <summary>The names referenced, one row per (statement database, referenced database, schema, name, kind).</summary>
    public IReadOnlyList<AggregatedReference> References
    {
        get
        {
            var rows = new List<AggregatedReference>(_references.Count);
            foreach (var a in _references.Values)
            {
                rows.Add(new AggregatedReference(
                    a.DatabaseName, a.ReferencedDatabase, a.SchemaName, a.ObjectName, a.IsFunction, a.Executions, a.LastExecution, a.Statements, a.TextMatched));
            }

            return rows;
        }
    }

    /// <summary>What was looked at, one row per statement database.</summary>
    public IReadOnlyList<AggregatedDatabase> Databases
    {
        get
        {
            var rows = new List<AggregatedDatabase>(_databases.Count);
            foreach (var (name, c) in _databases)
            {
                rows.Add(new AggregatedDatabase(name, c[0], c[1], c[2], c[3]));
            }

            return rows;
        }
    }

    private static bool IsSystemSchema(string? schema) =>
        string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase)
        || string.Equals(schema, "INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase);

    private void Accumulate(
        string databaseName, string referencedDatabase, string schema, string name, bool isFunction, long executions, DateTime? lastExecution,
        bool textMatched)
    {
        var key = (databaseName, FoldName(referencedDatabase), FoldName(schema), FoldName(name), isFunction);
        if (!_references.TryGetValue(key, out var a))
        {
            a = new Accumulator(databaseName, referencedDatabase, schema, name, isFunction);
            _references.Add(key, a);
        }

        a.Executions += executions;
        a.Statements++;
        a.TextMatched |= textMatched;
        if (lastExecution is { } last && (a.LastExecution is null || last > a.LastExecution))
        {
            a.LastExecution = last;
        }
    }

    private sealed class Accumulator(string databaseName, string referencedDatabase, string schemaName, string objectName, bool isFunction)
    {
        public string DatabaseName { get; } = databaseName;
        public string ReferencedDatabase { get; } = referencedDatabase;
        public string SchemaName { get; } = schemaName;
        public string ObjectName { get; } = objectName;
        public bool IsFunction { get; } = isFunction;
        public long Executions { get; set; }
        public DateTime? LastExecution { get; set; }
        public int Statements { get; set; }
        public bool TextMatched { get; set; }
    }
}
