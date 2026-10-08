/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Keeps the active column filters across a restart (#5565): per server + grid + column, active filters only,
/// removed when cleared. One JSON file, written debounced and atomically (temp file, then replace). Bounded: at
/// most <see cref="MaxGrids"/> grids are kept (the least recently used go first), and a column whose value list
/// names more than <see cref="MaxValuesPerColumn"/> values keeps its text match and loses only the value part. The
/// file holds only what the user ticked or typed, plus the server, grid and column names that key it: no row data
/// beyond those values, and no text match on a column that never gets a list (query, statement, plan, XML and
/// prose columns: a term typed there is kept for the session only). Every limit applies when the file is read as
/// well as when it is written, and an entry that is null or of the wrong type is skipped. A file that cannot be
/// read is reported once and ignored; a grid is never blocked by the store.
/// </summary>
public sealed class ColumnFilterStore : IDisposable
{
    public const int MaxGrids = 200;
    public const int MaxValuesPerColumn = 1000;

    /// <summary>The longest value, text match or key (column name) the store keeps; also checked on read.</summary>
    public const int MaxValueLength = 1000;

    /// <summary>At most this many columns are kept per grid.</summary>
    public const int MaxColumnsPerGrid = 200;

    /// <summary>A file larger than this is ignored whole (the limits above cap a real file far below it).</summary>
    public const long MaxFileBytes = 16L * 1024 * 1024;

    private const int FileVersion = 1;
    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = true };

    private static ColumnFilterStore? s_current;
    private static bool s_exitHooked;

    /// <summary>
    /// The store the running app wrote its path into (<see cref="Install"/>). Null means filters are kept for the
    /// session only: a test, or an app that did not name a path.
    /// </summary>
    public static ColumnFilterStore? Current
    {
        get => Volatile.Read(ref s_current);
        set => Volatile.Write(ref s_current, value);
    }

    /// <summary>
    /// Points the running app's grids at one file, and flushes it when the process exits. <paramref name="legacyFilePath"/>
    /// names the place an earlier build kept the file: it is read once (moved to <paramref name="filePath"/>) and then gone.
    /// </summary>
    public static void Install(string filePath, Action<string>? warn = null, string? legacyFilePath = null)
    {
        if (!string.IsNullOrEmpty(legacyFilePath))
            MigrateLegacyFile(legacyFilePath, filePath, warn);
        Current = new ColumnFilterStore(filePath, warn);
        if (!s_exitHooked)
        {
            s_exitHooked = true;
            AppDomain.CurrentDomain.ProcessExit += (_, _) => Current?.Flush();
        }
    }

    /// <summary>
    /// The Viewer first kept its file under the roaming profile, which copies the logins, hosts and application names
    /// it holds to a profile server. It now lives under local application data like Lite's. A file found at the old
    /// place becomes the new one when there is none yet (so the filters survive the move), and is deleted either way.
    /// </summary>
    public static void MigrateLegacyFile(string legacyFilePath, string filePath, Action<string>? warn = null)
    {
        try
        {
            if (!File.Exists(legacyFilePath) || string.Equals(Path.GetFullPath(legacyFilePath), Path.GetFullPath(filePath), StringComparison.OrdinalIgnoreCase))
                return;
            if (!File.Exists(filePath))
            {
                var directory = Path.GetDirectoryName(filePath);
                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);
                File.Move(legacyFilePath, filePath);
            }
            else
            {
                File.Delete(legacyFilePath);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException or ArgumentException)
        {
            warn?.Invoke($"The earlier column filter file '{legacyFilePath}' could not be moved to '{filePath}': {ex.Message}");
        }
    }

    private sealed class GridEntry
    {
        public string Server = string.Empty;
        public string Grid = string.Empty;
        public List<ColumnFilterState> Filters = new();
    }

    private readonly object _gate = new();
    private readonly string _filePath;
    private readonly Action<string>? _warn;
    private readonly TimeSpan _debounce;
    private readonly Timer _timer;
    private readonly List<GridEntry> _entries = new(); // least recently used first
    private bool _loaded;
    private bool _dirty;
    private bool _disposed;
    private bool _saveWarned;

    public ColumnFilterStore(string filePath, Action<string>? warn = null, TimeSpan? debounce = null)
    {
        _filePath = filePath;
        _warn = warn;
        _debounce = debounce ?? TimeSpan.FromSeconds(1.5);
        _timer = new Timer(_ => Flush(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public string FilePath => _filePath;

    /// <summary>The active filters stored for a server's grid, or none. Counts as a use of the grid.</summary>
    public IReadOnlyList<ColumnFilterState> Load(string server, string grid)
    {
        lock (_gate)
        {
            EnsureLoaded();
            var index = _entries.FindIndex(e => Matches(e, server, grid));
            if (index < 0)
                return Array.Empty<ColumnFilterState>();

            var entry = _entries[index];
            if (index != _entries.Count - 1)
            {
                _entries.RemoveAt(index);
                _entries.Add(entry);
                MarkDirty();
            }
            return entry.Filters.Select(Clone).ToList();
        }
    }

    /// <summary>
    /// Replaces a server's grid's stored filters with these (the storable active ones: see <see cref="Storable"/>).
    /// None removes the grid.
    /// </summary>
    public void Save(string server, string grid, IEnumerable<ColumnFilterState> filters)
    {
        lock (_gate)
        {
            EnsureLoaded();
            var keep = filters
                .Select(Storable)
                .OfType<ColumnFilterState>()
                .Take(MaxColumnsPerGrid)
                .ToList();

            var index = _entries.FindIndex(e => Matches(e, server, grid));
            if (index >= 0)
                _entries.RemoveAt(index);

            if (keep.Count > 0)
            {
                _entries.Add(new GridEntry { Server = server, Grid = grid, Filters = keep });
                while (_entries.Count > MaxGrids)
                    _entries.RemoveAt(0);
            }
            else if (index < 0)
            {
                return; // nothing stored, nothing to store
            }

            MarkDirty();
        }
    }

    /// <summary>Writes any pending change now (the debounce timer and the process exit both end here).</summary>
    public void Flush()
    {
        lock (_gate)
        {
            if (!_dirty || _disposed)
                return;

            try
            {
                var file = new FileDto
                {
                    Version = FileVersion,
                    Grids = _entries.Select(e => new GridDto
                    {
                        Server = e.Server,
                        Grid = e.Grid,
                        Columns = e.Filters.Select(ToDto).ToList()
                    }).ToList()
                };

                var directory = Path.GetDirectoryName(_filePath);
                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);

                var temp = _filePath + ".tmp";
                File.WriteAllText(temp, JsonSerializer.Serialize(file, s_json));
                if (File.Exists(_filePath))
                    File.Replace(temp, _filePath, null);
                else
                    File.Move(temp, _filePath);
                _dirty = false;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
            {
                /* Still dirty: the next debounce tick tries again (a locked file or a full disk is often gone by then).
                   The warning is given once per run, not once per failed try. */
                if (!_saveWarned)
                {
                    _saveWarned = true;
                    _warn?.Invoke($"The column filters could not be saved to '{_filePath}' (the save is tried again at each change): {ex.Message}");
                }
                if (!_disposed)
                    _timer.Change(_debounce, Timeout.InfiniteTimeSpan);
            }
        }
    }

    public void Dispose()
    {
        Flush();
        lock (_gate)
            _disposed = true;
        _timer.Dispose();
    }

    private static bool Matches(GridEntry e, string server, string grid) =>
        string.Equals(e.Server, server, StringComparison.Ordinal) && string.Equals(e.Grid, grid, StringComparison.Ordinal);

    private void MarkDirty()
    {
        _dirty = true;
        if (!_disposed)
            _timer.Change(_debounce, Timeout.InfiniteTimeSpan);
    }

    private void EnsureLoaded()
    {
        if (_loaded)
            return;
        _loaded = true;

        if (!File.Exists(_filePath))
            return;

        try
        {
            if (new FileInfo(_filePath).Length > MaxFileBytes)
            {
                _warn?.Invoke($"The column filter file '{_filePath}' is larger than {MaxFileBytes / (1024 * 1024)} MB, so it is ignored.");
                return;
            }

            using var doc = JsonDocument.Parse(File.ReadAllText(_filePath), new JsonDocumentOptions { MaxDepth = 16 });
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty(nameof(FileDto.Version), out var version) ||
                version.ValueKind != JsonValueKind.Number || !version.TryGetInt32(out var v) || v != FileVersion ||
                !root.TryGetProperty(nameof(FileDto.Grids), out var grids) || grids.ValueKind != JsonValueKind.Array)
            {
                _warn?.Invoke($"The column filter file '{_filePath}' is not in a format this version reads, so it is ignored.");
                return;
            }

            /* Only the newest MaxGrids entries are looked at, so a file with millions of grids costs no more than 200. */
            var skip = grids.GetArrayLength() - MaxGrids;
            var index = 0;
            foreach (var grid in grids.EnumerateArray())
            {
                if (index++ < skip)
                    continue;
                var entry = ReadGrid(grid);
                if (entry is not null)
                    _entries.Add(entry);
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or NotSupportedException or ArgumentException or InvalidOperationException)
        {
            _entries.Clear();
            _warn?.Invoke($"The column filter file '{_filePath}' could not be read, so it is ignored: {ex.Message}");
        }
    }

    /// <summary>One grid of the file, or null when it is not a usable one. A column that is not one is left out on its own.</summary>
    private static GridEntry? ReadGrid(JsonElement grid)
    {
        if (grid.ValueKind != JsonValueKind.Object ||
            !TryText(grid, nameof(GridDto.Server), MaxValueLength, out var server) || server.Length == 0 ||
            !TryText(grid, nameof(GridDto.Grid), MaxValueLength, out var name) || name.Length == 0 ||
            !grid.TryGetProperty(nameof(GridDto.Columns), out var columns) || columns.ValueKind != JsonValueKind.Array)
            return null;

        var filters = new List<ColumnFilterState>();
        foreach (var column in columns.EnumerateArray())
        {
            if (filters.Count >= MaxColumnsPerGrid)
                break;
            var filter = ReadColumn(column);
            if (filter is not null)
                filters.Add(filter);
        }
        return filters.Count == 0 ? null : new GridEntry { Server = server, Grid = name, Filters = filters };
    }

    /// <summary>
    /// One column of the file, field by field. A text match that is too long is left out, and so is a value part with
    /// too many values, a value that is too long or a mode this version does not know (the same rules the web copy
    /// keeps); a column with nothing left, or one that never gets a list, is null.
    /// </summary>
    private static ColumnFilterState? ReadColumn(JsonElement column)
    {
        if (column.ValueKind != JsonValueKind.Object ||
            !TryText(column, nameof(ColumnDto.Column), MaxValueLength, out var name) || name.Length == 0)
            return null;

        var filter = new ColumnFilterState { ColumnName = name };
        if (TryText(column, nameof(ColumnDto.Operator), 64, out var op) &&
            Enum.TryParse<FilterOperator>(op, out var parsedOp) && Enum.IsDefined(parsedOp))
            filter.Operator = parsedOp;
        if (TryText(column, nameof(ColumnDto.Value), MaxValueLength, out var text))
            filter.Value = text;

        if (TryText(column, nameof(ColumnDto.ValueMode), 64, out var mode) &&
            Enum.TryParse<ColumnValueMode>(mode, out var parsedMode) && Enum.IsDefined(parsedMode) && parsedMode != ColumnValueMode.None &&
            column.TryGetProperty(nameof(ColumnDto.Values), out var values) && values.ValueKind == JsonValueKind.Array &&
            values.GetArrayLength() <= MaxValuesPerColumn)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var valid = true;
            foreach (var value in values.EnumerateArray())
            {
                var item = value.ValueKind == JsonValueKind.String ? value.GetString() : null;
                if (item is null || item.Length > MaxValueLength)
                {
                    valid = false;
                    break;
                }
                set.Add(item);
            }
            if (valid)
            {
                filter.ValueMode = parsedMode;
                filter.Values = set;
                filter.ValueBlank = column.TryGetProperty(nameof(ColumnDto.ValueBlank), out var blank) && blank.ValueKind == JsonValueKind.True;
            }
        }

        return Storable(filter);
    }

    private static bool TryText(JsonElement parent, string property, int maxLength, out string value)
    {
        value = string.Empty;
        if (!parent.TryGetProperty(property, out var element) || element.ValueKind != JsonValueKind.String)
            return false;
        var text = element.GetString();
        if (text is null || text.Length > maxLength)
            return false;
        value = text;
        return true;
    }

    /// <summary>
    /// What the store keeps of a column's filter, or null for nothing. A column that never gets a list (query,
    /// statement, plan, XML and prose columns) keeps nothing: a term typed or pasted there is for the session only.
    /// A value part naming more than <see cref="MaxValuesPerColumn"/> values (or a value longer than
    /// <see cref="MaxValueLength"/>) is dropped and the text match stays, since a cut set would change what it hides.
    /// </summary>
    private static ColumnFilterState? Storable(ColumnFilterState f)
    {
        if (string.IsNullOrEmpty(f.ColumnName) || f.ColumnName.Length > MaxValueLength || ColumnValueListColumns.IsExcluded(f.ColumnName))
            return null;

        var copy = Clone(f);
        if (copy.Value.Length > MaxValueLength)
            copy.Value = string.Empty;
        if (copy.Values.Count > MaxValuesPerColumn || copy.Values.Any(v => v.Length > MaxValueLength))
        {
            copy.ValueMode = ColumnValueMode.None;
            copy.Values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            copy.ValueBlank = false;
        }
        return copy.IsActive ? copy : null;
    }

    private static ColumnFilterState Clone(ColumnFilterState f) => new()
    {
        ColumnName = f.ColumnName,
        Operator = f.Operator,
        Value = f.Value,
        ValueMode = f.ValueMode,
        Values = new HashSet<string>(f.Values, StringComparer.OrdinalIgnoreCase),
        ValueBlank = f.ValueBlank
    };

    private static ColumnDto ToDto(ColumnFilterState f) => new()
    {
        Column = f.ColumnName,
        Operator = f.Operator.ToString(),
        Value = f.Value,
        ValueMode = f.ValueMode.ToString(),
        Values = f.Values.OrderBy(v => v, StringComparer.OrdinalIgnoreCase).ToList(),
        ValueBlank = f.ValueBlank
    };

    private sealed class FileDto
    {
        public int Version { get; set; }
        public List<GridDto>? Grids { get; set; }
    }

    private sealed class GridDto
    {
        public string? Server { get; set; }
        public string? Grid { get; set; }
        public List<ColumnDto>? Columns { get; set; }
    }

    private sealed class ColumnDto
    {
        public string? Column { get; set; }
        public string? Operator { get; set; }
        public string? Value { get; set; }
        public string? ValueMode { get; set; }
        public List<string>? Values { get; set; }
        public bool ValueBlank { get; set; }
    }
}
