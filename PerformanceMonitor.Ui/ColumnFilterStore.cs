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
/// names more than <see cref="MaxValuesPerColumn"/> values is not stored. The file holds only what the user ticked
/// or typed, plus the server, grid and column names that key it: no row data beyond those values. A file that
/// cannot be read is reported once and ignored; a grid is never blocked by the store.
/// </summary>
public sealed class ColumnFilterStore : IDisposable
{
    public const int MaxGrids = 200;
    public const int MaxValuesPerColumn = 1000;

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

    /// <summary>Points the running app's grids at one file, and flushes it when the process exits.</summary>
    public static void Install(string filePath, Action<string>? warn = null)
    {
        Current = new ColumnFilterStore(filePath, warn);
        if (!s_exitHooked)
        {
            s_exitHooked = true;
            AppDomain.CurrentDomain.ProcessExit += (_, _) => Current?.Flush();
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
    /// Replaces a server's grid's stored filters with these (active ones only). None removes the grid.
    /// </summary>
    public void Save(string server, string grid, IEnumerable<ColumnFilterState> filters)
    {
        lock (_gate)
        {
            EnsureLoaded();
            var keep = filters
                .Where(f => f.IsActive && f.Values.Count <= MaxValuesPerColumn)
                .Select(Clone)
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
            _dirty = false;

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
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException)
            {
                _warn?.Invoke($"The column filters could not be saved to '{_filePath}': {ex.Message}");
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
            var file = JsonSerializer.Deserialize<FileDto>(File.ReadAllText(_filePath));
            if (file?.Grids is null || file.Version != FileVersion)
            {
                _warn?.Invoke($"The column filter file '{_filePath}' is not in a format this version reads, so it is ignored.");
                return;
            }

            foreach (var grid in file.Grids)
            {
                if (string.IsNullOrEmpty(grid.Server) || string.IsNullOrEmpty(grid.Grid) || grid.Columns is null)
                    continue;
                var filters = grid.Columns
                    .Where(c => !string.IsNullOrEmpty(c.Column))
                    .Select(FromDto)
                    .Where(f => f.IsActive)
                    .ToList();
                if (filters.Count > 0)
                    _entries.Add(new GridEntry { Server = grid.Server, Grid = grid.Grid, Filters = filters });
            }
            while (_entries.Count > MaxGrids)
                _entries.RemoveAt(0);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or NotSupportedException or ArgumentException)
        {
            _entries.Clear();
            _warn?.Invoke($"The column filter file '{_filePath}' could not be read, so it is ignored: {ex.Message}");
        }
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

    private static ColumnFilterState FromDto(ColumnDto c) => new()
    {
        ColumnName = c.Column ?? string.Empty,
        Operator = Enum.TryParse<FilterOperator>(c.Operator, out var op) ? op : FilterOperator.Contains,
        Value = c.Value ?? string.Empty,
        ValueMode = Enum.TryParse<ColumnValueMode>(c.ValueMode, out var mode) ? mode : ColumnValueMode.None,
        Values = new HashSet<string>((c.Values ?? new List<string>()).Where(v => v is not null), StringComparer.OrdinalIgnoreCase),
        ValueBlank = c.ValueBlank
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
