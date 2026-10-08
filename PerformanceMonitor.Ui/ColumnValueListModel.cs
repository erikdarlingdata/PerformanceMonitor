/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>One line of the value list: a value, or the (Blanks) entry, with its tick.</summary>
public sealed class ColumnValueEntry : INotifyPropertyChanged
{
    private readonly Action<ColumnValueEntry> _changed;
    private bool _isTicked;

    internal ColumnValueEntry(string label, string? value, bool isBlank, bool isTicked, Action<ColumnValueEntry> changed)
    {
        Label = label;
        Value = value;
        IsBlank = isBlank;
        _isTicked = isTicked;
        _changed = changed;
    }

    public string Label { get; }

    /// <summary>The value, or null for the (Blanks) entry.</summary>
    public string? Value { get; }

    public bool IsBlank { get; }

    public bool IsTicked
    {
        get => _isTicked;
        set
        {
            if (_isTicked == value)
                return;
            _isTicked = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsTicked)));
            _changed(this);
        }
    }

    internal void Set(bool ticked)
    {
        if (_isTicked == ticked)
            return;
        _isTicked = ticked;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsTicked)));
    }

    public event PropertyChangedEventHandler? PropertyChanged;
}

/// <summary>
/// What the column filter popup's value list shows and does (#5565), apart from the controls: the search box, the
/// entries (the (Blanks) entry first, the cap), Select All under a search, the two notes, and the value part of the
/// filter state the ticks make. The popup binds to it; the tests drive it directly.
/// </summary>
public sealed class ColumnValueListModel
{
    private readonly ColumnValueCatalog _catalog;
    private readonly ColumnValueSelection _selection;
    private ColumnValueListing _listing;
    private string _search = string.Empty;

    public ColumnValueListModel(ColumnValueCatalog catalog, ColumnFilterState? existing)
    {
        _catalog = catalog;
        _selection = new ColumnValueSelection(catalog, existing);
        _listing = catalog.List(_search);
        Rebuild();
    }

    /// <summary>The entries listed under the current search (what the ListBox shows).</summary>
    public ObservableCollection<ColumnValueEntry> Entries { get; } = new();

    public string Search
    {
        get => _search;
        set
        {
            if (_search == value)
                return;
            _search = value;
            _listing = _catalog.List(_search);
            Rebuild();
        }
    }

    /// <summary>The cap note, or null.</summary>
    public string? CapNote => _listing.CapNote;

    /// <summary>The no-ticks note, or null.</summary>
    public string? NoTicksNote => Preview().ShowsNoValues ? ColumnValueCatalog.NoTicksNote : null;

    /// <summary>Both notes that apply, one per line, or null for none.</summary>
    public string? Note
    {
        get
        {
            var notes = new[] { CapNote, NoTicksNote }.Where(n => n is not null).ToArray();
            return notes.Length == 0 ? null : string.Join(Environment.NewLine, notes);
        }
    }

    /// <summary>Select All: true when every listed match is ticked, false when none is, null when mixed.</summary>
    public bool? SelectAllState
    {
        get
        {
            var values = _listing.AllMatches;
            var ticked = values.Count(_selection.IsTicked) + (_listing.HasBlankEntry && _selection.IsBlankTicked ? 1 : 0);
            var total = values.Count + (_listing.HasBlankEntry ? 1 : 0);
            return total == 0 ? true : ticked == total ? true : ticked == 0 ? false : null;
        }
    }

    /// <summary>Select All as clicked: ticks every match unless they all are ticked already, then unticks them.</summary>
    public void ToggleSelectAll()
    {
        SetAll(SelectAllState != true);
    }

    /// <summary>Ticks or unticks every value the current search matches (past the cap too), and (Blanks) when listed.</summary>
    public void SetAll(bool ticked)
    {
        _selection.SetAll(_listing, ticked);
        foreach (var entry in Entries)
            entry.Set(ticked);
    }

    /// <summary>Raised after a tick changes, so the popup can refresh Select All and the note.</summary>
    public event EventHandler? TicksChanged;

    /// <summary>Writes the value part the ticks make into <paramref name="state"/>.</summary>
    public void ApplyTo(ColumnFilterState state) => _selection.ApplyTo(state);

    /// <summary>Ticks only these (used by tests and by "show only").</summary>
    public void TickOnly(System.Collections.Generic.IEnumerable<string> values, bool blank)
    {
        _selection.TickOnly(values, blank);
        foreach (var entry in Entries)
            entry.Set(entry.IsBlank ? _selection.IsBlankTicked : _selection.IsTicked(entry.Value!));
    }

    private ColumnFilterState Preview()
    {
        var preview = new ColumnFilterState();
        _selection.ApplyTo(preview);
        return preview;
    }

    private void Rebuild()
    {
        Entries.Clear();
        if (_listing.HasBlankEntry)
            Entries.Add(new ColumnValueEntry("(Blanks)", null, true, _selection.IsBlankTicked, OnEntryChanged));
        foreach (var value in _listing.Values)
            Entries.Add(new ColumnValueEntry(value, value, false, _selection.IsTicked(value), OnEntryChanged));
        TicksChanged?.Invoke(this, EventArgs.Empty);
    }

    private void OnEntryChanged(ColumnValueEntry entry)
    {
        if (entry.IsBlank)
            _selection.SetBlankTicked(entry.IsTicked);
        else
            _selection.SetTicked(entry.Value!, entry.IsTicked);
        TicksChanged?.Invoke(this, EventArgs.Empty);
    }
}
