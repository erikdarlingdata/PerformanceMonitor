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

namespace PerformanceMonitor.Common;

/// <summary>
/// The value list a column filter offers (#5565), like the filter in Excel: the distinct values of the column's
/// cells (ordinal ignore-case), sorted ordinal ignore-case, with a (Blanks) entry first when any cell is null, empty
/// or whitespace. No WPF dependency, so the popup and the tests drive the same code.
/// </summary>
public sealed class ColumnValueCatalog
{
    /// <summary>At most this many values are listed; the search box still reaches all of them.</summary>
    public const int ListCap = 1000;

    /// <summary>
    /// A column whose longest value is longer than this gets no list (it is prose, a statement or a plan, not a
    /// category), and keeps the text match.
    /// </summary>
    public const int MaxValueLength = 256;

    /// <summary>The wording under the list when the cap cuts it off.</summary>
    public static string CapNote(int total) =>
        string.Create(CultureInfo.InvariantCulture, $"Showing {ListCap:N0} of {total:N0} values. Search to find the rest.");

    /// <summary>The wording under the list when no value is ticked.</summary>
    public const string NoTicksNote = "No values are ticked, so no rows show.";

    private ColumnValueCatalog(IReadOnlyList<string> values, bool hasBlank, int maxLength)
    {
        Values = values;
        HasBlank = hasBlank;
        MaxLength = maxLength;
    }

    /// <summary>The distinct non-blank values, sorted ordinal ignore-case (the first spelling met is kept).</summary>
    public IReadOnlyList<string> Values { get; }

    /// <summary>At least one cell is null, empty or whitespace.</summary>
    public bool HasBlank { get; }

    /// <summary>The length of the longest non-blank value.</summary>
    public int MaxLength { get; }

    /// <summary>The column is a category-sized text column: every value fits <see cref="MaxValueLength"/>.</summary>
    public bool IsListable => MaxLength <= MaxValueLength;

    public static ColumnValueCatalog Build(IEnumerable<string?> cells)
    {
        var distinct = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var hasBlank = false;
        var maxLength = 0;
        foreach (var cell in cells)
        {
            if (string.IsNullOrWhiteSpace(cell))
            {
                hasBlank = true;
                continue;
            }
            if (cell.Length > maxLength)
                maxLength = cell.Length;
            distinct.TryAdd(cell, cell);
        }

        var values = distinct.Values.ToList();
        values.Sort(StringComparer.OrdinalIgnoreCase);
        return new ColumnValueCatalog(values, hasBlank, maxLength);
    }

    /// <summary>
    /// The entries to list under a search: the (Blanks) entry first (only while the search box is empty), then the
    /// values that contain the search text (ignore-case), cut to <see cref="ListCap"/>.
    /// </summary>
    public ColumnValueListing List(string? search)
    {
        var text = search?.Trim() ?? string.Empty;
        var matching = text.Length == 0
            ? Values
            : Values.Where(v => v.Contains(text, StringComparison.OrdinalIgnoreCase)).ToList();
        var shown = matching.Count > ListCap ? matching.Take(ListCap).ToList() : matching;
        return new ColumnValueListing(
            text.Length == 0 && HasBlank,
            shown,
            matching,
            matching.Count > ListCap ? CapNote(matching.Count) : null);
    }
}

/// <summary>One search's worth of the list: the (Blanks) entry, the listed values, and the cap note.</summary>
public sealed class ColumnValueListing
{
    internal ColumnValueListing(bool hasBlankEntry, IReadOnlyList<string> values, IReadOnlyList<string> allMatches, string? capNote)
    {
        HasBlankEntry = hasBlankEntry;
        Values = values;
        AllMatches = allMatches;
        CapNote = capNote;
    }

    /// <summary>The list opens with the (Blanks) entry.</summary>
    public bool HasBlankEntry { get; }

    /// <summary>The values listed (at most <see cref="ColumnValueCatalog.ListCap"/>).</summary>
    public IReadOnlyList<string> Values { get; }

    /// <summary>Every value the search matches, past the cap too: what Select All reaches.</summary>
    public IReadOnlyList<string> AllMatches { get; }

    /// <summary>"Showing 1,000 of N values. Search to find the rest." when the cap cut the list off, else null.</summary>
    public string? CapNote { get; }

    /// <summary>The entries listed, the (Blanks) entry included.</summary>
    public int Count => Values.Count + (HasBlankEntry ? 1 : 0);
}

/// <summary>
/// The ticks on a column's value list, from an existing filter state, and the state they make. Mode is chosen as
/// the spec says: everything ticked is None; fewer unticked than ticked is Hide the unticked (so a value that
/// first appears after a refresh still shows); otherwise ShowOnly the ticked (nothing ticked is ShowOnly of
/// nothing: no rows). A value the stored filter names but the rows no longer hold is kept, not listed.
/// </summary>
public sealed class ColumnValueSelection
{
    private readonly ColumnValueCatalog _catalog;
    private readonly HashSet<string> _unticked = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _absentNamed;
    private readonly ColumnValueMode _absentMode;
    private readonly bool _absentBlankNamed;
    private bool _blankTicked = true;

    public ColumnValueSelection(ColumnValueCatalog catalog, ColumnFilterState? existing)
    {
        _catalog = catalog;
        _absentNamed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (existing is null || !existing.HasValuePart)
        {
            _absentMode = ColumnValueMode.None;
            return;
        }

        _absentMode = existing.ValueMode;
        var present = new HashSet<string>(catalog.Values, StringComparer.OrdinalIgnoreCase);

        if (existing.ValueMode == ColumnValueMode.Hide)
        {
            foreach (var v in catalog.Values)
                if (existing.Values.Contains(v))
                    _unticked.Add(v);
            _blankTicked = !(existing.ValueBlank && catalog.HasBlank);
        }
        else
        {
            foreach (var v in catalog.Values)
                if (!existing.Values.Contains(v))
                    _unticked.Add(v);
            _blankTicked = existing.ValueBlank || !catalog.HasBlank;
        }

        foreach (var v in existing.Values)
            if (!present.Contains(v))
                _absentNamed.Add(v);
        _absentBlankNamed = existing.ValueBlank && !catalog.HasBlank;
    }

    /// <summary>Whether a value is ticked.</summary>
    public bool IsTicked(string value) => !_unticked.Contains(value);

    /// <summary>Whether the (Blanks) entry is ticked.</summary>
    public bool IsBlankTicked => _blankTicked;

    public void SetTicked(string value, bool ticked)
    {
        if (ticked) _unticked.Remove(value); else _unticked.Add(value);
    }

    public void SetBlankTicked(bool ticked) => _blankTicked = ticked;

    /// <summary>
    /// Select All: ticks or unticks every value a search matches (past the cap too), and the (Blanks) entry when
    /// the search shows it.
    /// </summary>
    public void SetAll(ColumnValueListing listing, bool ticked)
    {
        foreach (var v in listing.AllMatches)
            SetTicked(v, ticked);
        if (listing.HasBlankEntry)
            SetBlankTicked(ticked);
    }

    /// <summary>Unticks everything, then ticks the given values (and the (Blanks) entry when asked).</summary>
    public void TickOnly(IEnumerable<string> values, bool blank)
    {
        _unticked.Clear();
        foreach (var v in _catalog.Values)
            _unticked.Add(v);
        foreach (var v in values)
            _unticked.Remove(v);
        _blankTicked = blank;
    }

    /// <summary>Writes the value part this selection makes into <paramref name="state"/> (the text match is left alone).</summary>
    public void ApplyTo(ColumnFilterState state)
    {
        var blankEntry = _catalog.HasBlank;
        var presentItems = _catalog.Values.Count + (blankEntry ? 1 : 0);
        var untickedPresent = _unticked.Count + (blankEntry && !_blankTicked ? 1 : 0);
        var tickedPresent = presentItems - untickedPresent;

        /* A value or blank the stored filter named but the rows no longer hold keeps its side: unticked under Hide
           (named = hidden), ticked under ShowOnly (named = shown). */
        var absentCount = _absentNamed.Count + (_absentBlankNamed ? 1 : 0);
        var absentUnticked = _absentMode == ColumnValueMode.Hide ? absentCount : 0;
        var absentTicked = _absentMode == ColumnValueMode.ShowOnly ? absentCount : 0;

        var unticked = untickedPresent + absentUnticked;
        var ticked = tickedPresent + absentTicked;

        state.Values = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        state.ValueBlank = false;

        if (unticked == 0)
        {
            state.ValueMode = ColumnValueMode.None;
            return;
        }

        if (unticked < ticked)
        {
            state.ValueMode = ColumnValueMode.Hide;
            foreach (var v in _unticked)
                state.Values.Add(v);
            state.ValueBlank = blankEntry ? !_blankTicked : _absentMode == ColumnValueMode.Hide && _absentBlankNamed;
            if (_absentMode == ColumnValueMode.Hide)
                foreach (var v in _absentNamed)
                    state.Values.Add(v);
        }
        else
        {
            state.ValueMode = ColumnValueMode.ShowOnly;
            foreach (var v in _catalog.Values)
                if (!_unticked.Contains(v))
                    state.Values.Add(v);
            state.ValueBlank = blankEntry ? _blankTicked : _absentMode == ColumnValueMode.ShowOnly && _absentBlankNamed;
            if (_absentMode == ColumnValueMode.ShowOnly)
                foreach (var v in _absentNamed)
                    state.Values.Add(v);
        }
    }
}
