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
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The value-list column filter (#5565): the list builder (distinct, ignore-case, sorted, blank flag, 1,000 cap,
/// search), the tick selection and its mode choice, the match, the chip text, which columns get a list, and the
/// popup's list model (Select All under a search, the notes). The shared fixture cases run in
/// <see cref="ColumnValueFilterCasesTests"/>.
/// </summary>
public sealed class ColumnValueListTests
{
    private static ColumnValueCatalog Catalog(params string?[] cells) => ColumnValueCatalog.Build(cells);

    [Fact]
    public void The_list_is_distinct_ignoring_case_sorted_and_keeps_the_first_spelling()
    {
        var catalog = Catalog("sa", "app", "APP", "Job_svc", "App");

        Assert.Equal(new[] { "app", "Job_svc", "sa" }, catalog.Values);
        Assert.False(catalog.HasBlank);
    }

    [Fact]
    public void Null_empty_and_whitespace_cells_are_all_the_blank_flag()
    {
        Assert.True(Catalog("a", null).HasBlank);
        Assert.True(Catalog("a", "").HasBlank);
        Assert.True(Catalog("a", "   ").HasBlank);
        Assert.False(Catalog("a", "b").HasBlank);
        Assert.Equal(new[] { "a" }, Catalog("a", "  ", null, "").Values);
    }

    [Fact]
    public void The_blanks_entry_opens_the_list_only_when_the_search_is_empty()
    {
        var catalog = Catalog("sa", null);

        Assert.True(catalog.List("").HasBlankEntry);
        Assert.True(catalog.List(null).HasBlankEntry);
        Assert.False(catalog.List("s").HasBlankEntry);
        Assert.Equal(2, catalog.List("").Count);
    }

    [Fact]
    public void The_list_is_cut_at_1000_with_a_note_and_the_search_reaches_the_rest()
    {
        var catalog = ColumnValueCatalog.Build(Enumerable.Range(0, 1500).Select(i => (string?)("x" + i)));

        var all = catalog.List("");
        Assert.Equal(1000, all.Values.Count);
        Assert.Equal(1500, all.AllMatches.Count);
        Assert.Equal("Showing 1,000 of 1,500 values. Search to find the rest.", all.CapNote);

        var found = catalog.List("X99");
        Assert.Equal(11, found.Values.Count);
        Assert.Null(found.CapNote);
        Assert.Contains("x99", found.Values);
        Assert.DoesNotContain("x99", all.Values);
    }

    [Fact]
    public void A_search_that_still_matches_more_than_1000_says_how_many()
    {
        var catalog = ColumnValueCatalog.Build(Enumerable.Range(0, 2500).Select(i => (string?)("login" + i)));

        Assert.Equal("Showing 1,000 of 2,500 values. Search to find the rest.", catalog.List("login").CapNote);
        Assert.Equal("Showing 1,000 of 1,111 values. Search to find the rest.", catalog.List("login1").CapNote);
    }

    [Fact]
    public void A_column_with_a_value_over_256_characters_gets_no_list()
    {
        Assert.True(Catalog("short", new string('a', 256)).IsListable);
        Assert.False(Catalog("short", new string('a', 257)).IsListable);
    }

    [Theory]
    [InlineData("QueryText", true)]
    [InlineData("SqlText", true)]
    [InlineData("StatementText", true)]
    [InlineData("TextData", true)]
    [InlineData("QueryPlan", true)]
    [InlineData("LiveQueryPlan", true)]
    [InlineData("QueryPlanXml", true)]
    [InlineData("DeadlockGraphXml", true)]
    [InlineData("ErrorMessage", true)]
    [InlineData("OriginalIndexDefinition", true)]
    [InlineData("LastSeenText", true)]
    [InlineData("SqlDurationFormatted", true)]
    [InlineData("LoginName", false)]
    [InlineData("DatabaseName", false)]
    [InlineData("HostName", false)]
    [InlineData("ProgramName", false)]
    [InlineData("WaitType", false)]
    [InlineData("", true)]
    public void Query_plan_xml_and_prose_columns_are_excluded_by_name(string column, bool excluded)
    {
        Assert.Equal(excluded, ColumnValueListColumns.IsExcluded(column));
    }

    private static ColumnFilterState Apply(ColumnValueSelection selection)
    {
        var state = new ColumnFilterState { ColumnName = "c" };
        selection.ApplyTo(state);
        return state;
    }

    [Fact]
    public void All_ticked_is_no_value_filter()
    {
        var state = Apply(new ColumnValueSelection(Catalog("a", "b", null), null));

        Assert.Equal(ColumnValueMode.None, state.ValueMode);
        Assert.False(state.IsActive);
    }

    [Fact]
    public void Fewer_unticked_than_ticked_hides_the_unticked_and_otherwise_shows_only_the_ticked()
    {
        var catalog = Catalog("a", "b", "c", "d");

        var hide = new ColumnValueSelection(catalog, null);
        hide.SetTicked("a", false);
        var hidden = Apply(hide);
        Assert.Equal(ColumnValueMode.Hide, hidden.ValueMode);
        Assert.Equal(new[] { "a" }, hidden.Values);

        var tie = new ColumnValueSelection(catalog, null);
        tie.SetTicked("a", false);
        tie.SetTicked("b", false);
        var shown = Apply(tie);
        Assert.Equal(ColumnValueMode.ShowOnly, shown.ValueMode);
        Assert.Equal(new[] { "c", "d" }, shown.Values.OrderBy(v => v));
    }

    [Fact]
    public void Nothing_ticked_is_show_only_of_nothing_and_it_says_so()
    {
        var model = new ColumnValueListModel(Catalog("a", "b"), null);
        model.SetAll(false);

        var state = new ColumnFilterState();
        model.ApplyTo(state);
        Assert.Equal(ColumnValueMode.ShowOnly, state.ValueMode);
        Assert.True(state.ShowsNoValues);
        Assert.True(state.IsActive);
        Assert.Equal("No values are ticked, so no rows show.", model.NoTicksNote);
        Assert.False(ColumnFilterMatcher.MatchesFilter(new Cell("a"), WithColumn(state)));
    }

    private sealed class Cell
    {
        public Cell(string? v) { Value = v; }
        public string? Value { get; }
    }

    private static ColumnFilterState WithColumn(ColumnFilterState state)
    {
        state.ColumnName = nameof(Cell.Value);
        return state;
    }

    [Fact]
    public void A_stored_filter_naming_a_value_the_rows_no_longer_hold_survives_an_unchanged_apply()
    {
        var existing = new ColumnFilterState { ColumnName = "c", ValueMode = ColumnValueMode.Hide };
        existing.Values.Add("job_svc");

        var state = Apply(new ColumnValueSelection(Catalog("sa", "app"), existing));

        Assert.Equal(ColumnValueMode.Hide, state.ValueMode);
        Assert.Equal(new[] { "job_svc" }, state.Values);
    }

    [Fact]
    public void Reopening_a_stored_filter_shows_the_same_ticks()
    {
        var catalog = Catalog("sa", "app", "job_svc", null);
        var first = new ColumnValueSelection(catalog, null);
        first.SetTicked("job_svc", false);
        first.SetBlankTicked(false);
        var stored = Apply(first);

        var again = new ColumnValueSelection(catalog, stored);

        Assert.False(again.IsTicked("job_svc"));
        Assert.True(again.IsTicked("sa"));
        Assert.True(again.IsTicked("app"));
        Assert.False(again.IsBlankTicked);
    }

    [Fact]
    public void The_value_part_and_the_text_match_are_anded_and_either_makes_the_filter_active()
    {
        var state = new ColumnFilterState { ColumnName = nameof(Cell.Value), ValueMode = ColumnValueMode.ShowOnly };
        state.Values.Add("app");

        Assert.True(state.IsActive);
        Assert.False(state.HasTextMatch);
        Assert.True(ColumnFilterMatcher.MatchesFilter(new Cell("APP"), state));
        Assert.False(ColumnFilterMatcher.MatchesFilter(new Cell("sa"), state));

        state.Operator = FilterOperator.StartsWith;
        state.Value = "z";
        Assert.False(ColumnFilterMatcher.MatchesFilter(new Cell("app"), state));

        var textOnly = new ColumnFilterState { ColumnName = nameof(Cell.Value), Value = "p" };
        Assert.True(textOnly.IsActive);
        Assert.True(ColumnFilterMatcher.MatchesFilter(new Cell("app"), textOnly));
    }

    [Fact]
    public void A_whitespace_cell_is_blank_and_a_real_value_spelled_Blanks_is_not()
    {
        var hideBlank = new ColumnFilterState { ColumnName = nameof(Cell.Value), ValueMode = ColumnValueMode.Hide, ValueBlank = true };

        Assert.False(ColumnFilterMatcher.MatchesFilter(new Cell("  "), hideBlank));
        Assert.False(ColumnFilterMatcher.MatchesFilter(new Cell(null), hideBlank));
        Assert.True(ColumnFilterMatcher.MatchesFilter(new Cell("(Blanks)"), hideBlank));
    }

    [Fact]
    public void The_chip_text_says_how_many_values_and_keeps_the_text_match()
    {
        var hide = new ColumnFilterState { ColumnName = "c", ValueMode = ColumnValueMode.Hide, ValueBlank = true };
        hide.Values.Add("a");
        hide.Values.Add("b");
        Assert.Equal("hides 3 values", hide.DisplayText);

        var show = new ColumnFilterState { ColumnName = "c", ValueMode = ColumnValueMode.ShowOnly, Operator = FilterOperator.Contains, Value = "x" };
        show.Values.Add("a");
        Assert.Equal("shows 1 value, Contains 'x'", show.DisplayText);

        Assert.Equal("Contains 'x'", new ColumnFilterState { Value = "x" }.DisplayText);
        Assert.Equal(string.Empty, new ColumnFilterState().DisplayText);
    }

    [Fact]
    public void The_model_lists_the_blanks_entry_first_and_a_value_spelled_Blanks_as_itself()
    {
        var model = new ColumnValueListModel(Catalog("(Blanks)", "x", null), null);

        Assert.Equal(new[] { "(Blanks)", "(Blanks)", "x" }, model.Entries.Select(e => e.Label));
        Assert.True(model.Entries[0].IsBlank);
        Assert.False(model.Entries[1].IsBlank);
    }

    [Fact]
    public void Select_All_under_a_search_ticks_only_what_the_search_shows_even_past_the_cap()
    {
        var model = new ColumnValueListModel(
            ColumnValueCatalog.Build(Enumerable.Range(0, 1500).Select(i => (string?)("x" + i)).Append(null)), null);

        model.Search = "x99";
        Assert.Equal(11, model.Entries.Count);
        Assert.Null(model.Note);

        model.SetAll(false);
        var state = new ColumnFilterState();
        model.ApplyTo(state);
        Assert.Equal(ColumnValueMode.Hide, state.ValueMode);
        Assert.Equal(11, state.Values.Count);
        Assert.False(state.ValueBlank);

        model.Search = "";
        Assert.Equal(1001, model.Entries.Count); // (Blanks) + the first 1,000
        Assert.Equal("Showing 1,000 of 1,500 values. Search to find the rest.", model.Note);
        Assert.Null(model.SelectAllState); // mixed

        model.ToggleSelectAll(); // not all ticked, so Select All ticks every one
        model.ApplyTo(state);
        Assert.Equal(ColumnValueMode.None, state.ValueMode);
        Assert.True(model.SelectAllState);
        model.ToggleSelectAll(); // all ticked, so it unticks every one
        model.ApplyTo(state);
        Assert.True(state.ShowsNoValues);
    }

    [Fact]
    public void Ticking_an_entry_updates_the_state_and_raises_TicksChanged()
    {
        var model = new ColumnValueListModel(Catalog("a", "b", "c"), null);
        var raised = 0;
        model.TicksChanged += (_, _) => raised++;

        model.Entries.Single(e => e.Value == "b").IsTicked = false;

        var state = new ColumnFilterState();
        model.ApplyTo(state);
        Assert.Equal(1, raised);
        Assert.Equal(ColumnValueMode.Hide, state.ValueMode);
        Assert.Equal(new[] { "b" }, state.Values);
        Assert.Null(model.SelectAllState);
    }
}
