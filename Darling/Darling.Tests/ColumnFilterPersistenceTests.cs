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
using System.Threading;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Column filters kept across a refresh and a restart (#5565): the store file (round trip, per-server separation,
/// least-recently-used bound, a bad file ignored, a cleared filter removed, no row data), and the grid manager that
/// loads them on the first scoped refresh, keeps them across UpdateData, and clears them on "Clear all filters".
/// </summary>
public sealed class ColumnFilterPersistenceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "colfilter-" + Guid.NewGuid().ToString("N"));
    private string FilePath => Path.Combine(_dir, "column-filters.json");

    public ColumnFilterPersistenceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        ColumnFilterStore.Current = null;
        try { Directory.Delete(_dir, true); } catch (IOException) { }
    }

    private ColumnFilterStore NewStore(List<string>? warnings = null) =>
        new(FilePath, warnings is null ? null : warnings.Add, TimeSpan.FromHours(1));

    private static ColumnFilterState Hide(string column, params string[] values)
    {
        var state = new ColumnFilterState { ColumnName = column, ValueMode = ColumnValueMode.Hide };
        foreach (var v in values) state.Values.Add(v);
        return state;
    }

    [Fact]
    public void A_filter_saved_comes_back_after_a_restart_with_its_mode_values_and_text_match()
    {
        var first = NewStore();
        var saved = Hide("LoginName", "job_svc", "NT AUTHORITY\\SYSTEM");
        saved.ValueBlank = true;
        saved.Operator = FilterOperator.StartsWith;
        saved.Value = "sa";
        first.Save("srv-a", "QuerySnapshotsGrid", new[] { saved });
        first.Flush();

        var restarted = NewStore();
        var loaded = Assert.Single(restarted.Load("srv-a", "QuerySnapshotsGrid"));

        Assert.Equal("LoginName", loaded.ColumnName);
        Assert.Equal(ColumnValueMode.Hide, loaded.ValueMode);
        Assert.True(loaded.Values.SetEquals(new[] { "job_svc", "NT AUTHORITY\\SYSTEM" }));
        Assert.True(loaded.ValueBlank);
        Assert.Equal(FilterOperator.StartsWith, loaded.Operator);
        Assert.Equal("sa", loaded.Value);
    }

    [Fact]
    public void Filters_are_kept_apart_per_server_and_per_grid()
    {
        var store = NewStore();
        store.Save("srv-a", "GridOne", new[] { Hide("LoginName", "a") });
        store.Save("srv-b", "GridOne", new[] { Hide("LoginName", "b") });
        store.Save("srv-a", "GridTwo", new[] { Hide("LoginName", "c") });
        store.Flush();

        var restarted = NewStore();
        Assert.Equal("a", Assert.Single(restarted.Load("srv-a", "GridOne")).Values.Single());
        Assert.Equal("b", Assert.Single(restarted.Load("srv-b", "GridOne")).Values.Single());
        Assert.Equal("c", Assert.Single(restarted.Load("srv-a", "GridTwo")).Values.Single());
        Assert.Empty(restarted.Load("srv-c", "GridOne"));
    }

    [Fact]
    public void A_cleared_filter_is_removed_from_the_file()
    {
        var store = NewStore();
        store.Save("srv-a", "GridOne", new[] { Hide("LoginName", "a") });
        store.Flush();
        store.Save("srv-a", "GridOne", new[] { new ColumnFilterState { ColumnName = "LoginName" } });
        store.Flush();

        Assert.Empty(NewStore().Load("srv-a", "GridOne"));
        Assert.DoesNotContain("srv-a", File.ReadAllText(FilePath), StringComparison.Ordinal);
    }

    [Fact]
    public void Only_the_200_most_recently_used_grids_are_kept()
    {
        var store = NewStore();
        for (var i = 0; i < 205; i++)
            store.Save("srv", "Grid" + i, new[] { Hide("c", "v") });
        store.Load("srv", "Grid0"); // not evicted yet? it was: the first five went when 201..205 arrived
        store.Flush();

        var restarted = NewStore();
        Assert.Empty(restarted.Load("srv", "Grid0"));
        Assert.Empty(restarted.Load("srv", "Grid4"));
        Assert.Single(restarted.Load("srv", "Grid5"));
        Assert.Single(restarted.Load("srv", "Grid204"));
    }

    [Fact]
    public void Using_a_grid_keeps_it_when_older_ones_are_dropped()
    {
        var store = NewStore();
        for (var i = 0; i < 200; i++)
            store.Save("srv", "Grid" + i, new[] { Hide("c", "v") });
        store.Load("srv", "Grid0"); // the oldest becomes the newest
        store.Save("srv", "Extra", new[] { Hide("c", "v") }); // 201st: Grid1 goes, not Grid0
        store.Flush();

        var restarted = NewStore();
        Assert.Single(restarted.Load("srv", "Grid0"));
        Assert.Empty(restarted.Load("srv", "Grid1"));
        Assert.Single(restarted.Load("srv", "Extra"));
    }

    [Fact]
    public void A_column_naming_more_than_1000_values_is_not_stored_and_1000_is()
    {
        var store = NewStore();
        var big = Hide("Big", Enumerable.Range(0, 1001).Select(i => "v" + i).ToArray());
        var edge = Hide("Edge", Enumerable.Range(0, 1000).Select(i => "v" + i).ToArray());
        store.Save("srv", "Grid", new[] { big, edge });
        store.Flush();

        var loaded = NewStore().Load("srv", "Grid");
        Assert.Equal("Edge", Assert.Single(loaded).ColumnName);
    }

    [Theory]
    [InlineData("this is not json {")]
    [InlineData("{\"version\": 99, \"grids\": []}")]
    [InlineData("[1,2,3]")]
    [InlineData("")]
    public void A_file_that_cannot_be_read_is_reported_once_and_ignored(string content)
    {
        File.WriteAllText(FilePath, content);
        var warnings = new List<string>();
        var store = NewStore(warnings);

        Assert.Empty(store.Load("srv", "Grid"));
        Assert.Empty(store.Load("srv", "Grid"));
        store.Save("srv", "Grid", new[] { Hide("c", "v") });
        Assert.Single(store.Load("srv", "Grid"));
        Assert.Single(warnings);
    }

    [Fact]
    public void A_missing_file_is_a_first_run_and_says_nothing()
    {
        var warnings = new List<string>();
        Assert.Empty(NewStore(warnings).Load("srv", "Grid"));
        Assert.Empty(warnings);
    }

    [Fact]
    public void The_file_holds_the_ticked_values_and_the_keys_and_no_row_data()
    {
        var store = NewStore();
        store.Save("srv-a", "QuerySnapshotsGrid", new[] { Hide("LoginName", "job_svc") });
        store.Flush();

        var text = File.ReadAllText(FilePath);
        Assert.Contains("job_svc", text, StringComparison.Ordinal);
        Assert.Contains("QuerySnapshotsGrid", text, StringComparison.Ordinal);
        Assert.False(File.Exists(FilePath + ".tmp"), "the temp file is replaced away");
    }

    [Fact]
    public void Writes_are_debounced_until_the_flush()
    {
        var store = NewStore(); // an hour's debounce
        store.Save("srv", "Grid", new[] { Hide("c", "v") });

        Assert.False(File.Exists(FilePath));
        store.Flush();
        Assert.True(File.Exists(FilePath));
    }

    [Fact]
    public void Writes_replace_the_old_file_in_one_step()
    {
        var store = NewStore();
        store.Save("srv", "One", new[] { Hide("c", "v") });
        store.Flush();
        store.Save("srv", "Two", new[] { Hide("c", "v") });
        store.Flush();

        var restarted = NewStore();
        Assert.Single(restarted.Load("srv", "One"));
        Assert.Single(restarted.Load("srv", "Two"));
        Assert.False(File.Exists(FilePath + ".tmp"));
    }

    // ---- the grid manager -------------------------------------------------

    private sealed class LoginRow
    {
        public string? LoginName { get; set; }
        public int SessionId { get; set; }
        public string? QueryText { get; set; }
        public string? Detail { get; set; }
    }

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        using var staGate = WpfStaGate.Enter();
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
            throw error;
        return result;
    }

    private static DataGrid NewGrid(string? name, string? server)
    {
        var grid = new DataGrid { AutoGenerateColumns = false };
        if (name is not null) grid.Name = name;
        if (server is not null) ColumnFilterScope.SetServer(grid, server);
        foreach (var path in new[] { "LoginName", "SessionId", "QueryText" })
            grid.Columns.Add(new DataGridTextColumn { Header = path, Binding = new Binding(path) });
        return grid;
    }

    private static List<LoginRow> Rows(params string?[] logins) =>
        logins.Select((l, i) => new LoginRow { LoginName = l, SessionId = i }).ToList();

    private static List<string?> Shown(DataGrid grid) =>
        grid.Items.OfType<LoginRow>().Select(r => r.LoginName).ToList();

    private static void Untick(IDataGridFilterManager manager, string column, string value)
    {
        var model = new ColumnValueListModel(manager.GetValueCatalog(column)!, manager.Filters.GetValueOrDefault(column));
        model.Entries.Single(e => e.Value == value).IsTicked = false;
        var state = new ColumnFilterState { ColumnName = column };
        model.ApplyTo(state);
        manager.SetFilter(state);
    }

    [Fact]
    public void A_filter_survives_a_refresh_and_a_value_that_first_appears_later_still_shows()
    {
        OnStaThread(() =>
        {
            var grid = NewGrid("G", null);
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "app", "job_svc"));
            Untick(manager, "LoginName", "job_svc");

            Assert.Equal(new string?[] { "sa", "app" }, Shown(grid));

            manager.UpdateData(Rows("sa", "app", "job_svc", "newuser"));
            Assert.Equal(new string?[] { "sa", "app", "newuser" }, Shown(grid));
            Assert.True(manager.AnyFilterActive);
            return true;
        });
    }

    [Fact]
    public void The_value_list_comes_from_the_unfiltered_rows_not_the_ones_shown()
    {
        OnStaThread(() =>
        {
            var grid = NewGrid("G", null);
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "app", "job_svc"));
            Untick(manager, "LoginName", "job_svc");

            var catalog = manager.GetValueCatalog("LoginName")!;
            Assert.Equal(new[] { "app", "job_svc", "sa" }, catalog.Values);
            return true;
        });
    }

    [Fact]
    public void Number_query_text_and_prose_columns_and_columns_the_row_lacks_get_no_list()
    {
        OnStaThread(() =>
        {
            var grid = NewGrid("G", null);
            var manager = new DataGridFilterManager<LoginRow>(grid);
            Assert.Null(manager.GetValueCatalog("LoginName")); // no rows yet
            manager.UpdateData(Rows("sa"));

            Assert.NotNull(manager.GetValueCatalog("LoginName"));
            Assert.Null(manager.GetValueCatalog("SessionId"));
            Assert.Null(manager.GetValueCatalog("QueryText"));
            Assert.Null(manager.GetValueCatalog("Detail"));
            Assert.Null(manager.GetValueCatalog("NoSuchColumn"));
            return true;
        });
    }

    [Fact]
    public void A_text_column_that_sorts_by_another_member_gets_no_list()
    {
        OnStaThread(() =>
        {
            var grid = NewGrid("G", null);
            ((DataGridTextColumn)grid.Columns[0]).SortMemberPath = "SessionId";
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa"));

            Assert.Null(manager.GetValueCatalog("LoginName"));
            return true;
        });
    }

    [Fact]
    public void A_column_with_a_value_over_256_characters_gets_no_list()
    {
        OnStaThread(() =>
        {
            var grid = NewGrid("G", null);
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", new string('x', 257)));

            Assert.Null(manager.GetValueCatalog("LoginName"));
            return true;
        });
    }

    [Fact]
    public void A_filter_set_in_one_session_is_loaded_by_the_next_on_its_first_refresh()
    {
        var shown = OnStaThread(() =>
        {
            var store = NewStore();
            ColumnFilterStore.Current = store;
            var grid = NewGrid("QuerySnapshotsGrid", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "app", "job_svc"));
            Untick(manager, "LoginName", "job_svc");
            store.Flush();
            store.Dispose();

            /* a restart: a new store over the same file, a new grid, a new manager */
            var restarted = NewStore();
            ColumnFilterStore.Current = restarted;
            var grid2 = NewGrid("QuerySnapshotsGrid", "srv-a");
            var manager2 = new DataGridFilterManager<LoginRow>(grid2);
            manager2.UpdateData(Rows("sa", "app", "job_svc", "newuser"));
            return Shown(grid2);
        });

        Assert.Equal(new string?[] { "sa", "app", "newuser" }, shown);
    }

    [Fact]
    public void Another_servers_filters_do_not_apply_and_a_grid_without_a_name_or_scope_is_not_stored()
    {
        OnStaThread(() =>
        {
            var store = NewStore();
            ColumnFilterStore.Current = store;

            var grid = NewGrid("G", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "job_svc"));
            Untick(manager, "LoginName", "job_svc");

            var other = NewGrid("G", "srv-b");
            var otherManager = new DataGridFilterManager<LoginRow>(other);
            otherManager.UpdateData(Rows("sa", "job_svc"));
            Assert.Equal(new string?[] { "sa", "job_svc" }, Shown(other));

            var unnamed = NewGrid(null, "srv-a");
            var unnamedManager = new DataGridFilterManager<LoginRow>(unnamed);
            unnamedManager.UpdateData(Rows("sa", "job_svc"));
            Untick(unnamedManager, "LoginName", "job_svc");

            var unscoped = NewGrid("G", null);
            var unscopedManager = new DataGridFilterManager<LoginRow>(unscoped);
            unscopedManager.UpdateData(Rows("sa", "job_svc"));
            Untick(unscopedManager, "LoginName", "job_svc");

            Assert.Single(store.Load("srv-a", "G"));
            Assert.Empty(store.Load("srv-a", ""));
            return true;
        });
    }

    [Fact]
    public void A_stored_filter_on_a_column_the_grid_no_longer_has_is_ignored()
    {
        OnStaThread(() =>
        {
            var store = NewStore();
            store.Save("srv-a", "G", new[] { Hide("RetiredColumn", "x"), Hide("LoginName", "job_svc") });
            ColumnFilterStore.Current = store;

            var grid = NewGrid("G", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "job_svc"));

            Assert.Equal(new[] { "LoginName" }, manager.Filters.Keys);
            Assert.Equal(new string?[] { "sa" }, Shown(grid));
            return true;
        });
    }

    [Fact]
    public void Clear_all_filters_clears_the_grid_and_the_stored_copy_but_a_server_switch_does_not()
    {
        OnStaThread(() =>
        {
            var store = NewStore();
            ColumnFilterStore.Current = store;
            var grid = NewGrid("G", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "job_svc"));
            Untick(manager, "LoginName", "job_svc");

            manager.ClearFilters(); // #2306: a context change
            Assert.Equal(2, Shown(grid).Count);
            Assert.Single(store.Load("srv-a", "G")); // still stored for that server

            manager.UpdateData(Rows("sa", "job_svc")); // the next refresh loads what is stored for the scope
            Assert.Equal(new string?[] { "sa" }, Shown(grid));

            manager.ClearAllFilters(); // the user
            Assert.Equal(2, Shown(grid).Count);
            Assert.False(manager.AnyFilterActive);
            Assert.Empty(store.Load("srv-a", "G"));

            manager.UpdateData(Rows("sa", "job_svc"));
            Assert.Equal(2, Shown(grid).Count);
            return true;
        });
    }

    [Fact]
    public void A_server_switch_loads_the_new_servers_own_filters()
    {
        OnStaThread(() =>
        {
            var store = NewStore();
            store.Save("srv-b", "G", new[] { Hide("LoginName", "sa") });
            ColumnFilterStore.Current = store;
            var grid = NewGrid("G", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            manager.UpdateData(Rows("sa", "job_svc"));
            Assert.Equal(2, Shown(grid).Count);

            ColumnFilterScope.SetServer(grid, "srv-b"); // the FinOps selector changes server
            manager.ClearFilters();
            manager.UpdateData(Rows("sa", "job_svc"));

            Assert.Equal(new string?[] { "job_svc" }, Shown(grid));
            return true;
        });
    }

    [Fact]
    public void A_store_that_fails_never_blocks_the_grid()
    {
        OnStaThread(() =>
        {
            File.WriteAllText(FilePath, "garbage");
            ColumnFilterStore.Current = NewStore();
            var grid = NewGrid("G", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);

            manager.UpdateData(Rows("sa", "job_svc"));
            Untick(manager, "LoginName", "job_svc");

            Assert.Equal(new string?[] { "sa" }, Shown(grid));
            return true;
        });
    }
}
