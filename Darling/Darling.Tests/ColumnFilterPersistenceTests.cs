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
    public void A_column_naming_more_than_1000_values_keeps_its_text_match_and_loses_only_the_value_part()
    {
        var store = NewStore();
        var big = Hide("Big", Enumerable.Range(0, 1001).Select(i => "v" + i).ToArray());
        big.ValueBlank = true;
        big.Value = "v1";
        var bigNoText = Hide("BigNoText", Enumerable.Range(0, 1001).Select(i => "v" + i).ToArray());
        var edge = Hide("Edge", Enumerable.Range(0, 1000).Select(i => "v" + i).ToArray());
        store.Save("srv", "Grid", new[] { big, bigNoText, edge });
        store.Flush();

        var loaded = NewStore().Load("srv", "Grid");

        /* the web page does the same: a cut set would change what it hides, but the text match fits and stays */
        Assert.Equal(new[] { "Big", "Edge" }, loaded.Select(f => f.ColumnName).OrderBy(n => n));
        var kept = loaded.Single(f => f.ColumnName == "Big");
        Assert.Equal(ColumnValueMode.None, kept.ValueMode);
        Assert.Empty(kept.Values);
        Assert.False(kept.ValueBlank);
        Assert.Equal("v1", kept.Value);
        Assert.Equal(1000, loaded.Single(f => f.ColumnName == "Edge").Values.Count);
    }

    [Fact]
    public void A_text_match_on_a_column_that_gets_no_list_is_never_written_but_one_on_a_list_column_is()
    {
        var store = NewStore();
        var statement = new ColumnFilterState { ColumnName = "QueryText", Value = "salary > 90000" };
        var script = new ColumnFilterState { ColumnName = "ImplementationScript", Value = "force_plan" };
        var error = new ColumnFilterState { ColumnName = "LastError", Value = "login failed for" };
        var login = new ColumnFilterState { ColumnName = "LoginName", Value = "svc_" };
        store.Save("srv", "Grid", new[] { statement, script, error, login });
        store.Flush();

        var text = File.ReadAllText(FilePath);
        Assert.DoesNotContain("salary", text, StringComparison.Ordinal);
        Assert.DoesNotContain("force_plan", text, StringComparison.Ordinal);
        Assert.DoesNotContain("login failed", text, StringComparison.Ordinal);
        Assert.Equal("LoginName", Assert.Single(NewStore().Load("srv", "Grid")).ColumnName);

        /* and one planted in the file is not loaded either */
        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":[{\"Server\":\"s\",\"Grid\":\"g\",\"Columns\":[" +
            "{\"Column\":\"QueryText\",\"Operator\":\"Contains\",\"Value\":\"x\"},{\"Column\":\"LoginName\",\"Operator\":\"Contains\",\"Value\":\"y\"}]}]}");
        Assert.Equal("LoginName", Assert.Single(NewStore().Load("s", "g")).ColumnName);
    }

    [Fact]
    public void The_stored_limits_apply_when_the_file_is_read_too()
    {
        var many = string.Join(",", Enumerable.Range(0, 1001).Select(i => "\"v" + i + "\""));
        var longValue = new string('x', ColumnFilterStore.MaxValueLength + 1);
        var grids = new List<string>
        {
            /* too many values: the value part goes, the text match stays */
            "{\"Server\":\"s\",\"Grid\":\"many\",\"Columns\":[{\"Column\":\"LoginName\",\"Operator\":\"Contains\",\"Value\":\"t\",\"ValueMode\":\"Hide\",\"Values\":[" + many + "]}]}",
            /* a value too long: the same */
            "{\"Server\":\"s\",\"Grid\":\"longvalue\",\"Columns\":[{\"Column\":\"LoginName\",\"Value\":\"t\",\"ValueMode\":\"Hide\",\"Values\":[\"" + longValue + "\"]}]}",
            /* a text match too long: it goes, the value part stays */
            "{\"Server\":\"s\",\"Grid\":\"longtext\",\"Columns\":[{\"Column\":\"LoginName\",\"Value\":\"" + longValue + "\",\"ValueMode\":\"Hide\",\"Values\":[\"a\"]}]}",
            /* a mode and an operator this version does not know: not match-all */
            "{\"Server\":\"s\",\"Grid\":\"badenum\",\"Columns\":[{\"Column\":\"LoginName\",\"Operator\":\"99\",\"ValueMode\":\"99\",\"Values\":[\"a\"]}]}",
            /* a column name that is too long */
            "{\"Server\":\"s\",\"Grid\":\"longcolumn\",\"Columns\":[{\"Column\":\"" + longValue + "\",\"Value\":\"t\"}]}",
        };
        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":[" + string.Join(",", grids) + "]}");
        var warnings = new List<string>();
        var store = NewStore(warnings);

        var many1 = Assert.Single(store.Load("s", "many"));
        Assert.Equal(ColumnValueMode.None, many1.ValueMode);
        Assert.Equal("t", many1.Value);
        var longv = Assert.Single(store.Load("s", "longvalue"));
        Assert.Equal(ColumnValueMode.None, longv.ValueMode);
        Assert.Equal("t", longv.Value);
        var longt = Assert.Single(store.Load("s", "longtext"));
        Assert.Equal("", longt.Value);
        Assert.Equal(ColumnValueMode.Hide, longt.ValueMode);
        Assert.Empty(store.Load("s", "badenum"));
        Assert.Empty(store.Load("s", "longcolumn"));
        Assert.Empty(warnings);
    }

    [Fact]
    public void A_file_with_more_than_200_grids_or_200_columns_keeps_only_the_newest_grids_and_the_first_columns()
    {
        var grids = Enumerable.Range(0, 300).Select(i =>
            "{\"Server\":\"s\",\"Grid\":\"g" + i + "\",\"Columns\":[{\"Column\":\"LoginName\",\"Value\":\"t\"}]}");
        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":[" + string.Join(",", grids) + "]}");
        var store = NewStore();

        Assert.Empty(store.Load("s", "g99"));
        Assert.Single(store.Load("s", "g100"));
        Assert.Single(store.Load("s", "g299"));

        var columns = string.Join(",", Enumerable.Range(0, 250).Select(i => "{\"Column\":\"C" + i + "\",\"Value\":\"t\"}"));
        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":[{\"Server\":\"s\",\"Grid\":\"wide\",\"Columns\":[" + columns + "]}]}");
        Assert.Equal(ColumnFilterStore.MaxColumnsPerGrid, NewStore().Load("s", "wide").Count);
    }

    [Fact]
    public void A_null_or_wrong_typed_entry_is_skipped_and_the_rest_of_the_file_is_used()
    {
        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":[null,5,\"x\",[1]," +
            "{\"Server\":\"s\",\"Grid\":\"g\",\"Columns\":[null,7,\"x\",{\"Column\":5},{\"Column\":\"LoginName\",\"Value\":\"ok\",\"Values\":[null,1]}," +
            "{\"Column\":\"HostName\",\"Value\":\"h\",\"ValueMode\":\"Hide\",\"Values\":[\"a\",null]}]}," +
            "{\"Server\":5,\"Grid\":\"g2\",\"Columns\":[]},{\"Server\":\"s\",\"Grid\":\"g3\",\"Columns\":null}]}");
        var warnings = new List<string>();
        var store = NewStore(warnings);

        var loaded = store.Load("s", "g");
        Assert.Equal(new[] { "HostName", "LoginName" }, loaded.Select(f => f.ColumnName).OrderBy(n => n));
        Assert.Equal("ok", loaded.Single(f => f.ColumnName == "LoginName").Value);
        /* a null among the values makes that value part unusable, not the column */
        Assert.Equal(ColumnValueMode.None, loaded.Single(f => f.ColumnName == "HostName").ValueMode);
        Assert.Empty(store.Load("s", "g2"));
        Assert.Empty(store.Load("s", "g3"));
        Assert.Empty(warnings);
    }

    [Fact]
    public void A_file_over_the_size_limit_or_nested_too_deep_is_ignored_whole_with_one_warning()
    {
        using (var stream = new FileStream(FilePath, FileMode.Create))
            stream.SetLength(ColumnFilterStore.MaxFileBytes + 1);
        var warnings = new List<string>();
        var big = NewStore(warnings);
        Assert.Empty(big.Load("s", "g"));
        Assert.Empty(big.Load("s", "g"));
        Assert.Single(warnings);

        File.WriteAllText(FilePath, "{\"Version\":1,\"Grids\":" + new string('[', 100) + new string(']', 100) + "}");
        warnings.Clear();
        var deep = NewStore(warnings);
        Assert.Empty(deep.Load("s", "g"));
        Assert.Single(warnings);
    }

    [Fact]
    public void A_failed_save_is_retried_on_the_next_tick_and_warned_once_per_run()
    {
        var warnings = new List<string>();
        var store = new ColumnFilterStore(FilePath, warnings.Add, TimeSpan.FromMilliseconds(50));
        Directory.CreateDirectory(FilePath + ".tmp"); // the temp file cannot be written
        store.Save("srv", "Grid", new[] { Hide("c", "v") });

        store.Flush();
        store.Flush();
        store.Flush();
        Assert.Single(warnings);
        Assert.False(File.Exists(FilePath));

        Directory.Delete(FilePath + ".tmp"); // the cause goes away: the debounce timer's own tick writes the file
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!File.Exists(FilePath) && DateTime.UtcNow < deadline)
            Thread.Sleep(50);

        Assert.True(File.Exists(FilePath), "the failed save was never retried");
        Assert.Single(warnings);
        Assert.Single(NewStore().Load("srv", "Grid"));
        store.Dispose();
    }

    [Fact]
    public void A_write_goes_through_a_temp_file_so_a_failed_write_leaves_the_old_file_whole()
    {
        var warnings = new List<string>();
        var store = NewStore(warnings);
        store.Save("srv", "One", new[] { Hide("c", "v") });
        store.Flush();
        var before = File.ReadAllBytes(FilePath);

        /* the temp file cannot be written: a write straight into the real file would change it, replacing the temp file does not */
        Directory.CreateDirectory(FilePath + ".tmp");
        store.Save("srv", "Two", new[] { Hide("c", "w") });
        store.Flush();

        Assert.Single(warnings);
        Assert.Equal(before, File.ReadAllBytes(FilePath));
        Assert.Single(NewStore().Load("srv", "One"));
        Assert.Empty(NewStore().Load("srv", "Two"));
    }

    [Fact]
    public void A_file_left_in_the_old_place_is_moved_to_the_new_one_once_and_deleted()
    {
        var legacy = Path.Combine(_dir, "roaming", "column-filters.json");
        Directory.CreateDirectory(Path.GetDirectoryName(legacy)!);
        var seed = NewStore();
        seed.Save("srv", "Grid", new[] { Hide("c", "v") });
        seed.Flush();
        File.Move(FilePath, legacy);

        var target = Path.Combine(_dir, "local", "column-filters.json");
        ColumnFilterStore.Install(target, null, legacy);
        try
        {
            Assert.False(File.Exists(legacy));
            Assert.True(File.Exists(target));
            Assert.Single(ColumnFilterStore.Current!.Load("srv", "Grid"));
        }
        finally
        {
            ColumnFilterStore.Current = null;
        }

        /* when the new place already has a file, the old one is only deleted */
        File.WriteAllText(legacy, "{\"Version\":1,\"Grids\":[]}");
        ColumnFilterStore.MigrateLegacyFile(legacy, target);
        Assert.False(File.Exists(legacy));
        Assert.True(File.Exists(target));
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
    public void A_text_match_on_a_query_text_column_lasts_the_session_but_is_not_stored()
    {
        var kept = OnStaThread(() =>
        {
            var store = NewStore();
            ColumnFilterStore.Current = store;
            var grid = NewGrid("QuerySnapshotsGrid", "srv-a");
            var manager = new DataGridFilterManager<LoginRow>(grid);
            var rows = new List<LoginRow>
            {
                new() { LoginName = "sa", QueryText = "select salary from pay" },
                new() { LoginName = "app", QueryText = "select 1" },
            };
            manager.UpdateData(rows);
            manager.SetFilter(new ColumnFilterState { ColumnName = "QueryText", Value = "salary" });
            manager.SetFilter(new ColumnFilterState { ColumnName = "LoginName", Value = "sa" });

            /* a refresh keeps both for the session */
            manager.UpdateData(rows);
            var afterRefresh = Shown(grid);
            store.Flush();
            store.Dispose();

            var restarted = NewStore();
            ColumnFilterStore.Current = restarted;
            var again = new DataGridFilterManager<LoginRow>(NewGrid("QuerySnapshotsGrid", "srv-a"));
            again.UpdateData(rows);
            return (afterRefresh, again.Filters.Keys.OrderBy(k => k).ToList());
        });

        Assert.Equal(new string?[] { "sa" }, kept.afterRefresh);
        Assert.Equal(new[] { "LoginName" }, kept.Item2);
        Assert.DoesNotContain("salary", File.ReadAllText(FilePath), StringComparison.Ordinal);
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
    public void A_cross_server_list_under_the_all_servers_scope_keeps_its_filters_across_a_restart()
    {
        var shown = OnStaThread(() =>
        {
            var store = NewStore();
            ColumnFilterStore.Current = store;
            var manager = new DataGridFilterManager<LoginRow>(NewGrid("AlertsDataGrid", ColumnFilterScope.AllServers));
            manager.UpdateData(Rows("sa", "app", "job_svc"));
            Untick(manager, "LoginName", "job_svc");
            store.Flush();
            store.Dispose();

            ColumnFilterStore.Current = NewStore();
            var grid = NewGrid("AlertsDataGrid", ColumnFilterScope.AllServers);
            new DataGridFilterManager<LoginRow>(grid).UpdateData(Rows("sa", "app", "job_svc"));
            return Shown(grid);
        });

        Assert.Equal(new string?[] { "sa", "app" }, shown);
        Assert.StartsWith("\u0001", ColumnFilterScope.AllServers, StringComparison.Ordinal);
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
