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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6391: the Viewer's Job History Server drop-down is built from the registered servers, not from the rows the read returned, so an
/// Azure SQL Database (no SQL Agent, so never any rows) can be picked and the tab says so in the web's plain words; and the Category
/// drop-down lists only the picked server's categories. The tests drive the real tab's own redraw (<c>ApplyChoices</c>) and its real
/// drop-downs.
/// </summary>
public sealed class ViewerJobHistoryServerFilterTests
{
    private const int SqlA = 1;
    private const int SqlB = 2;
    private const int AzureDb = 5;

    private static readonly ViewerDataService.JobHistoryServers Servers = ViewerDataService.ServersOf(
    [
        new DarlingServer(SqlA, "sql-a", "sql-a", true, 16),
        new DarlingServer(SqlB, "sql-b", "sql-b", true, 16),
        new DarlingServer(AzureDb, "azure-db", "azure-db", true, 12, engineEdition: CollectorEngineCapability.AzureSqlDatabaseEngineEdition),
    ]);

    private static void OnTab(Action<JobHistoryTab> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
            try
            {
                body(new JobHistoryTab());
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        });

    private static void Pick(ComboBox combo, string tag) =>
        combo.SelectedIndex = combo.Items.OfType<ComboBoxItem>().ToList().FindIndex(i => i.Tag?.ToString() == tag);

    private static List<string> Texts(ComboBox combo) => combo.Items.OfType<ComboBoxItem>().Select(i => (string)i.Content).ToList();

    [Fact]
    public void ServerDropDown_OffersAnAzureSqlDatabase_WithNoRowsRead()
    {
        OnTab(tab =>
        {
            tab.ApplyChoices(Servers, [], []);

            Assert.Equal(new[] { "All Servers", "azure-db", "sql-a", "sql-b" }, Texts(tab.ServerFilterComboBox));
        });
    }

    [Fact]
    public void PickingAnAzureSqlDatabase_SaysItHasNoSqlAgent_AndPickingAnotherServerGoesBackToTheUsualNote()
    {
        OnTab(tab =>
        {
            tab.ApplyChoices(Servers, [], []);
            Assert.Equal("No job runs recorded in the selected time range", tab.NoJobsMessage.Text);

            Pick(tab.ServerFilterComboBox, AzureDb.ToString());
            Assert.Equal("Azure SQL Database has no SQL Agent, so it has no job history.", tab.NoJobsMessage.Text);

            /* A redraw from a later choices read keeps the sentence for the picked Azure server. */
            tab.ApplyChoices(Servers, [], []);
            Assert.Equal("Azure SQL Database has no SQL Agent, so it has no job history.", tab.NoJobsMessage.Text);

            Pick(tab.ServerFilterComboBox, SqlA.ToString());
            Assert.Equal("No job runs recorded in the selected time range", tab.NoJobsMessage.Text);
        });
    }

    [Fact]
    public void CategoryDropDown_ListsOnlyThePickedServersCategories_AndDropsAnEarlierServersPick()
    {
        OnTab(tab =>
        {
            Pick(tab.ServerFilterComboBox, SqlA.ToString());
            tab.ApplyChoices(Servers, ["Backup", "Maintenance"], []);
            Assert.Equal(new[] { "All Categories", "Backup", "Maintenance" }, Texts(tab.CategoryFilterComboBox));

            Pick(tab.CategoryFilterComboBox, "Backup");
            Assert.Equal(1, tab.CategoryFilterComboBox.SelectedIndex);

            /* The other server: its read has not arrived yet, so the earlier server's categories are gone and so is the pick. */
            Pick(tab.ServerFilterComboBox, SqlB.ToString());
            Assert.Equal(0, tab.CategoryFilterComboBox.SelectedIndex);
            tab.ApplyChoices(Servers, ["Reports"], []);
            Assert.Equal(new[] { "All Categories", "Reports" }, Texts(tab.CategoryFilterComboBox));

            /* An Azure SQL Database has no categories at all. */
            Pick(tab.ServerFilterComboBox, AzureDb.ToString());
            tab.ApplyChoices(Servers, [], []);
            Assert.Equal(new[] { "All Categories" }, Texts(tab.CategoryFilterComboBox));

            /* All Servers lists the fleet's. */
            tab.ServerFilterComboBox.SelectedIndex = 0;
            tab.ApplyChoices(Servers, ["Backup", "Reports"], []);
            Assert.Equal(new[] { "All Categories", "Backup", "Reports" }, Texts(tab.CategoryFilterComboBox));
        });
    }
}
