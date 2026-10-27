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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6823: the Add / Edit server dialog opened at its default width with the Save button pushed out of view. The footer
/// row held Test Connection, Copy grant script, the FinOps check box, Cancel and Save in one DockPanel, wider than the
/// 450 px window, and a DockPanel clips its last children first. The dialog is shown at its default width and every
/// mode of it (Add and Edit, SQL Server and PostgreSQL) must have Save and Cancel fully inside the window.
/// </summary>
public sealed class AddServerDialogFooterTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    public static IEnumerable<object[]> Modes()
    {
        yield return new object[] { "add-sqlserver" };
        yield return new object[] { "add-postgres" };
        yield return new object[] { "edit-sqlserver" };
        yield return new object[] { "edit-postgres" };
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public void SaveAndCancel_AreFullyInsideTheDialog_AtItsDefaultWidth(string mode)
    {
        /* Runs on the shared STA thread with the app's Dark theme merged, as App.xaml supplies it (the dialog's Save button
           takes its style from it), and takes it back out afterwards. */
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var theme = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(theme);
            try
            {
                AssertFooterInside(mode);
            }
            finally
            {
                app.Resources.MergedDictionaries.Remove(theme);
            }
        });
    }

    private static void AssertFooterInside(string mode)
    {
        using var dir = new ScratchDir();
        var secrets = new NoSecrets();
        var servers = new ViewerServerStore(Path.Combine(dir.Path, "servers.json"), secrets);
        var profiles = new ViewerProfileStore(servers, Path.Combine(dir.Path, "profiles.json"), secrets);

        AddServerDialog dialog;
        if (mode.StartsWith("edit-", StringComparison.Ordinal))
        {
            /* The data service only builds a connection pool object; nothing here opens a connection. */
            var data = new ViewerDataService("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x");
            var row = new MonitoredServerRow
            {
                ServerId = 7,
                Name = "Edited",
                Host = "edited-host",
                Engine = mode == "edit-postgres" ? MonitoredServerRow.EnginePostgres : MonitoredServerRow.EngineSqlServer,
                Auth = ServerStoreCredential.Sql,
                Username = "monitor",
            };
            dialog = new AddServerDialog(data, servers, profiles, row, isFavorite: false);
        }
        else
        {
            dialog = new AddServerDialog(null, servers, profiles);
            if (mode == "add-postgres")
            {
                dialog.PostgresEngineRadio.IsChecked = true;
            }
        }

        try
        {
            StaTestThread.Settle(dialog);

            var content = Assert.IsAssignableFrom<FrameworkElement>(dialog.Content);
            Assert.Equal(450d, dialog.ActualWidth, precision: 0);

            var cancel = Descendants<Button>(dialog).Single(b => b.IsCancel);
            foreach (var button in new[] { dialog.SaveButton, cancel })
            {
                Assert.True(button.IsVisible, $"{mode}: {button.Content} is not visible");
                Assert.Equal(80d, button.ActualWidth, precision: 1);
                Assert.Equal(30d, button.ActualHeight, precision: 1);

                var bounds = button.TransformToAncestor(content).TransformBounds(new Rect(0, 0, button.ActualWidth, button.ActualHeight));
                Assert.True(bounds.Left >= -0.5 && bounds.Right <= content.ActualWidth + 0.5,
                    $"{mode}: {button.Content} spans {bounds.Left:0.#}..{bounds.Right:0.#} of a {content.ActualWidth:0.#} px window");
                Assert.True(bounds.Top >= -0.5 && bounds.Bottom <= content.ActualHeight + 0.5,
                    $"{mode}: {button.Content} spans {bounds.Top:0.#}..{bounds.Bottom:0.#} of a {content.ActualHeight:0.#} px window");
            }
        }
        finally
        {
            dialog.Close();
        }
    }

    private static IEnumerable<T> Descendants<T>(DependencyObject parent) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var nested in Descendants<T>(child))
            {
                yield return nested;
            }
        }
    }

    private sealed class ScratchDir : IDisposable
    {
        public string Path { get; } = Directory.CreateTempSubdirectory("darling-6823-").FullName;
        public void Dispose() { try { Directory.Delete(Path, recursive: true); } catch { /* best-effort */ } }
    }

    private sealed class NoSecrets : IViewerServerSecretStore
    {
        public void Save(string id, string username, string password) { }
        public (string Username, string Password)? Find(string id) => null;
        public void Delete(string id) { }
    }
}
