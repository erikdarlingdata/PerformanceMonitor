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
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// When the Excluded Databases or XE Databases dialog cannot read a server's database list, the dialog says so once, in its own
/// status line, in plain words: the server can't be reached, or the login was refused, or the list could not be read and the log
/// has the details. A refused login with no database set is a failure, not a one-row list of master. The exception's own text stays out of the dialog, and no message box opens: the load runs on the test's
/// thread, and a modal box there would stop the test from returning. The reader is replaced so no server is needed to fail.
/// </summary>
[Collection("credential-manager")]
public sealed class ExcludedDatabasesDialogLoadFailureTests
{
    private const string RawText = "A network-related or instance-specific error occurred while establishing a connection to SQL Server.";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AnUnreachableServer_ReadsAsAPlainReasonInTheStatusLine_WithNoRawExceptionText(bool xeSessionsMode)
    {
        var status = StatusAfterFailedLoad(xeSessionsMode, new SocketException((int)SocketError.TimedOut));

        Assert.StartsWith("Reporting replica can't be reached, so its database list can't load.", status, StringComparison.Ordinal);
        Assert.DoesNotContain("network-related", status, StringComparison.Ordinal);
        Assert.DoesNotContain("Failed to load", status, StringComparison.Ordinal);
    }

    [Fact]
    public void AnyOtherFailure_PointsAtTheLog_WithoutTheExceptionText()
    {
        var status = StatusAfterFailedLoad(xeSessionsMode: false, new InvalidOperationException(RawText));

        Assert.Equal("The database list for Reporting replica could not be read. The log has the details.", status);
    }

    [Fact]
    public void TheWording_TellsAnUnreachableServerFromARefusedLogin()
    {
        Assert.StartsWith("db-01 can't be reached", ExcludedDatabasesDialog.LoadFailureText("db-01", new TimeoutException("timed out")), StringComparison.Ordinal);
        Assert.StartsWith("db-01 can't be reached", ExcludedDatabasesDialog.LoadFailureText("db-01", new InvalidOperationException("wrapped", new SocketException((int)SocketError.HostNotFound))), StringComparison.Ordinal);
        Assert.Equal("The database list for db-01 could not be read. The log has the details.", ExcludedDatabasesDialog.LoadFailureText("db-01", new InvalidOperationException(RawText)));
    }

    [Fact]
    public void ARefusedLogin_WithNoDatabaseSet_ReadsAsRefused_AndListsNoMasterRow()
    {
        var (status, listed) = StatusAfterRealRead(databaseName: null, SqlExceptionFactory.Create(18456, 14, "Login failed for user 'monitor'."));

        Assert.StartsWith("Reporting replica refused the login, so its database list can't load.", status, StringComparison.Ordinal);
        Assert.Empty(listed);
    }

    [Fact]
    public void ARefusedMasterRead_WithAUserDatabaseSet_FallsBackToThatDatabase()
    {
        var (status, listed) = StatusAfterRealRead("Sales", SqlExceptionFactory.Create(18456, 14, "Login failed for user 'monitor'."));

        Assert.StartsWith("Server firewall blocks reading the full list from master", status, StringComparison.Ordinal);
        Assert.Equal(new[] { "Sales" }, listed);
    }

    [Fact]
    public void TheFallback_TakesAUserDatabase_NotMasterOrNone()
    {
        Assert.True(ExcludedDatabasesDialog.CanFallBackToConnectedDatabase("Sales"));
        Assert.False(ExcludedDatabasesDialog.CanFallBackToConnectedDatabase("master"));
        Assert.False(ExcludedDatabasesDialog.CanFallBackToConnectedDatabase("MASTER"));
        Assert.False(ExcludedDatabasesDialog.CanFallBackToConnectedDatabase(""));
        Assert.False(ExcludedDatabasesDialog.CanFallBackToConnectedDatabase(null));
    }

    /// <summary>Drives the real read path, with only the query replaced: the connection string, the fallback and the wording are the dialog's own.</summary>
    private static (string Status, List<string> Listed) StatusAfterRealRead(string? databaseName, Exception failure)
    {
        var dir = DialogWalkTestHost.ScratchDir("ExcludedDbRealRead");
        try
        {
            var status = string.Empty;
            var listed = new List<string>();
            StaTestThread.Run(async () =>
            {
                await DialogWalkTestHost.WithTheme(async () =>
                {
                    var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "sql-reporting-02.corp.example", DisplayName = "Reporting replica", DatabaseName = databaseName, IsEnabled = true };
                    var manager = new ServerManager(dir);
                    /* A saved server whose host this Windows account has confirmed, as every server that reaches this dialog is. */
                    manager.AddServer(server);
                    manager.ConfirmHost(server);
                    /* The read path must reach the query: a connection string the resolver cannot build would be a different failure. */
                    var resolverFault = Record.Exception(() => manager.CredentialResolver.GetConnectionString(server));
                    Assert.True(resolverFault is null, "the connection string could not be built: " + resolverFault);
                    string? queried = null;
                    var dialog = new ExcludedDatabasesDialog(manager, server)
                    {
                        DatabaseQuery = connectionString => { queried = connectionString; return Task.FromException<List<string>>(failure); },
                    };
                    await dialog.LoadForTestAsync();
                    Assert.True(queried is not null, "the read never reached the query");
                    status = ((TextBlock)dialog.FindName("StatusText")).Text;
                    listed = (((ItemsControl)dialog.FindName("DatabasesItemsControl")).ItemsSource as IEnumerable<DatabaseExclusionItem>)?.Select(i => i.Name).ToList() ?? new List<string>();
                    return true;
                });
            });
            return (status, listed);
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }

    private static string StatusAfterFailedLoad(bool xeSessionsMode, Exception failure)
    {
        var dir = DialogWalkTestHost.ScratchDir("ExcludedDbLoadFail");
        try
        {
            string status = string.Empty;
            StaTestThread.Run(async () =>
            {
                await DialogWalkTestHost.WithTheme(async () =>
                {
                    var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "sql-reporting-02.corp.example", DisplayName = "Reporting replica", IsEnabled = true };
                    var dialog = new ExcludedDatabasesDialog(new ServerManager(dir), server, xeSessionsMode)
                    {
                        DatabaseListReader = () => Task.FromException<List<string>>(failure),
                    };
                    await dialog.LoadForTestAsync();
                    status = ((TextBlock)dialog.FindName("StatusText")).Text;
                    return true;
                });
            });
            return status;
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
