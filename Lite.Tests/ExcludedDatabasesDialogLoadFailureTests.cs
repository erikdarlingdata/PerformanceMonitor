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
/// has the details. The exception's own text stays out of the dialog, and no message box opens: the load runs on the test's
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
