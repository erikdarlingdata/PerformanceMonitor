/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9333: the store-log capture's warning names the cause the fault proves. A 58P01 is a store with no log folder
/// (the compose store ran without logging_collector), and the old text blamed missing privileges for it.
/// </summary>
[Trait("Stage", "Guard")]
public class StoreLogCaptureFailureCauseTests
{
    private static PostgresException Pg(string sqlState)
        => new("boom", "ERROR", "ERROR", sqlState);

    [Fact]
    public void AMissingLogFolderIsNotBlamedOnPrivileges()
    {
        var missingFolder = DarlingWorker.StoreLogCaptureFailureCause(Pg(PostgresErrorCodes.UndefinedFile));
        var denied = DarlingWorker.StoreLogCaptureFailureCause(Pg(PostgresErrorCodes.InsufficientPrivilege));

        /* The privilege cause names the roles the read needs; the missing-folder cause must name none of them. */
        Assert.NotEqual(denied, missingFolder);
        Assert.DoesNotContain("pg_monitor", missingFolder, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_binary_file", missingFolder, StringComparison.Ordinal);
    }

    [Fact]
    public void AnyFaultButAMissingFolderKeepsThePrivilegeCause()
    {
        var denied = DarlingWorker.StoreLogCaptureFailureCause(Pg(PostgresErrorCodes.InsufficientPrivilege));

        Assert.Equal(denied, DarlingWorker.StoreLogCaptureFailureCause(Pg(PostgresErrorCodes.QueryCanceled)));
        Assert.Equal(denied, DarlingWorker.StoreLogCaptureFailureCause(new InvalidOperationException("x")));
        /* The read needs pg_monitor (pg_ls_logdir and the log_directory setting) and EXECUTE on pg_read_binary_file.
           pg_read_server_files is not among them: PostgreSQL lets EXECUTE read any file under the data directory or an
           absolute log_directory, so naming it would ask for a role that reads every file the server can. */
        Assert.Contains("pg_monitor", denied, StringComparison.Ordinal);
        Assert.Contains("pg_read_binary_file", denied, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_read_server_files", denied, StringComparison.Ordinal);
    }

    [Fact]
    public void ARelativeLogDirectoryOutsideTheDataDirectory_IsNamed_WithAnAbsolutePathAsTheFix()
    {
        /* PostgreSQL's own refusal for a relative path that leaves the data directory, such as log_directory = '../pglog'.
           No EXECUTE grant reads there, so the privilege cause would send the operator after grants they already hold. */
        var refused = DarlingWorker.StoreLogCaptureFailureCause(new PostgresException(
            "path must be in or below the data directory", "ERROR", "ERROR", PostgresErrorCodes.InsufficientPrivilege));
        var denied = DarlingWorker.StoreLogCaptureFailureCause(Pg(PostgresErrorCodes.InsufficientPrivilege));

        Assert.NotEqual(denied, refused);
        Assert.Contains("log_directory is a relative path", refused, StringComparison.Ordinal);
        Assert.Contains("absolute path", refused, StringComparison.Ordinal);
    }
}
