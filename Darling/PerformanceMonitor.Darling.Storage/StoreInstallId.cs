/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Placeholder: the tests that pin the install id store compile against these signatures.</summary>
public static class StoreInstallId
{
    public const string ReadSql = "";

    public static Task<string> EnsureAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken) =>
        throw new NotImplementedException();

    public static Task<string?> TryReadAsync(NpgsqlConnection connection, CancellationToken cancellationToken) =>
        throw new NotImplementedException();

    public static Task<string?> TryReadAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken) =>
        throw new NotImplementedException();
}
