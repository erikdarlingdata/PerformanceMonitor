/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// A store table health read that ran past its <see cref="StorageCommandDeadlines.McpReadSeconds"/> limit (#8806). Npgsql words its own
/// client deadline as "Exception while reading from stream", which does not say a limit tripped, so
/// <see cref="DarlingStoreTableHealthReader"/> turns both shapes of a deadline trip (the store's <c>statement_timeout</c> 57014 and the
/// client's <c>CommandTimeout</c>) into this exception. Its <see cref="Exception.Message"/> is the sentence the MCP tool, the web page and
/// the Viewer tab show. A caller that cancelled its own token never gets this: that stays an <see cref="OperationCanceledException"/>.
/// </summary>
public sealed class StoreTableHealthTimeoutException : Exception
{
    /// <summary>The limit that tripped, in seconds.</summary>
    public int TimeoutSeconds { get; }

    public StoreTableHealthTimeoutException(Exception inner)
        : base(Sentence(), inner)
    {
        TimeoutSeconds = StorageCommandDeadlines.McpReadSeconds;
    }

    /// <summary>True when <paramref name="exception"/> is the store's statement timeout or the client's command timeout and the caller did
    /// not cancel.</summary>
    internal static bool IsDeadlineTrip(Exception exception, CancellationToken cancellationToken) =>
        !cancellationToken.IsCancellationRequested
        && (exception is PostgresException { SqlState: "57014" }
            || (exception is NpgsqlException && exception.InnerException is TimeoutException));

    private static string Sentence() =>
        string.Create(CultureInfo.InvariantCulture,
            $"The store table health read ran past its {StorageCommandDeadlines.McpReadSeconds}-second limit and was stopped. Nothing was changed. The store is busy or holds very many chunks, so try again when it is quieter.");
}
