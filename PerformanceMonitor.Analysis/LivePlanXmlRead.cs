/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Reads the one value a live plan fetch returns (#9195): the first column of the first row of the command, the
/// string <c>ExecuteScalarAsync</c> gives, without that call's cost on a large plan.
///
/// <para><c>sys.dm_exec_query_plan</c> returns the plan of a whole batch, so one plan handle can carry more than a
/// hundred statements and ten megabytes of XML. Microsoft.Data.SqlClient's asynchronous reads of one large value
/// (<c>ExecuteScalarAsync</c>, a row-buffered <c>ReadAsync</c>, and <c>GetFieldValueAsync</c> even under
/// <see cref="CommandBehavior.SequentialAccess"/>) parse everything buffered so far again each time a packet arrives,
/// so their CPU grows faster than the value does. The row is read here with
/// <see cref="CommandBehavior.SequentialAccess"/> and the value is taken with the synchronous
/// <see cref="DbDataReader.GetChars"/>, <see cref="ChunkChars"/> characters a call, which reads each packet once.
/// Measured on SQL Server 2022 with Microsoft.Data.SqlClient 7.1.2: a 10.5 million character plan costs the client
/// about 6,000 ms of CPU through <c>ExecuteScalarAsync</c> and about 560 ms read this way, and a 0.9 million
/// character plan 97 ms against 67. Both return the same string.</para>
///
/// <para>The value is read in chunks because of the command's timeout. SqlClient's command timeout is the
/// cumulative network read time of one call on the reader, so one call for the whole value has to receive all of
/// it inside the timeout: read with one <c>GetValue</c>, the ten megabyte plan failed in the field with Execution
/// Timeout Expired under the fetch's 15 seconds. A chunk is a call of its own, so the timeout bounds a server that
/// stops answering and not the length of the transfer.</para>
///
/// <para>The chunk reads block their thread for the length of the transfer, so the awaits before them leave the
/// caller's context. The token cancels the command while they run and is checked after each chunk, and a read that
/// fails under a cancelled token is reported as the cancellation it is (#2443), never as a failed fetch.</para>
/// </summary>
public static class LivePlanXmlRead
{
    /// <summary>
    /// Characters asked of the provider in one call. The buffer stays under the large object heap's threshold, and
    /// at this size the chunked read costs the CPU of one read of the whole value. At a million characters a call
    /// it costs twice that, measured on a 10.5 million character plan.
    /// </summary>
    public const int ChunkChars = 32 * 1024;

    /// <summary>The largest length the text is sized for before the value is read.</summary>
    private const int MaxPresizeChars = 64 * 1024 * 1024;

    /// <summary>The plan text, or null when the command returns no row or a NULL value.</summary>
    public static async Task<string?> FirstValueAsync(DbCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        try
        {
            var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    return null;

                using var abandon = cancellationToken.Register(static state => ((DbCommand)state!).Cancel(), command);
                if (reader.IsDBNull(0))
                    return null;

                // A provider that knows the value's length says so here. SqlClient does not know an xml value's
                // length before it has read it, and answers -1.
                var length = reader.GetChars(0, 0, null, 0, 0);
                var text = length is > 0 and <= MaxPresizeChars ? new StringBuilder((int)length) : new StringBuilder();
                var chunk = new char[ChunkChars];
                long read;
                while ((read = reader.GetChars(0, text.Length, chunk, 0, chunk.Length)) > 0)
                {
                    text.Append(chunk, 0, (int)read);
                    cancellationToken.ThrowIfCancellationRequested();
                }

                return text.ToString();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The plan read was cancelled.", ex, cancellationToken);
        }
    }
}
