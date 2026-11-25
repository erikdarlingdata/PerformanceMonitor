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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Reads the one value a live plan fetch returns (#9195): the first column of the first row of the command, as
/// <c>ExecuteScalarAsync</c> gives it, without that call's cost on a large plan.
///
/// <para><c>sys.dm_exec_query_plan</c> returns the plan of a whole batch, so one plan handle can carry more than a
/// hundred statements and ten megabytes of XML. Microsoft.Data.SqlClient's asynchronous reads of one large value
/// (<c>ExecuteScalarAsync</c>, a row-buffered <c>ReadAsync</c>, and <c>GetFieldValueAsync</c> even under
/// <see cref="CommandBehavior.SequentialAccess"/>) parse everything buffered so far again each time a packet arrives,
/// so their CPU grows faster than the value does. The row is read here with
/// <see cref="CommandBehavior.SequentialAccess"/> and the value is taken with the synchronous
/// <see cref="DbDataReader.GetValue"/>, which reads each packet once. Measured against SQL Server 2022 with
/// Microsoft.Data.SqlClient 7.1.2: a plan eight times the size costs <c>ExecuteScalarAsync</c> 25 times the CPU and
/// this read 5 times, and on a 10.5 million character plan of 136 statements this read's CPU is a twelfth of
/// <c>ExecuteScalarAsync</c>'s. Both return the same string.</para>
///
/// <para>The value read blocks its thread for the length of the transfer, so the awaits before it leave the caller's
/// context. The command's timeout bounds that transfer, counted from the start of the value read: the asynchronous
/// reads let a transfer run past the timeout for as long as data keeps arriving, and this one ends it there, as a
/// failed fetch. The token cancels the command while the value read runs, and a read that fails under a cancelled
/// token is reported as the cancellation it is (#2443), never as a failed fetch.</para>
/// </summary>
public static class LivePlanXmlRead
{
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
                var value = reader.GetValue(0);
                return value is null or DBNull ? null : value.ToString();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The plan read was cancelled.", ex, cancellationToken);
        }
    }
}
