/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #8146: the one <c>ring_buffer</c> target clause of the deadlock, blocked-process and long-query sessions, so every create
/// site writes <c>max_events_limit</c> explicitly and the loss note can compare the session's limit with the setting. A
/// session keeps the limit it was created with; the engine's own default (1000) is what a create without the option gets.
/// </summary>
public static class RingBufferTarget
{
    public const int DefaultMaxEvents = 1000;
    public const int MinMaxEvents = 1000;
    public const int MaxMaxEvents = 10000;

    /// <summary>The target clause, without a trailing newline. A null limit leaves the option out (the engine's default).</summary>
    public static string Sql(int? maxEvents) =>
        "ADD TARGET package0.ring_buffer\n(\n    SET max_memory = 4096"
        + (maxEvents is { } limit ? ", max_events_limit = " + limit.ToString(CultureInfo.InvariantCulture) : string.Empty)
        + "\n)";

    /// <summary>
    /// The setting's value bounded to [<see cref="MinMaxEvents"/>, <see cref="MaxMaxEvents"/>]; unset is the default. Takes a long
    /// so a value too large for an int (Lite reads settings.json as one) still lands on the nearest bound (#8146).
    /// </summary>
    public static int Clamp(long? configured) =>
        configured is { } value ? (int)Math.Clamp(value, MinMaxEvents, MaxMaxEvents) : DefaultMaxEvents;

    /// <summary>
    /// #8146: the errors an engine raises when it refuses <c>max_events_limit</c>, measured on SQL Server 2022: 25629 ("the
    /// customizable attribute ... does not exist", an engine that does not know the option) and 25713 ("the value specified for
    /// customizable attribute ... is invalid"). Only these send a create again without the option; any other failure (a read-only
    /// database, a permission error, "already exists") is not about the option and surfaces as it did before #8146.
    /// </summary>
    public static readonly IReadOnlyList<int> OptionRefusedErrorNumbers = new[] { 25629, 25713 };

    /// <summary>
    /// #8146: the engine's "the session is already there" errors, 25631 (already exists) and 25705 (already started); the same
    /// numbers the hosts' <c>IsBenignXeSessionAlreadyPresent</c> treats as benign. A retry that fails with only these is not
    /// trusted, see <see cref="CreateWithoutLimitOnRefusalAsync"/>.
    /// </summary>
    public static readonly IReadOnlyList<int> AlreadyPresentErrorNumbers = new[] { 25631, 25705 };

    /// <summary>True when <paramref name="errorNumbers"/> is not empty and holds only <see cref="AlreadyPresentErrorNumbers"/>.</summary>
    public static bool IsOnlyAlreadyPresent(IEnumerable<int> errorNumbers)
    {
        ArgumentNullException.ThrowIfNull(errorNumbers);

        var numbers = errorNumbers.ToList();
        return numbers.Count > 0 && numbers.All(AlreadyPresentErrorNumbers.Contains);
    }

    /// <summary>
    /// #8146: an Azure SQL Database create of a session whose DDL carries <c>max_events_limit</c>, which is unverified there. A
    /// session at the engine's default limit is better than none, and the loss note shows the limit the session got, so a refused
    /// create is sent once more with the option left out (<paramref name="create"/> gets null), but only when the errors read by
    /// <paramref name="errorNumbersOf"/> include one of <see cref="OptionRefusedErrorNumbers"/>. Any other failure, and a cancellation, reaches the caller as before. If the retry fails
    /// too, its exception surfaces, except an "already there" one (<see cref="IsOnlyAlreadyPresent"/>): the create and the start
    /// can go as one batch, so the first attempt may have created a session it never started, and the callers swallow "already
    /// there" as benign. Then the first attempt's exception surfaces instead, so a session that did not start is not hidden.
    /// The twin of the deadlock and blocked-process retry in <c>AlwaysOnXeSessions.CreateAsync</c>.
    /// </summary>
    public static async Task CreateWithoutLimitOnRefusalAsync(int maxEvents, Func<int?, Task> create, Func<Exception, IEnumerable<int>> errorNumbersOf)
    {
        ArgumentNullException.ThrowIfNull(create);
        ArgumentNullException.ThrowIfNull(errorNumbersOf);

        try
        {
            await create(maxEvents);
        }
        catch (Exception refused) when (refused is not OperationCanceledException && errorNumbersOf(refused).Any(OptionRefusedErrorNumbers.Contains))
        {
            try
            {
                await create(null);
            }
            catch (Exception retry) when (retry is not OperationCanceledException && IsOnlyAlreadyPresent(errorNumbersOf(retry)))
            {
                ExceptionDispatchInfo.Capture(refused).Throw();
            }
        }
    }
}
