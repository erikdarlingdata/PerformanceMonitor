/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Service for reading collected data from DuckDB.
/// Partial class - individual data type readers are in separate files.
/// </summary>
public partial class LocalDataService
{
    private readonly DuckDbInitializer _duckDb;

    public LocalDataService(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>Lite's own database after a fatal error, for Collection Health. Read from memory, never the database.</summary>
    public LocalDatabaseHealth LocalDatabaseHealth => _duckDb.LocalDatabaseHealth;

    /// <summary>
    /// Creates and opens a DuckDB connection wrapped in a read lock.
    /// The lock prevents CHECKPOINT and compaction from reorganizing the database file
    /// while this connection is reading from it.
    /// </summary>
    internal async Task<LockedConnection> OpenConnectionAsync()
    {
        var readLock = _duckDb.AcquireReadLock();
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, readLock);
        }
        catch
        {
            readLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The write-lock wait <see cref="OpenWriteConnectionAsync"/> gets in a host that declares none: what a
    /// WPF dispatcher can afford to spend before <see cref="GetDatabaseStateDeviationsAsync"/> skips a
    /// maintenance cycle rather than stall the window. The shipped app declares none, so this is the app's.
    /// </summary>
    internal static readonly TimeSpan DefaultWriteLockBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The runtime configuration property a host sets, in seconds, to state its own
    /// <see cref="WriteLockBudget"/>. Declared as a <c>RuntimeHostConfigurationOption</c> in the host's
    /// project file, which lands it in that host's <c>runtimeconfig.json</c>.
    /// </summary>
    internal const string WriteLockBudgetConfigKey = "PerformanceMonitorLite.WriteLockBudgetSeconds";

    /// <summary>
    /// The largest budget a host can state. A wait that outlives the process or the job containing it
    /// cannot produce <see cref="OpenWriteConnectionAsync"/>'s timeout at all — something else kills the
    /// run first — so beyond this a declared budget is indistinguishable from no timeout, and being short
    /// of forever is the whole reason there is a number here. It is also what keeps the resolver total:
    /// <c>NumberStyles.Float</c> admits exponents, and .NET parses the invariant <c>Infinity</c> symbol
    /// whatever the style, so <c>1e300</c> and <c>Infinity</c> both reach
    /// <see cref="TimeSpan.FromSeconds"/> as positive doubles that overflow it. Out of a static
    /// initializer that throw is a <c>TypeInitializationException</c> on the first store call in
    /// the process — worse than the timeout it would be replacing, and a third outcome besides "the wait
    /// succeeded" and "fail loudly".
    /// </summary>
    internal static readonly TimeSpan MaxWriteLockBudget = TimeSpan.FromHours(1);

    /// <summary>
    /// How long <see cref="OpenWriteConnectionAsync"/> waits for the write lock in THIS host.
    ///
    /// <para><b>The wait belongs to the host, because the thing it protects does.</b>
    /// <c>DuckDbInitializer.s_dbLock</c> is process-wide, so an acquisition here queues behind every other
    /// holder in the process however few rows it means to touch, and a dispatcher awaiting this call cannot
    /// afford an unbounded queue — which is the entire reason this one caller passes a timeout while the
    /// app's other write-lock callers pass none. A host with no dispatcher to protect is in the position of
    /// those other callers: waiting is the correct answer, and expiring is the wrong one.</para>
    ///
    /// <para>So the host states it in its own project file, where the value is fixed before <c>Main</c> and
    /// out of reach of anything that runs afterwards. That matters more than it looks: this number governs
    /// every caller in the process at once, so a settable static would let one component — or one test —
    /// put every concurrent neighbour on whatever fuse it picked, which is a smaller copy of the problem
    /// rather than a fix for it.</para>
    ///
    /// <para><c>Lite.Tests</c> is the host that declares one. Its classes hold their own DuckDB files but
    /// share this lock, several of its test methods take the exclusive lock deliberately and hold it for
    /// seconds while they assert on timing, and its acquisitions have no dispatcher behind them. Its budget
    /// is two minutes — orders of magnitude above the holds its own suite takes, and still short enough to
    /// fail loudly, with this method's own message, if a holder wedges rather than merely being slow.</para>
    /// </summary>
    internal static readonly TimeSpan WriteLockBudget =
        ResolveWriteLockBudget(AppContext.GetData(WriteLockBudgetConfigKey));

    /// <summary>
    /// The seconds a host declared, as a budget. Absent, unparseable, non-positive, above
    /// <see cref="MaxWriteLockBudget"/>, and small enough to round away all resolve
    /// <see cref="DefaultWriteLockBudget"/>: the failure worth guarding against is a host quietly losing
    /// its dispatcher protection, not a host quietly gaining a longer wait. TOTAL — every input returns a
    /// budget in <c>(TimeSpan.Zero, MaxWriteLockBudget]</c> and none throws, which matters because this
    /// runs in a static initializer, where an exception takes the whole type down rather than one call.
    ///
    /// <para>Takes the raw value rather than reading the key itself, so the parse can be exercised without
    /// a second host. MSBuild types it as a JSON number in <c>runtimeconfig.json</c> and the host hands it
    /// back as a string, which is why it is parsed rather than cast — and parsed against the invariant
    /// culture, because a project file is not written in the machine's locale.</para>
    ///
    /// <para>The upper bound is not <see cref="TimeSpan"/>'s representable range, which reaches roughly
    /// 29,000 years: a budget that large parses, converts and then makes a wedged lock hang for the life
    /// of the process, turning a loud failure into a silent one. The bound has to mean something for the
    /// refusal to be worth having.</para>
    /// </summary>
    internal static TimeSpan ResolveWriteLockBudget(object? configured)
    {
        if (configured is string declared &&
            double.TryParse(declared, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds) &&
            seconds > 0 &&
            seconds <= MaxWriteLockBudget.TotalSeconds)
        {
            /* The BUDGET is what gets checked, not the number that produced it. A positive double under
               the ceiling can still round to TimeSpan.Zero - double.Epsilon does - and a zero timeout is
               TryEnterWriteLock giving up without waiting, which is a budget in name only. */
            var budget = TimeSpan.FromSeconds(seconds);
            if (budget > TimeSpan.Zero)
            {
                return budget;
            }
        }

        return DefaultWriteLockBudget;
    }

    /// <summary>
    /// Creates and opens a DuckDB connection wrapped in an exclusive write lock, bounded by
    /// <see cref="WriteLockBudget"/> so the UI thread cannot freeze behind an in-flight archival.
    ///
    /// <para><b>This doc comment used to say "use for UPDATE/DELETE/INSERT operations that must not race
    /// with archival or compaction", and was read as the house rule for the whole app (#2463).</b> It is
    /// not, and the INSERT in that sentence is the part that was wrong: excluding archival is what the
    /// READ lock already does, since a held read lock blocks <c>EnterWriteLock</c>. What this method
    /// additionally buys is exclusion of OTHER WRITERS, which an UPDATE or a DELETE needs — DuckDB's
    /// optimistic concurrency fails the loser of a write-write collision rather than queueing it — and
    /// which an append of new rows does not. The rule, with the measurements behind it, is on
    /// <c>DuckDbInitializer.s_dbLock</c>; the fourteen callers here are UPDATE, DELETE and #2208's
    /// multi-statement maintenance block, and all of them sit on the right side of it.</para>
    ///
    /// <para>The timeout is this method's own contribution and is not part of the lock rule: every other
    /// write-lock caller in the app waits indefinitely, which they can afford and the UI thread cannot.
    /// See <see cref="LocalDataService.GetDatabaseStateDeviationsAsync"/> for what a caller does when it
    /// expires.</para>
    /// </summary>
    internal async Task<LockedConnection> OpenWriteConnectionAsync()
    {
        var writeLock = _duckDb.AcquireWriteLock(timeout: WriteLockBudget);
        try
        {
            var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();
            return new LockedConnection(connection, writeLock);
        }
        catch
        {
            writeLock.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Safely converts a DuckDB value to double, handling BigInteger from SUM aggregations.
    /// </summary>
    private static double ToDouble(object value)
    {
        if (value is BigInteger bi)
            return (double)bi;
        return Convert.ToDouble(value);
    }

    /// <summary>
    /// Safely converts a DuckDB value to long, handling BigInteger from SUM/COUNT aggregations.
    /// </summary>
    private static long ToInt64(object value)
    {
        if (value is BigInteger bi)
            return (long)bi;
        return Convert.ToInt64(value);
    }

    /// <summary>
    /// Gets the time range for queries based on hoursBack or explicit date range.
    /// Returns UTC time for collection_time queries (most tables store collection_time in UTC).
    ///
    /// <para>A custom range is a naive-UTC pair (#4766): the tab holds it as instants, the pickers, a slicer
    /// selection and a drill each produce it in UTC, and it reaches this read and the SQL beside it in the same
    /// frame, so it is returned as it came. Nothing between the producer and the query converts a bound through
    /// a server clock. Converting a wall-clock bound back to UTC had to pick one occurrence of an hour that
    /// repeats after a fall-back and picked the first, so a range typed for the second occurrence read the hour
    /// before it. <c>internal</c> so the tests can call it.</para>
    /// </summary>
    /// <param name="fromDate">The custom range's start, naive UTC, or <c>null</c> for an hours-back window.</param>
    /// <param name="toDate">The custom range's end, naive UTC, or <c>null</c> for an hours-back window. The custom
    /// range applies only when both are supplied.</param>
    internal static (DateTime startTime, DateTime endTime) GetTimeRange(int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime? asOfUtc)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            /* Custom date range - already UTC, the frame collection_time is stored in (#4766). */
            return (fromDate.Value, toDate.Value);
        }

        /*
            #2495: asOfUtc moves the END of the hoursBack window off "now" so a caller can ask about a
            past incident. It is a separate parameter from fromDate/toDate so a caller states the window's
            length once (hoursBack) and its end once (asOfUtc); a custom range wins over it when both are given.
        */
        var anchor = asOfUtc ?? DateTime.UtcNow;

        /* Use UTC directly since collection_time is stored in UTC */
        return (anchor.AddHours(-hoursBack), anchor);
    }

    /// <summary>
    /// #4279: the exact UTC window GetTopQueriesByCpuAsync / GetTopProceduresByCpuAsync /
    /// GetQueryStoreTopQueriesAsync read for the Queries tab's three grids -- the SAME <see cref="GetTimeRange"/>
    /// call those three make internally. <c>internal</c> (not private) so <c>ServerTab.RefreshWindowTruncatedBannerAsync</c>
    /// (ServerTab.Refresh.cs) can probe this exact window instead of recomputing its own copy: before this
    /// existed, the banner's custom-range window was built from a server-local pair while
    /// GetTopQueriesByCpuAsync's own window went through GetTimeRange's custom-range branch and came out UTC --
    /// the grid and its banner silently disagreed on any server not on UTC (#4279). The custom range is a
    /// naive-UTC pair end to end now (#4766), so the grid and its banner take the same two instants.
    /// </summary>
    internal static (DateTime startUtc, DateTime endUtc) GetQueriesTabWindowUtc(int hoursBack, DateTime? fromDate, DateTime? toDate)
        => GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);

    /// <summary>
    /// Gets the time range in server local time (for tables like cpu_utilization_stats.sample_time).
    ///
    /// <para>The window is the server-local rendering of its two UTC ends (#4766). An hours-back window ends at the
    /// anchor on the server's clock and starts at <c>anchor - hoursBack</c> on the server's clock, and a custom range
    /// (a naive-UTC pair, as <see cref="GetTimeRange"/> takes it) renders each bound the same way, each with the
    /// offset in force at its own instant. A window that spans a daylight saving change is therefore as long in real
    /// time as the caller asked, and its wall-clock span is an hour more or less than <c>hoursBack</c>.
    /// <c>internal</c> so the tests can call it.</para>
    /// </summary>
    /// <param name="serverClock">
    /// The clock of the server whose rows this window will select — the same server as the
    /// <c>server_id</c> in the predicate beside it. REQUIRED rather than defaulted: a clock and a
    /// server_id are two halves of one question, and taking the clock from ambient state is how they came
    /// to name two different servers. A caller with no server-specific clock to give has to say so at the
    /// call site instead of inheriting one silently. It renders both a custom range and an hours-back window.
    /// </param>
    internal static (DateTime startTime, DateTime endTime) GetTimeRangeServerLocal(int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime? asOfUtc, ServerClock serverClock)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            /* fromDate/toDate are naive UTC (#4766); each bound goes to the server's wall clock at its own instant. */
            return (serverClock.ToServerLocal(fromDate.Value), serverClock.ToServerLocal(toDate.Value));
        }

        /* The anchor arrives in UTC (see GetTimeRange) and is carried into server-local here, so both
           families answer the same instant even though they window on differently-based columns. The start is
           converted from its own UTC instant, not derived from the server-local end, so a change inside the
           window moves it by the right amount. */
        var anchor = asOfUtc ?? DateTime.UtcNow;
        return (serverClock.ToServerLocal(anchor.AddHours(-hoursBack)), serverClock.ToServerLocal(anchor));
    }

    /// <summary>
    /// Starts query timing for performance logging. Use with 'using' statement.
    /// Only logs queries that exceed the slow query threshold (default 500ms).
    /// </summary>
    private static Helpers.QueryExecutionContext TimeQuery(string context, string sql)
    {
        return Helpers.QueryLogger.StartQuery(context, sql, source: "DuckDB");
    }

}
