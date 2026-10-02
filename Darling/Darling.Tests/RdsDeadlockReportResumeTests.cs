/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A deadlock report held between two chunks of the RDS log (#4735), followed through the whole ingestor: what it
/// stores, and where the saved resume position is while the report's head lives only in memory.
///
/// <para>The rows are caught by <see cref="RdsDeadlockIngestor.RowWriter"/> instead of a store, so "stored" is a row
/// counted and "the marker is saved" is the fake state read after it. The store the data source names is never
/// opened.</para>
/// </summary>
public sealed class RdsDeadlockReportResumeTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";
    private const string Older = "error/postgresql.log.2026-08-25-17";
    private const string Newest = "error/postgresql.log.2026-08-25-18";

    private const string Prefix = "2026-08-26 22:25:24.100 UTC [1549] ";

    private const string Header = Prefix + "ERROR:  deadlock detected\n";

    private const string DetailFirst =
        Prefix + "DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n";

    private const string DetailRest =
        "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n";

    private const string Detail = DetailFirst + DetailRest;

    private const string Hint = Prefix + "HINT:  See server log for query details.\n";

    /// <summary>Header, DETAIL and HINT: a report a read got whole.</summary>
    private const string Whole = Header + Detail + Hint;

    private const string Other = "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n";

    /// <summary>One ingestor over the fake log and the fake position store, and the rows it stores. A new call is a new
    /// process: the report carry and the source's in-memory positions start empty, while <paramref name="state"/> is what
    /// the last process saved.</summary>
    private static (Func<Task<RdsIngestOutcome>> Ingest, List<PgDeadlocksCollector.Row> Stored) Ingestor(
        RdsRotationFakeRds client, RdsFakeCollectorState state)
    {
        var stored = new List<PgDeadlocksCollector.Row>();

        var ingestor = new RdsDeadlockIngestor(
            NpgsqlDataSource.Create(DeadStore), new RdsLogSource(_ => client), resume: state.Store("pg_deadlocks"))
        {
            RowWriter = (rows, _) =>
            {
                stored.AddRange(rows);
                return Task.FromResult(rows.Count);
            },
        };

        return (() => ingestor.IngestAsync(1, "s", Host, false, false, CancellationToken.None), stored);
    }

    private static string SavedPosition(RdsFakeCollectorState state) => state.Value("pg_deadlocks", "log_resume") ?? "(none)";

    /// <summary>
    /// A chunk that ends inside a report holds the report's head in memory only, so the position saved after that
    /// chunk would send a restart to the middle of the report, where no header is left to find. The position waits: the
    /// chunk that finishes the report stores it once, whole, and only then is the position saved.
    /// </summary>
    [Fact]
    public async Task AChunkThatEndsInsideAReport_SavesNoPosition_UntilTheNextChunkStoresTheWholeReport()
    {
        var client = new RdsRotationFakeRds(Newest) { Body = Header + DetailFirst };
        var state = new RdsFakeCollectorState();
        var (ingest, stored) = Ingestor(client, state);

        await ingest();

        Assert.Equal(0, state.Saves);
        Assert.Empty(stored);

        client.Body = DetailRest + Hint;
        await ingest();

        var row = Assert.Single(stored);
        Assert.Equal(PgDeadlockLogParser.Extract(Whole).Single().DeadlockHash, row.DeadlockHash);
        Assert.Equal(1, state.Saves);
        Assert.Equal("rds|1|solo|M2|" + Newest, SavedPosition(state));
    }

    /// <summary>
    /// A restart while a report is held reads again from the position saved before the chunk that began the report, so
    /// the report is found whole in that read and stored, instead of the chunk after it starting mid-report and the
    /// deadlock never being stored.
    /// </summary>
    [Fact]
    public async Task ARestartWhileAReportIsHeld_ReadsTheChunkThatBeganItAgain()
    {
        var client = new RdsRotationFakeRds(Newest) { Body = Other };
        var state = new RdsFakeCollectorState();
        var (first, _) = Ingestor(client, state);

        await first();
        Assert.Equal("rds|1|solo|M1|" + Newest, SavedPosition(state));

        /* The next chunk ends inside a report: held, and the position stays where the quiet chunk left it. */
        client.Body = Header + Detail;
        await first();
        Assert.Equal("rds|1|solo|M1|" + Newest, SavedPosition(state));

        /* A new process. The log now holds the finished report, and the read starts from the saved position. */
        client.Downloads.Clear();
        client.Body = Whole;
        var (restarted, stored) = Ingestor(client, state);

        await restarted();

        Assert.Equal("M1", client.Downloads[0].Marker);
        Assert.Equal(0, client.Downloads[0].NumberOfLines);

        var row = Assert.Single(stored);
        Assert.Equal(PgDeadlockLogParser.Extract(Whole).Single().DeadlockHash, row.DeadlockHash);
    }

    /// <summary>
    /// A quiet cycle on the newest file gives a held report nothing new to finish it with, and the file can still grow,
    /// so the report stays held: not stored early as the fragment it is, and stored whole once the rest arrives.
    /// </summary>
    [Fact]
    public async Task AQuietCycleOnTheNewestFile_KeepsTheReportHeld_UntilTheRestArrives()
    {
        var client = new RdsRotationFakeRds(Newest) { Body = Header + Detail };
        var (ingest, stored) = Ingestor(client, new RdsFakeCollectorState());

        await ingest();
        client.Body = string.Empty;
        await ingest();

        Assert.Empty(stored);

        client.Body = Hint;
        await ingest();

        var row = Assert.Single(stored);
        Assert.Equal(PgDeadlockLogParser.Extract(Whole).Single().DeadlockHash, row.DeadlockHash);
    }

    /// <summary>
    /// The last read of a file that has been rotated away can come back empty: nothing more was written to it. A report
    /// the previous read ended inside can then never be finished, and it is stored as it is, once, before the position
    /// moves to the newer file. It used to stay held, and the read of the newer file dropped it without a count.
    /// </summary>
    [Fact]
    public async Task AReportHeldAtTheEndOfARotatedFile_IsStoredOnce_WhenTheFileHasNothingMoreToRead()
    {
        var client = new RdsRotationFakeRds(Older) { Body = Header + Detail };
        var state = new RdsFakeCollectorState();
        var (ingest, stored) = Ingestor(client, state);

        await ingest();
        Assert.Empty(stored);

        client.Listed = new[] { Older, Newest };
        client.Body = string.Empty;
        client.Downloads.Clear();

        await ingest();

        Assert.Equal(Older, client.Downloads[0].LogFileName);

        var row = Assert.Single(stored);
        Assert.Equal(PgDeadlockLogParser.Extract(Header + Detail).Single().DeadlockHash, row.DeadlockHash);

        /* The position moved on to the newer file, and the next cycle has nothing left to store. */
        Assert.Equal("rds|1|solo|M3|" + Newest, SavedPosition(state));

        await ingest();
        Assert.Single(stored);
    }

    /// <summary>
    /// A report held for a file that RDS no longer lists is never finished either, and the next read starts on the newest
    /// file, so the report is stored as it is there instead of being dropped when the carry moves to the new file's name.
    /// </summary>
    [Fact]
    public async Task AReportHeldForAFileRdsNoLongerLists_IsStoredAsItIs_WhenTheNewestFileIsRead()
    {
        var client = new RdsRotationFakeRds(Older) { Body = Header + Detail };
        var (ingest, stored) = Ingestor(client, new RdsFakeCollectorState());

        await ingest();
        Assert.Empty(stored);

        client.Listed = new[] { Newest };
        client.Body = Other;
        client.Downloads.Clear();

        await ingest();

        Assert.Equal(Newest, client.Downloads[0].LogFileName);

        var row = Assert.Single(stored);
        Assert.Equal(PgDeadlockLogParser.Extract(Header + Detail).Single().DeadlockHash, row.DeadlockHash);

        await ingest();
        Assert.Single(stored);
    }
}
