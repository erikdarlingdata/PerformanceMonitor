/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The stop-time store writes when the store is going down with the service (#9794): the query-tag sightings flush, the start
/// history's last flush and the collector-cost and read-latency flush. Each is driven against a store that answers the connection
/// with a shutdown SQLSTATE (57P01, 57P02, 57P03), against one that answers with another SQLSTATE, and against a port nothing
/// listens on. No PostgreSQL is needed: <see cref="RefusingStore"/> speaks the start of the wire protocol and answers every
/// connection with one error, which is what a writer sees from a store that is stopping.
/// </summary>
public sealed class StoreStopNoticeTests
{
    private static readonly DateTime T0 = new(2026, 11, 1, 8, 0, 0, DateTimeKind.Utc);

    /// <summary>A real failure for the contrast cases: the store is up and refuses the login for being at its connection limit.</summary>
    private const string TooManyConnections = "53300";

    public static TheoryData<string> ShutdownStates => new() { "57P01", "57P02", "57P03" };

    private static QueryTagSighting[] ThreeSightings() =>
    [
        new QueryTagSighting(QueryTagKeyKind.Hash, "0x01", Database: "Sales"),
        new QueryTagSighting(QueryTagKeyKind.Hash, "0x02", Database: "Sales"),
        new QueryTagSighting(QueryTagKeyKind.Hash, "0x03", Database: "Sales"),
    ];

    private static ServiceStartRecorder Recorder() =>
        new(new ServiceLogTally(), Guid.NewGuid(), "host-a", "3.14.0-test.1", "0123abc", T0, () => T0.AddMinutes(1));

    private static (CollectorCostAccumulator Cost, ReadLatencyAccumulator Latency) Samples()
    {
        var cost = new CollectorCostAccumulator();
        var latency = new ReadLatencyAccumulator();
        cost.Record(1, "wait_stats", rows: 10, sqlMs: 40, storageMs: 5);
        latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);
        return (cost, latency);
    }

    [Theory]
    [MemberData(nameof(ShutdownStates))]
    public async Task SightingsFlushAtStop_StoreGoingDown_GoesToTheNoticeNotAWarning(string sqlState)
    {
        await using var store = new RefusingStore(sqlState);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var buffer = new QueryTagSightingBuffer(postgres, logger);
        await buffer.AppendAsync(1, ThreeSightings(), CancellationToken.None);

        var written = await buffer.FlushOnShutdownAsync(TimeSpan.FromSeconds(30), notice);

        Assert.Equal(0, written);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(1, notice.Count);

        notice.Report();

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        /* The line carries how many sightings were not written. */
        Assert.Contains("3", logger.Lines.Single(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SightingsFlushAtStop_AnotherSqlState_StaysAWarning()
    {
        await using var store = new RefusingStore(TooManyConnections);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var buffer = new QueryTagSightingBuffer(postgres, logger);
        await buffer.AppendAsync(1, ThreeSightings(), CancellationToken.None);

        await buffer.FlushOnShutdownAsync(TimeSpan.FromSeconds(30), notice);
        notice.Report();

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(0, notice.Count);
    }

    /// <summary>The minute flush has no notice: a store that goes away while the service keeps running is still a Warning.</summary>
    [Fact]
    public async Task SightingsFlushWhileRunning_StoreGoingDown_StaysAWarning()
    {
        await using var store = new RefusingStore("57P01");
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var buffer = new QueryTagSightingBuffer(postgres, logger);
        await buffer.AppendAsync(1, ThreeSightings(), CancellationToken.None);

        await buffer.FlushAsync(CancellationToken.None);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task SightingsFlushAtStop_ConnectionRefused_GoesToTheNoticeNotAWarning()
    {
        await using var postgres = NpgsqlDataSource.Create(RefusingStore.ConnectionStringFor(RefusingStore.ClosedPort()));
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var buffer = new QueryTagSightingBuffer(postgres, logger);
        await buffer.AppendAsync(1, ThreeSightings(), CancellationToken.None);

        await buffer.FlushOnShutdownAsync(TimeSpan.FromSeconds(30), notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, notice.Count);
    }

    [Theory]
    [MemberData(nameof(ShutdownStates))]
    public async Task SampleFlushAtStop_StoreGoingDown_NamesBothSampleSetsAndLogsNoWarning(string sqlState)
    {
        await using var store = new RefusingStore(sqlState);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var (cost, latency) = Samples();

        await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger, notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        /* One entry for the collector-cost samples and one for the read-latency samples. */
        Assert.Equal(2, notice.Count);

        notice.Report();

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
    }

    /// <summary>Only the accumulator that held samples is named.</summary>
    [Fact]
    public async Task SampleFlushAtStop_StoreGoingDown_OnlyReadLatencyHeld_NamesOneSampleSet()
    {
        await using var store = new RefusingStore("57P03");
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var latency = new ReadLatencyAccumulator();
        latency.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);

        await DarlingWorker.FlushSamplesOnStopAsync(postgres, new CollectorCostAccumulator(), latency, TimeSpan.FromSeconds(30), logger, notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, notice.Count);
    }

    [Fact]
    public async Task SampleFlushAtStop_AnotherSqlState_StaysAWarning()
    {
        await using var store = new RefusingStore(TooManyConnections);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var (cost, latency) = Samples();

        await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger, notice);
        notice.Report();

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Information));
    }

    [Fact]
    public async Task SampleFlushAtStop_ConnectionRefused_GoesToTheNoticeNotAWarning()
    {
        await using var postgres = NpgsqlDataSource.Create(RefusingStore.ConnectionStringFor(RefusingStore.ClosedPort()));
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var (cost, latency) = Samples();

        await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger, notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(2, notice.Count);
    }

    /// <summary>The flush loop's last flush, which runs once the stop is signalled.</summary>
    [Theory]
    [MemberData(nameof(ShutdownStates))]
    public async Task StartHistoryLastFlush_StoreGoingDown_GoesToTheNoticeNotAWarning(string sqlState)
    {
        await using var store = new RefusingStore(sqlState);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        using var recorder = Recorder();
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        await recorder.RunAsync(postgres, logger, stopped.Token, stopNotice: notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, notice.Count);
    }

    [Fact]
    public async Task StartHistoryLastFlush_AnotherSqlState_StaysAWarning()
    {
        await using var store = new RefusingStore(TooManyConnections);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        using var recorder = Recorder();
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        await recorder.RunAsync(postgres, logger, stopped.Token, stopNotice: notice);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, notice.Count);
    }

    [Fact]
    public async Task StartHistoryLastFlush_ConnectionRefused_GoesToTheNoticeNotAWarning()
    {
        await using var postgres = NpgsqlDataSource.Create(RefusingStore.ConnectionStringFor(RefusingStore.ClosedPort()));
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        using var recorder = Recorder();
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        await recorder.RunAsync(postgres, logger, stopped.Token, stopNotice: notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, notice.Count);
    }

    /// <summary>
    /// The store goes down between the open and the first write: the connection opens, the collector-cost insert is answered with 57P01 and
    /// the connection is lost, and opening it again for the read-latency flush is refused. Each sample set is named once and nothing warns.
    /// </summary>
    [Fact]
    public async Task SampleFlushAtStop_StoreGoesDownAfterTheOpen_NamesEachSampleSetOnce()
    {
        await using var store = new RefusingStore("57P01", statementsBeforeRefusing: 0);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var (cost, latency) = Samples();

        await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger, notice);

        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(2, notice.Count);
        /* The open succeeded and a statement was sent on it: the failure was the insert's, not the connect's. */
        Assert.Equal(1, store.StatementsRefused);
    }

    /// <summary>
    /// The store goes down between two statements of one sightings flush: the first statement's rows land, the second meets 57P01, and the
    /// notice counts only the rows that were not written.
    /// </summary>
    [Fact]
    public async Task SightingsFlushAtStop_StoreGoesDownAfterTheFirstStatement_CountsOnlyTheUnwrittenRows()
    {
        await using var store = new RefusingStore("57P01", statementsBeforeRefusing: 1);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var notice = new StoreStopNotice(logger);
        var rows = Enumerable.Range(0, QueryTagSightingWriter.RowsPerStatement + 1)
            .Select(i => new QueryTagSightingWriter.CollapsedSighting { ServerId = 1, Kind = "hash", Key = "k" + i.ToString("D6", CultureInfo.InvariantCulture), Count = 1 })
            .ToList();

        var written = await QueryTagSightingWriter.WriteAsync(postgres, rows, logger, CancellationToken.None, stopNotice: notice);

        Assert.Equal(QueryTagSightingWriter.RowsPerStatement, written);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(1, notice.Count);
        Assert.Equal(1, store.StatementsRefused);

        notice.Report();

        /* The only number in the line is the one row the second statement carried. */
        Assert.Equal("1", new string(logger.Lines.Single().Where(char.IsDigit).ToArray()));
    }

    /// <summary>
    /// The start's store-retry arms flush right after logging that the store cannot be reached. A store that is not up yet is no second
    /// Warning there, and it leaves the recorder's own first-failure Warning unspent: the minute flush that later meets the same store warns.
    /// </summary>
    [Fact]
    public async Task StartHistoryFlush_WhileTheStartRetries_StoreNotUpYet_LogsNoWarning()
    {
        await using var store = new RefusingStore("57P03");
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        using var recorder = Recorder();

        var wrote = await recorder.FlushAsync(postgres, logger, CancellationToken.None, startIsRetrying: true);

        Assert.False(wrote);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));

        await recorder.FlushAsync(postgres, logger, CancellationToken.None);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task StartHistoryFlush_WhileTheStartRetries_AnotherSqlState_StaysAWarning()
    {
        await using var store = new RefusingStore(TooManyConnections);
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        using var recorder = Recorder();

        await recorder.FlushAsync(postgres, logger, CancellationToken.None, startIsRetrying: true);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>The whole stop against one store going down: three writers, one notice, one Information line and no Warning.</summary>
    [Fact]
    public async Task AllThreeStopWrites_StoreGoingDown_ReadAsOneInformationLine()
    {
        await using var store = new RefusingStore("57P01");
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        var buffer = new QueryTagSightingBuffer(postgres, logger);
        await buffer.AppendAsync(1, ThreeSightings(), CancellationToken.None);
        using var recorder = Recorder();
        var (cost, latency) = Samples();
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        using (var notice = new StoreStopNotice(logger))
        {
            await DarlingWorker.DrainQueryTagSightingsAsync(mapPass: null, buffer, notice);
            await recorder.RunAsync(postgres, logger, stopped.Token, stopNotice: notice);
            await DarlingWorker.FlushSamplesOnStopAsync(postgres, cost, latency, TimeSpan.FromSeconds(30), logger, notice);
            Assert.Equal(4, notice.Count);

            notice.Report();

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        }

        /* Leaving the scope reports only what was added after the last report: nothing here, so still one line. */
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    /// <summary>A way out of the worker that never reaches its own report still says the line, when the notice's scope ends.</summary>
    [Fact]
    public async Task NoticeLeftUnreported_ReportsWhenItsScopeEnds()
    {
        await using var store = new RefusingStore("57P01");
        await using var postgres = NpgsqlDataSource.Create(store.ConnectionString);
        var logger = new CapturingTestLogger();
        using var recorder = Recorder();
        using var stopped = new CancellationTokenSource();
        await stopped.CancelAsync();

        using (var notice = new StoreStopNotice(logger))
        {
            await using var loop = recorder.StartFlushLoop(postgres, logger, stopped.Token, stopNotice: notice);
            await loop.StopAsync();
            Assert.Equal(0, logger.CountAtLevel(LogLevel.Information));
        }

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Information));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void NothingAdded_ReportsNothing()
    {
        var logger = new CapturingTestLogger();
        using (var notice = new StoreStopNotice(logger))
        {
            notice.Report();
        }

        Assert.Empty(logger.Lines);
    }

    /// <summary>
    /// A listener that answers every PostgreSQL connection with one error and closes it: what a client sees from a store that is
    /// shutting down, starting up or refusing the login. It declines an encryption request, reads the startup packet and sends an
    /// ErrorResponse with the SQLSTATE given.
    /// <para>With <c>statementsBeforeRefusing</c> it is a store that goes down mid-write instead: it lets the connection open, answers that
    /// many statements as written, answers the next one with the SQLSTATE given and closes, and from then on refuses every new connection
    /// with 57P03, as a postmaster that is shutting down does.</para>
    /// </summary>
    private sealed class RefusingStore : IAsyncDisposable
    {
        private const int SslRequestCode = 80877103;
        private const int GssEncryptionRequestCode = 80877104;

        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly string _sqlState;
        private readonly bool _opens;
        private readonly Task _accepting;
        private int _statementsLeft;
        private int _statementsRefused;
        private int _down;

        public RefusingStore(string sqlState, int statementsBeforeRefusing = -1)
        {
            _sqlState = sqlState;
            _opens = statementsBeforeRefusing >= 0;
            _statementsLeft = Math.Max(statementsBeforeRefusing, 0);
            _listener.Start();
            _accepting = AcceptAsync();
        }

        /// <summary>How many statements were answered with the error, on a connection that had opened.</summary>
        public int StatementsRefused => Volatile.Read(ref _statementsRefused);

        /* No type loading: the open then sends no statement of its own, so the first statement the listener sees is the writer's. */
        public string ConnectionString =>
            ConnectionStringFor(((IPEndPoint)_listener.LocalEndpoint).Port) + (_opens ? ";Server Compatibility Mode=NoTypeLoading" : string.Empty);

        public static string ConnectionStringFor(int port) =>
            $"Host=127.0.0.1;Port={port};Username=darling;Database=darling;SSL Mode=Disable;Timeout=15;Pooling=false";

        /// <summary>A loopback port nothing listens on: bound once to get a free number, then released.</summary>
        public static int ClosedPort()
        {
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (true)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    _ = AnswerAsync(client);
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException or InvalidOperationException)
            {
                /* The test is over. InvalidOperationException is Stop() landing between two accepts (#9950). */
            }
        }

        private async Task AnswerAsync(TcpClient client)
        {
            try
            {
                using (client)
                {
                    var stream = client.GetStream();
                    var header = new byte[8];
                    while (true)
                    {
                        await stream.ReadExactlyAsync(header, _stop.Token);
                        var length = BinaryPrimitives.ReadInt32BigEndian(header);
                        var code = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(4));
                        if (length == 8 && code is SslRequestCode or GssEncryptionRequestCode)
                        {
                            await stream.WriteAsync("N"u8.ToArray(), _stop.Token);
                            continue;
                        }

                        var rest = new byte[length - 8];
                        await stream.ReadExactlyAsync(rest, _stop.Token);
                        break;
                    }

                    if (!_opens || Volatile.Read(ref _down) == 1)
                    {
                        await RefuseAsync(client, stream, _opens ? "57P03" : _sqlState);
                        return;
                    }

                    await stream.WriteAsync(Handshake(), _stop.Token);
                    var replies = new List<byte>();
                    var prefix = new byte[5];
                    while (true)
                    {
                        /* One frontend message: a type byte, a length that counts itself, and the payload. */
                        await stream.ReadExactlyAsync(prefix, _stop.Token);
                        var payload = new byte[BinaryPrimitives.ReadInt32BigEndian(prefix.AsSpan(1)) - 4];
                        await stream.ReadExactlyAsync(payload, _stop.Token);
                        switch ((char)prefix[0])
                        {
                            case 'X':
                                return;
                            case 'P':
                                replies.AddRange(Message('1'));
                                break;
                            case 'B':
                                replies.AddRange(Message('2'));
                                break;
                            case 'D':
                                replies.AddRange(Message('n'));
                                break;
                            case 'E':
                                replies.AddRange(Message('C', Encoding.ASCII.GetBytes("INSERT 0 1\0")));
                                break;
                            case 'S':
                                /* The whole statement has been read, so the client is reading when the answer goes out. */
                                if (Interlocked.Decrement(ref _statementsLeft) < 0)
                                {
                                    Volatile.Write(ref _down, 1);
                                    Interlocked.Increment(ref _statementsRefused);
                                    await RefuseAsync(client, stream, _sqlState);
                                    return;
                                }

                                replies.AddRange(Message('Z', "I"u8.ToArray()));
                                await stream.WriteAsync(replies.ToArray(), _stop.Token);
                                replies.Clear();
                                break;
                        }
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
            {
                /* The client went away first. */
            }
        }

        private async Task RefuseAsync(TcpClient client, NetworkStream stream, string sqlState)
        {
            await stream.WriteAsync(ErrorResponse(sqlState), _stop.Token);
            await stream.FlushAsync(_stop.Token);
            client.Client.Shutdown(SocketShutdown.Send);
            /* Wait for the client to close its side, so the error is read before the socket goes. */
            var rest = new byte[256];
            while (await stream.ReadAsync(rest, _stop.Token) > 0)
            {
            }
        }

        private static byte[] Message(char type, byte[]? payload = null)
        {
            payload ??= [];
            var message = new byte[5 + payload.Length];
            message[0] = (byte)type;
            BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1), 4 + payload.Length);
            payload.CopyTo(message, 5);
            return message;
        }

        /* AuthenticationOk, the parameters a client reads at connect, a backend key and ReadyForQuery. */
        private static byte[] Handshake()
        {
            var bytes = new List<byte>();
            bytes.AddRange(Message('R', new byte[4]));
            foreach (var (name, value) in new[]
            {
                ("server_version", "17.0"), ("server_encoding", "UTF8"), ("client_encoding", "UTF8"), ("integer_datetimes", "on"),
                ("standard_conforming_strings", "on"), ("DateStyle", "ISO, MDY"), ("TimeZone", "UTC"),
            })
            {
                bytes.AddRange(Message('S', Encoding.ASCII.GetBytes(name + "\0" + value + "\0")));
            }

            bytes.AddRange(Message('K', new byte[8]));
            bytes.AddRange(Message('Z', "I"u8.ToArray()));
            return bytes.ToArray();
        }

        private static byte[] ErrorResponse(string sqlState)
        {
            var fields = new List<byte>();
            void Field(char type, string value)
            {
                fields.Add((byte)type);
                fields.AddRange(Encoding.ASCII.GetBytes(value));
                fields.Add(0);
            }

            Field('S', "FATAL");
            Field('V', "FATAL");
            Field('C', sqlState);
            Field('M', "the store answered " + sqlState);
            fields.Add(0);

            var message = new byte[1 + 4 + fields.Count];
            message[0] = (byte)'E';
            BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1), 4 + fields.Count);
            fields.CopyTo(message, 5);
            return message;
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _accepting;
            _stop.Dispose();
        }
    }
}
