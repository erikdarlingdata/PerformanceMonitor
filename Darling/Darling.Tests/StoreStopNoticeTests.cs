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
    /// </summary>
    private sealed class RefusingStore : IAsyncDisposable
    {
        private const int SslRequestCode = 80877103;
        private const int GssEncryptionRequestCode = 80877104;

        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new();
        private readonly string _sqlState;
        private readonly Task _accepting;

        public RefusingStore(string sqlState)
        {
            _sqlState = sqlState;
            _listener.Start();
            _accepting = AcceptAsync();
        }

        public string ConnectionString => ConnectionStringFor(((IPEndPoint)_listener.LocalEndpoint).Port);

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
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                /* The test is over. */
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

                    await stream.WriteAsync(ErrorResponse(), _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                    client.Client.Shutdown(SocketShutdown.Send);
                    /* Wait for the client to close its side, so the error is read before the socket goes. */
                    while (await stream.ReadAsync(header, _stop.Token) > 0)
                    {
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
            {
                /* The client went away first. */
            }
        }

        private byte[] ErrorResponse()
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
            Field('C', _sqlState);
            Field('M', "the store answered " + _sqlState);
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
