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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store's pseudonym key (#6066), read once over the store owner's connection and kept in this process's memory. The key is
/// the one row of <c>config.pseudonym_key</c> (V198), which no role but the store owner can read. It is a singleton in the service's
/// services and a service parameter only: <b>no <c>[McpServerTool]</c> method takes it</b> (<c>PseudonymKeySourceTests</c> pins that by
/// reflection), the MCP host's filter, the brief composer and the worker's webhook dispatcher take it as <see cref="IPseudonymKeySource"/>,
/// and it hands out a <see cref="PseudonymKey"/>, which has no member that returns its bytes to a caller outside the product's assemblies.
/// The key is never logged, returned, journaled or put in an error: a failed read logs the failure's type and message only.
/// <para>Until the worker has published the owner's data source (<see cref="StoreOwnerSource"/>), or while a read fails, <see cref="GetAsync"/>
/// answers null, so a <c>metadataOnly</c> answer numbers logins per answer (still no name); it is asked again on the next answer, but at
/// most once every <see cref="RetryAfterMilliseconds"/> after a failure, so a store that is down does not add a wait to every answer.</para>
/// </summary>
public sealed class PseudonymKeySource : IPseudonymKeySource, IDisposable
{
    /// <summary>The deadline of the one read, in seconds.</summary>
    public const int ReadTimeoutSeconds = 10;

    /// <summary>The least time between two failed reads, in milliseconds.</summary>
    public const int RetryAfterMilliseconds = 30_000;

    private const string ReadSql = "SELECT key FROM config.pseudonym_key WHERE id = 1";

    private readonly StoreOwnerSource _owner;
    private readonly ILogger? _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile PseudonymKey? _key;
    private long _nextAttemptAtTick;
    private int _warned;

    /// <param name="owner">Where the worker publishes the store owner's data source.</param>
    /// <param name="logger">Where a failed read is warned about, once.</param>
    public PseudonymKeySource(StoreOwnerSource owner, ILogger<PseudonymKeySource>? logger = null)
    {
        _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        _logger = logger;
        if (logger is not null) PseudonymKeyNotice.Sink = message => logger.LogWarning("{Message}", message);
    }

    /// <inheritdoc />
    public async ValueTask<PseudonymKey?> GetAsync(CancellationToken cancellationToken = default)
    {
        PseudonymKey? known = _key;
        if (known is not null) return known;

        NpgsqlDataSource? postgres = _owner.Read();
        if (postgres is null || Environment.TickCount64 < Interlocked.Read(ref _nextAttemptAtTick)) return null;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_key is not null) return _key;
            if (Environment.TickCount64 < Interlocked.Read(ref _nextAttemptAtTick)) return null;

            try
            {
                await using var command = postgres.CreateCommand(ReadSql);
                command.CommandTimeout = ReadTimeoutSeconds;
                object? value = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (value is byte[] { Length: PseudonymKey.Length } bytes)
                {
                    _key = new PseudonymKey(bytes);
                    Array.Clear(bytes);
                    return _key;
                }

                Failed("the store has no usable pseudonym key row");
                return null;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
#pragma warning disable CA1031 // an unreadable key means "no key": the answer is numbered per answer, never shown with the name
            catch (Exception ex)
#pragma warning restore CA1031
            {
                Failed(ex.GetType().Name + ": " + ex.Message);
                return null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose() => _gate.Dispose();

    private void Failed(string reason)
    {
        Interlocked.Exchange(ref _nextAttemptAtTick, Environment.TickCount64 + RetryAfterMilliseconds);
        if (Interlocked.Exchange(ref _warned, 1) == 0)
        {
            PseudonymKeyNotice.Spent();
            _logger?.LogWarning(
                "The pseudonym key could not be read from the store ({Reason}). Under mcp.dataDetail metadataOnly, logins and hosts are numbered per answer until it can; every name stays hidden. It is tried again on a later answer.",
                reason);
        }
    }
}
