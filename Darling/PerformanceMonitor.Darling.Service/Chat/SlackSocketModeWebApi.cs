/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Chat;

/// <summary>#7657: the product's transport. Not built yet: every call answers that.</summary>
public sealed class SlackSocketModeWebApi : ISlackSocketModeTransport
{
    public static readonly TimeSpan CallTimeout = TimeSpan.FromSeconds(15);
    public const int MaxFrameBytes = 1024 * 1024;

    public SlackSocketModeWebApi(string? apiBase = null, HttpMessageHandler? handler = null)
    {
    }

    public Task<SlackAuthTest> AuthTestAsync(string botToken, CancellationToken cancellationToken) =>
        Task.FromResult(new SlackAuthTest(false, null, null, null, "not built"));

    public Task<SlackConnectionOpen> OpenConnectionAsync(string appToken, CancellationToken cancellationToken) =>
        Task.FromResult(new SlackConnectionOpen(false, null, "not built"));

    public Task<ISlackSocket> ConnectAsync(Uri url, CancellationToken cancellationToken) =>
        throw new NotSupportedException("not built");

    public override string ToString() => "SlackSocketModeWebApi";
}
