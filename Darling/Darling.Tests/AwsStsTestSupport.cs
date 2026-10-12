/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Microsoft.Extensions.Logging;

namespace Darling.Tests;

/// <summary>
/// A fake STS for the assumed-role credential tests (#5452): no AWS, no network. It answers
/// <c>AssumeRoleAsync</c> through a handler and records every request, the region it was built for and the credentials
/// it was given. Every other member of the interface is unsupported, which fails a test loudly if the code under test
/// starts calling something else.
/// </summary>
public class FakeStsProxy : DispatchProxy
{
    internal Func<AssumeRoleRequest, CancellationToken, Task<AssumeRoleResponse>> Handler { get; set; } = null!;

    internal List<AssumeRoleRequest> Requests { get; set; } = null!;

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        switch (targetMethod?.Name)
        {
            case "AssumeRoleAsync":
                var request = (AssumeRoleRequest)args![0]!;
                lock (Requests)
                {
                    Requests.Add(request);
                }

                return Handler(request, (CancellationToken)args[1]!);
            case "Dispose":
                return null;
            default:
                throw new NotSupportedException("FakeSts does not implement " + targetMethod?.Name);
        }
    }
}

/// <summary>Builds <see cref="FakeStsProxy"/> clients and the hooks the credentials class takes.</summary>
internal sealed class FakeSts
{
    private readonly object _gate = new();

    public FakeSts(Func<AssumeRoleRequest, CancellationToken, Task<AssumeRoleResponse>>? handler = null)
    {
        Handler = handler ?? ((_, _) => Task.FromResult(Grant()));
    }

    public Func<AssumeRoleRequest, CancellationToken, Task<AssumeRoleResponse>> Handler { get; set; }

    public List<AssumeRoleRequest> Requests { get; } = new();

    public List<string> Regions { get; } = new();

    public List<ImmutableCredentials> SourceCredentialsSeen { get; } = new();

    public int Calls
    {
        get
        {
            lock (Requests)
            {
                return Requests.Count;
            }
        }
    }

    public Func<RegionEndpoint, AWSCredentials, IAmazonSecurityTokenService> Factory => (region, credentials) =>
    {
        lock (_gate)
        {
            Regions.Add(region.SystemName);
            SourceCredentialsSeen.Add(credentials.GetCredentials());
        }

        var proxy = DispatchProxy.Create<IAmazonSecurityTokenService, FakeStsProxy>();
        var fake = (FakeStsProxy)(object)proxy;
        fake.Handler = (request, token) => Handler(request, token);
        fake.Requests = Requests;
        return proxy;
    };

    /// <summary>A source of credentials that always answers with the same example keys.</summary>
    public static Func<RegionEndpoint, Task<AWSCredentials>> Source { get; } =
        _ => Task.FromResult<AWSCredentials>(new BasicAWSCredentials("AKIAEXAMPLESOURCE", "example-source-secret"));

    /// <summary>An STS response with example credentials. <paramref name="expiration"/> is deliberately settable: the code must not read it.</summary>
    public static AssumeRoleResponse Grant(string token = "example-session-token", DateTime? expiration = null) => new()
    {
        Credentials = new Credentials
        {
            AccessKeyId = "ASIAEXAMPLEASSUMED",
            SecretAccessKey = "example-assumed-secret",
            SessionToken = token,
            Expiration = expiration,
        },
    };

    /// <summary>An STS error with an AWS error code.</summary>
    public static AmazonSecurityTokenServiceException Error(string code, string message) => new(message)
    {
        ErrorCode = code,
    };
}

/// <summary>A clock that moves only when a test moves it.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private DateTimeOffset _now;

    public ManualTimeProvider(DateTimeOffset? start = null)
    {
        _now = start ?? new DateTimeOffset(2026, 10, 7, 12, 0, 0, TimeSpan.Zero);
    }

    public override DateTimeOffset GetUtcNow() => _now;

    public void Advance(TimeSpan by) => _now += by;
}

/// <summary>A logger that keeps every formatted line and its exception.</summary>
internal sealed class CapturingAwsLogger : ILogger
{
    public List<string> Lines { get; } = new();

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (Lines)
        {
            Lines.Add($"{logLevel}: {formatter(state, exception)}" + (exception is null ? string.Empty : " | " + exception));
        }
    }

    public string All
    {
        get
        {
            lock (Lines)
            {
                return string.Join("\n", Lines);
            }
        }
    }
}
