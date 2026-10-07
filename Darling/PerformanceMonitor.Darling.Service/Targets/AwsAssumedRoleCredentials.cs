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
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Credentials;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// AWS credentials for one assumed role (#5452): the credentials STS hands back for an <see cref="AwsRoleKey"/>,
/// refreshed before they expire. One instance serves every client for that key, so many short-lived RDS and
/// Performance Insights clients share one STS call per refresh. It is our own class, not the SDK's
/// <c>AssumeRoleAWSCredentials</c>: that one calls STS in the process's own region (the wrong partition for an
/// aws-cn or aws-us-gov target), finds STS by reflection (no seam for a fake), and ends its background refresh in an
/// unobserved exception.
///
/// <para><b>Serving.</b> Credentials that are still valid are served at once and never wait on a refresh: past the
/// refresh point one caller takes the gate with a zero-wait try (<c>WaitAsync(0)</c>) and refreshes, and every other
/// caller keeps using the credentials it already has. Only a caller with nothing valid to use waits on the gate, and
/// re-checks inside it, so a burst makes one STS call. The refresh point is 15 minutes before expiry, but never sooner
/// than <see cref="MinimumRefreshInterval"/> after the credentials were received.</para>
///
/// <para><b>Expiry is receipt time plus <see cref="DurationSeconds"/>.</b> The response's own <c>Expiration</c> is
/// not read: it is a time on AWS's clock, compared here against the host's, and it can be absent. The same rule applies
/// when it is null.</para>
///
/// <para><b>Failure.</b> A failed refresh is turned into an <see cref="AwsRoleAssumeException"/> (the external ID is
/// scrubbed, the SDK exception is dropped) and remembered for <see cref="FailureCacheSeconds"/> seconds, so a denied
/// role costs STS at most one call a minute. During that time a caller gets the old credentials if they have more than
/// a minute left, and otherwise a NEW exception made from the remembered facts: the same instance is never thrown
/// twice. The cached failure belongs to the region it happened in; a call from another region of the same role tries
/// again. The call is bounded: the STS client has a timeout and a retry limit, and the whole refresh runs under a
/// deadline.</para>
///
/// <para><b>Region.</b> A refresh calls STS in the region of the call that triggered it, so the endpoint is in the
/// target's own partition (the SDK has no global STS endpoint to fall back on in 4.0.101.1). Session tokens from
/// regional STS are valid in every region of the partition, so the credentials serve targets in any of them.
/// <see cref="ForRegion"/> binds a region to a handle the SDK client can hold.</para>
///
/// <para><b>The source.</b> The credentials that call STS come from the default chain (<c>AWS_PROFILE</c>, the
/// environment, the instance role), read again at every refresh so a rotated source is picked up. They are resolved
/// BEFORE the call, so "the host has no credentials" is told apart from "STS refused".</para>
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable",
    Justification = "The gate is a SemaphoreSlim that is only waited on with WaitAsync, which never creates its kernel wait handle, so there is nothing to release; the cache drops entries with no disposal point.")]
public sealed class AwsAssumedRoleCredentials : AWSCredentials
{
    /// <summary>How long each assumed-role session lasts. One hour also fits role chaining, which AWS caps at one hour.</summary>
    public const int DurationSeconds = 3600;

    /// <summary>How long before expiry a refresh starts.</summary>
    public static readonly TimeSpan RefreshLead = TimeSpan.FromMinutes(15);

    /// <summary>The soonest a refresh may start after credentials were received.</summary>
    public static readonly TimeSpan MinimumRefreshInterval = TimeSpan.FromMinutes(1);

    /// <summary>How long a failed refresh is remembered.</summary>
    public const int FailureCacheSeconds = 60;

    /// <summary>Old credentials must have more than this left to be served while a failure is remembered.</summary>
    public static readonly TimeSpan MinimumServedLife = TimeSpan.FromMinutes(1);

    /// <summary>The longest one STS refresh may take, retries included.</summary>
    public static readonly TimeSpan RefreshDeadline = TimeSpan.FromSeconds(30);

    /// <summary>The STS client's HTTP timeout.</summary>
    public static readonly TimeSpan StsTimeout = TimeSpan.FromSeconds(10);

    /// <summary>How many times the STS client retries an error that can be retried.</summary>
    public const int StsMaxErrorRetry = 1;

    /// <summary>The session name used when the store's install id is not known.</summary>
    public const string FallbackSessionName = "darling-collector";

    private static readonly TimeSpan FailureCacheTtl = TimeSpan.FromSeconds(FailureCacheSeconds);

    private readonly Func<RegionEndpoint, AWSCredentials, IAmazonSecurityTokenService> _stsFactory;
    private readonly Func<RegionEndpoint, Task<AWSCredentials>> _source;
    private readonly Func<string?>? _installId;
    private readonly TimeProvider _clock;
    private readonly TimeSpan _refreshDeadline;
    private readonly ILogger? _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile Grant? _grant;
    private volatile Failure? _failure;
    private volatile RegionEndpoint _lastRegion;

    /// <param name="key">The role and external ID.</param>
    /// <param name="defaultRegion">The STS region for a call that names none: the SDK's own, parameterless
    /// <c>GetCredentialsAsync()</c>. <see cref="ForRegion"/> overrides it per call.</param>
    /// <param name="stsFactory">Builds the STS client from a region and the source credentials. Default: a real client
    /// with <see cref="CreateStsConfig"/>.</param>
    /// <param name="source">The credentials that call STS, for a region. Default: the SDK's default chain.</param>
    /// <param name="installId">The store's install id, for the session name. Read at each refresh.</param>
    /// <param name="clock">The clock the expiry arithmetic reads. Default: the system clock.</param>
    /// <param name="refreshDeadline">The longest one refresh may take. Default: <see cref="RefreshDeadline"/>.</param>
    /// <param name="logger">Gets one warning line for each failed refresh, with the already-scrubbed message.</param>
    public AwsAssumedRoleCredentials(
        AwsRoleKey key,
        RegionEndpoint defaultRegion,
        Func<RegionEndpoint, AWSCredentials, IAmazonSecurityTokenService>? stsFactory = null,
        Func<RegionEndpoint, Task<AWSCredentials>>? source = null,
        Func<string?>? installId = null,
        TimeProvider? clock = null,
        TimeSpan? refreshDeadline = null,
        ILogger? logger = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key.RoleArn);
        ArgumentNullException.ThrowIfNull(defaultRegion);
        Key = key;
        _lastRegion = defaultRegion;
        _stsFactory = stsFactory ?? DefaultStsFactory;
        _source = source ?? DefaultSource;
        _installId = installId;
        _clock = clock ?? TimeProvider.System;
        _refreshDeadline = refreshDeadline ?? RefreshDeadline;
        _logger = logger;
    }

    /// <summary>The role and external ID these credentials are for.</summary>
    public AwsRoleKey Key { get; }

    /// <summary>The credentials for this role, refreshed on demand, STS called in the default region.</summary>
    public override ImmutableCredentials GetCredentials() => GetCredentialsAsync().GetAwaiter().GetResult();

    /// <inheritdoc cref="GetCredentials"/>
    public override Task<ImmutableCredentials> GetCredentialsAsync() => GetCredentialsAsync(_lastRegion, CancellationToken.None);

    /// <summary>A handle to hand an SDK client: it serves these credentials and refreshes in <paramref name="stsRegion"/>.</summary>
    public AWSCredentials ForRegion(RegionEndpoint stsRegion)
    {
        ArgumentNullException.ThrowIfNull(stsRegion);
        _lastRegion = stsRegion;
        return new RegionHandle(this, stsRegion);
    }

    /// <summary>
    /// The session name for an install id: <c>darling-</c> and the first 12 hex digits of the id, lowercase, hyphens
    /// removed (20 characters, inside AWS's <c>[\w+=,.@-]{2,64}</c>). CloudTrail in the role's account then shows
    /// which Darling installation assumed the role, without the host's machine name. An id with fewer than 12 hex
    /// digits, or with anything else in it, gives <see cref="FallbackSessionName"/>.
    /// </summary>
    public static string SessionNameFor(string? installId)
    {
        if (string.IsNullOrWhiteSpace(installId))
        {
            return FallbackSessionName;
        }

        var hex = installId.Trim().Replace("-", string.Empty, StringComparison.Ordinal).ToLowerInvariant();
        if (hex.Length < 12)
        {
            return FallbackSessionName;
        }

        foreach (var c in hex)
        {
            if (!(c is >= '0' and <= '9' || c is >= 'a' and <= 'f'))
            {
                return FallbackSessionName;
            }
        }

        return "darling-" + hex[..12];
    }

    /// <summary>
    /// The STS client settings for a region: the regional endpoint (a global one does not exist in this SDK), a
    /// bounded HTTP timeout and a small retry limit.
    /// </summary>
    internal static AmazonSecurityTokenServiceConfig CreateStsConfig(RegionEndpoint region) => new()
    {
        RegionEndpoint = region,
        Timeout = StsTimeout,
        MaxErrorRetry = StsMaxErrorRetry,
    };

    /// <summary>Serves the cached credentials, or refreshes them in <paramref name="stsRegion"/> when they are due.</summary>
    public async Task<ImmutableCredentials> GetCredentialsAsync(RegionEndpoint stsRegion, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stsRegion);

        var now = _clock.GetUtcNow();
        var grant = _grant;
        if (grant is not null && now < grant.RefreshAt)
        {
            return grant.Credentials;
        }

        if (RememberedFailureFor(stsRegion, now) is { } early)
        {
            return ServeOrThrow(grant, early, now);
        }

        /* Past the refresh point but still valid: try the gate without waiting, and let the loser use what it has. */
        if (grant is not null && now < grant.ValidUntil)
        {
            if (!await _gate.WaitAsync(0, CancellationToken.None).ConfigureAwait(false))
            {
                return grant.Credentials;
            }
        }
        else
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            now = _clock.GetUtcNow();
            grant = _grant;
            if (grant is not null && now < grant.RefreshAt)
            {
                return grant.Credentials;
            }

            if (RememberedFailureFor(stsRegion, now) is { } inside)
            {
                return ServeOrThrow(grant, inside, now);
            }

            try
            {
                var fresh = await RefreshAsync(stsRegion, cancellationToken).ConfigureAwait(false);
                _grant = fresh;
                _failure = null;
                return fresh.Credentials;
            }
            catch (AwsRoleAssumeException failure)
            {
                var remembered = new Failure(_clock.GetUtcNow(), stsRegion.SystemName, failure);
                _failure = remembered;
                _logger?.LogWarning("{Message}", failure.Message);
                return ServeOrThrowFirst(grant, failure, _clock.GetUtcNow());
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private Failure? RememberedFailureFor(RegionEndpoint region, DateTimeOffset now)
    {
        var failure = _failure;
        return failure is not null
            && string.Equals(failure.Region, region.SystemName, StringComparison.Ordinal)
            && now < failure.At + FailureCacheTtl
                ? failure
                : null;
    }

    /// <summary>Old credentials with more than a minute left, or a new exception made from the remembered failure.</summary>
    private ImmutableCredentials ServeOrThrow(Grant? old, Failure failure, DateTimeOffset now)
    {
        if (old is not null && now < old.ValidUntil - MinimumServedLife)
        {
            return old.Credentials;
        }

        throw failure.ToException(Key);
    }

    /// <summary>Like <see cref="ServeOrThrow"/>, but the first throw is the exception the refresh just made.</summary>
    private static ImmutableCredentials ServeOrThrowFirst(Grant? old, AwsRoleAssumeException failure, DateTimeOffset now)
    {
        if (old is not null && now < old.ValidUntil - MinimumServedLife)
        {
            return old.Credentials;
        }

        throw failure;
    }

    private async Task<Grant> RefreshAsync(RegionEndpoint stsRegion, CancellationToken cancellationToken)
    {
        var region = stsRegion.SystemName;

        ImmutableCredentials sourceCredentials;
        try
        {
            var source = await _source(stsRegion).ConfigureAwait(false)
                ?? throw new AmazonClientException("The credential source returned no credentials.");
            sourceCredentials = await source.GetCredentialsAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw AwsRoleAssumeException.ForNoSourceIdentity(Key, region, ex);
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(_refreshDeadline);

        var request = new AssumeRoleRequest
        {
            RoleArn = Key.RoleArn,
            RoleSessionName = SessionNameFor(_installId?.Invoke()),
            DurationSeconds = DurationSeconds,
        };
        if (Key.HasExternalId)
        {
            request.ExternalId = Key.ExternalId;
        }

        AssumeRoleResponse? response;
        try
        {
            var stsCredentials = sourceCredentials.Token is { Length: > 0 }
                ? (AWSCredentials)new SessionAWSCredentials(sourceCredentials.AccessKey, sourceCredentials.SecretKey, sourceCredentials.Token)
                : new BasicAWSCredentials(sourceCredentials.AccessKey, sourceCredentials.SecretKey);
            using var sts = _stsFactory(stsRegion, stsCredentials);
            response = await sts.AssumeRoleAsync(request, deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            throw AwsRoleAssumeException.ForTimeout(Key, region, _refreshDeadline);
        }
        catch (Exception ex)
        {
            throw AwsRoleAssumeException.ForStsFailure(Key, region, ex);
        }

        var granted = response?.Credentials;
        if (granted is null || string.IsNullOrEmpty(granted.AccessKeyId) || string.IsNullOrEmpty(granted.SecretAccessKey)
            || string.IsNullOrEmpty(granted.SessionToken))
        {
            throw AwsRoleAssumeException.ForEmptyResponse(Key, region);
        }

        var received = _clock.GetUtcNow();
        var validUntil = received.AddSeconds(DurationSeconds);

        return new Grant(
            new ImmutableCredentials(granted.AccessKeyId, granted.SecretAccessKey, granted.SessionToken),
            validUntil, RefreshPointFor(received, validUntil));
    }

    /// <summary>
    /// When a refresh is due for credentials received at <paramref name="received"/> and valid until
    /// <paramref name="validUntil"/>: <see cref="RefreshLead"/> before expiry, but never sooner than
    /// <see cref="MinimumRefreshInterval"/> after receipt, so credentials that live only a short time cannot make every
    /// call refresh.
    /// </summary>
    internal static DateTimeOffset RefreshPointFor(DateTimeOffset received, DateTimeOffset validUntil)
    {
        var refreshAt = validUntil - RefreshLead;
        var soonest = received + MinimumRefreshInterval;
        return refreshAt < soonest ? soonest : refreshAt;
    }

    private static AmazonSecurityTokenServiceClient DefaultStsFactory(RegionEndpoint region, AWSCredentials credentials) =>
        new AmazonSecurityTokenServiceClient(credentials, CreateStsConfig(region));

    private static async Task<AWSCredentials> DefaultSource(RegionEndpoint region)
    {
        return await DefaultAWSCredentialsIdentityResolver.GetCredentialsAsync(CreateStsConfig(region)).ConfigureAwait(false);
    }

    private sealed record Grant(ImmutableCredentials Credentials, DateTimeOffset ValidUntil, DateTimeOffset RefreshAt);

    /// <summary>What a failed refresh left behind: the facts, never the exception, so every throw is a new one.</summary>
    private sealed class Failure
    {
        private readonly AwsRoleAssumeKind _kind;
        private readonly string _message;
        private readonly string? _code;
        private readonly string? _type;

        public Failure(DateTimeOffset at, string region, AwsRoleAssumeException from)
        {
            At = at;
            Region = region;
            _kind = from.Kind;
            _message = from.Message;
            _code = from.SourceErrorCode;
            _type = from.SourceExceptionType;
        }

        public DateTimeOffset At { get; }

        public string Region { get; }

        public AwsRoleAssumeException ToException(AwsRoleKey key) => AwsRoleAssumeException.Rebuild(_kind, key, Region, _message, _code, _type);
    }

    private sealed class RegionHandle : AWSCredentials
    {
        private readonly AwsAssumedRoleCredentials _owner;
        private readonly RegionEndpoint _region;

        public RegionHandle(AwsAssumedRoleCredentials owner, RegionEndpoint region)
        {
            _owner = owner;
            _region = region;
        }

        public override ImmutableCredentials GetCredentials() => GetCredentialsAsync().GetAwaiter().GetResult();

        public override Task<ImmutableCredentials> GetCredentialsAsync() => _owner.GetCredentialsAsync(_region, CancellationToken.None);
    }
}
