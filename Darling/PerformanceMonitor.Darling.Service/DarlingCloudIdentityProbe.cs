/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The store host's cloud instance identity (#4214 part 2b): the EC2 instance type or Azure VM size,
/// when this host runs on one of those two clouds. <see cref="Provider"/> is <c>"aws"</c> or <c>"azure"</c>, or
/// null when nothing was found — a failed probe is never an error (ruling 4), just an empty answer. No
/// disk-class mapping or other derived fact rides along (ruling 4): just the provider and the raw instance
/// type/VM size string.</summary>
internal readonly record struct CloudIdentity(string? Provider, string? InstanceType)
{
    /// <summary>Nothing found — a timeout, a non-2xx response, a redirect, an oversized body, or a value that
    /// fails <see cref="DarlingCloudIdentityProbe.ValuePattern"/> all collapse to this one result (ruling 2/4).</summary>
    internal static readonly CloudIdentity None = new(null, null);
}

/// <summary>
/// Probes the link-local instance metadata endpoint (169.254.169.254) for the cloud instance type/VM size this
/// store host runs on (#4214 part 2b). Called ONLY from <see cref="DarlingStoreHostProfile.GatherAsync"/>
/// (ruling 3) — never from <see cref="DarlingStoreHostProfile.GatherStartupProfileAsync"/> or anything
/// <c>DarlingWorker</c> runs at service start, so a host with no metadata endpoint never pays this probe's cost
/// on every collection cycle; <c>get_store_host</c>'s own cache (<see cref="Mcp.StoreHostProfileCache"/>) means
/// one probe per cache fill from there too. <see cref="DarlingCloudIdentityProbeSourcePinTests"/> pins both of
/// those never-called claims against the actual source.
///
/// <para><b>Safety (ruling 2).</b> <see cref="CreateHandler"/> builds a handler used for nothing else in the
/// process: <c>UseProxy = false</c> (a configured proxy must never carry these requests) and
/// <c>AllowAutoRedirect = false</c> (a redirect off the link-local address is never legitimate metadata, and a
/// followed hop could leave the link-local address entirely). Every request targets the link-local IP literal
/// <see cref="MetadataHost"/> directly — no DNS name is ever resolved for this probe. The whole probe — the
/// EC2 token PUT, the EC2 instance-type GET, and, only when EC2 found nothing, the Azure GET — shares ONE
/// <see cref="ProbeBudget"/> (200 ms) from a single cancellation source, not a per-request timeout, so a slow
/// first hop cannot spend the budget twice. A response is read only up to <see cref="MaxResponseBytes"/> (256)
/// bytes and rejected past that cap. The final value must match <see cref="ValuePattern"/> or it does not
/// count. ANY failure — timeout, non-2xx status, a redirect, an oversized body, a pattern mismatch, a thrown
/// exception — returns <see cref="CloudIdentity.None"/> rather than throwing (ruling 4): a caller never needs
/// its own try/catch around this.</para>
/// </summary>
internal static class DarlingCloudIdentityProbe
{
    /// <summary>The link-local instance metadata address both clouds use. Referenced from doc comments only —
    /// every request URL below is written out as its own literal so nothing here risks a runtime string-concat
    /// mistake landing on a different host.</summary>
    internal const string MetadataHost = "169.254.169.254";

    private const string Ec2TokenUrl = "http://169.254.169.254/latest/api/token";
    private const string Ec2InstanceTypeUrl = "http://169.254.169.254/latest/meta-data/instance-type";
    private const string AzureVmSizeUrl =
        "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text";

    private const string Ec2TokenTtlHeader = "X-aws-ec2-metadata-token-ttl-seconds";
    private const string Ec2TokenHeader = "X-aws-ec2-metadata-token";

    /// <summary>The TTL this probe asks for on its IMDSv2 token. The token is used once, for the very next
    /// request, and never persisted — the value only needs to outlive the round trip within
    /// <see cref="ProbeBudget"/>, so the shortest TTL IMDSv2 accepts is enough.</summary>
    private const string Ec2TokenTtlSeconds = "60";

    /// <summary>200 ms TOTAL for the whole probe (ruling 2) — the EC2 attempt (token PUT + instance-type GET)
    /// and, when that finds nothing, the Azure GET all share this one budget.</summary>
    private static readonly TimeSpan ProbeBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>The response cap (ruling 2): a body is read only up to this many bytes, then rejected as
    /// oversized. Every real value here — an EC2 instance type, an EC2 IMDSv2 token, an Azure VM size — is
    /// well under this.</summary>
    private const int MaxResponseBytes = 256;

    /// <summary>The only values ruling 2 accepts for the final reported instance type/VM size: 1-64 characters
    /// of the set every real value is drawn from. Anything else — empty, oversized, or carrying a character
    /// neither cloud's naming scheme uses — counts as "not found", the same as a network failure. Deliberately
    /// NOT applied to the intermediate EC2 token (a base64-ish string that legitimately carries <c>+</c>,
    /// <c>/</c> and <c>=</c>, none of which this pattern allows) — only the value this probe actually reports.</summary>
    internal static readonly Regex ValuePattern = new(@"^[A-Za-z0-9._-]{1,64}$", RegexOptions.Compiled);

    /// <summary>The production entry point: builds this probe's own handler (ruling 2) and runs it.</summary>
    internal static async Task<CloudIdentity> ProbeAsync(CancellationToken cancellationToken)
    {
        /* The two-argument overload never disposes the handler it is given (tests pass their own fake), so
           the production path owns and disposes the handler it creates. */
        using var handler = CreateHandler();
        return await ProbeAsync(handler, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>This probe's own handler (ruling 2) — never a handler shared with any other outbound call in
    /// the process. <see cref="DarlingCloudIdentityProbeSourcePinTests"/> pins these two settings against the
    /// source text.</summary>
    internal static HttpMessageHandler CreateHandler() => new SocketsHttpHandler
    {
        UseProxy = false,
        AllowAutoRedirect = false,
    };

    /// <summary>The handler-injectable core a fake <see cref="HttpMessageHandler"/> drives in tests.</summary>
    internal static async Task<CloudIdentity> ProbeAsync(HttpMessageHandler handler, CancellationToken cancellationToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(ProbeBudget);

        using var client = new HttpClient(handler, disposeHandler: false) { Timeout = Timeout.InfiniteTimeSpan };

        try
        {
            var ec2 = await TryEc2Async(client, budget.Token).ConfigureAwait(false);
            if (ec2 is not null)
            {
                return new CloudIdentity("aws", ec2);
            }

            var azure = await TryAzureAsync(client, budget.Token).ConfigureAwait(false);
            return azure is not null ? new CloudIdentity("azure", azure) : CloudIdentity.None;
        }
        catch
        {
            /* Ruling 4: a failure here is never an error. A timeout (OperationCanceledException off the budget
               token), a connection failure, a malformed response header — all the same "not found" answer. */
            return CloudIdentity.None;
        }
    }

    /// <summary>EC2's IMDSv2 round trip: a PUT for a short-lived token, then a GET carrying it. No IMDSv1
    /// fallback — the issue's ruling names IMDSv2 only, and an unauthenticated v1 GET would be a second way to
    /// reach the same data that this probe deliberately does not add.</summary>
    private static async Task<string?> TryEc2Async(HttpClient client, CancellationToken cancellationToken)
    {
        using var tokenRequest = new HttpRequestMessage(HttpMethod.Put, Ec2TokenUrl);
        tokenRequest.Headers.Add(Ec2TokenTtlHeader, Ec2TokenTtlSeconds);

        using var tokenResponse = await client
            .SendAsync(tokenRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        var token = await ReadCappedValueAsync(tokenResponse, cancellationToken).ConfigureAwait(false);
        if (token is null)
        {
            return null;
        }

        using var metadataRequest = new HttpRequestMessage(HttpMethod.Get, Ec2InstanceTypeUrl);
        metadataRequest.Headers.Add(Ec2TokenHeader, token);

        using var metadataResponse = await client
            .SendAsync(metadataRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        var value = await ReadCappedValueAsync(metadataResponse, cancellationToken).ConfigureAwait(false);
        return value is not null && ValuePattern.IsMatch(value) ? value : null;
    }

    /// <summary>Azure's single unauthenticated GET, gated on the <c>Metadata: true</c> header every Azure
    /// metadata endpoint requires.</summary>
    private static async Task<string?> TryAzureAsync(HttpClient client, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, AzureVmSizeUrl);
        request.Headers.Add("Metadata", "true");

        using var response = await client
            .SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
            .ConfigureAwait(false);
        var value = await ReadCappedValueAsync(response, cancellationToken).ConfigureAwait(false);
        return value is not null && ValuePattern.IsMatch(value) ? value : null;
    }

    /// <summary>Reads a response body capped at <see cref="MaxResponseBytes"/> (ruling 2): a non-2xx status — a
    /// redirect included, since <see cref="CreateHandler"/> disables auto-redirect so a 3xx surfaces here as
    /// its own status rather than a followed hop — or a body over the cap both return null. The trimmed text is
    /// returned un-pattern-checked; callers apply <see cref="ValuePattern"/> themselves, since the EC2 token
    /// (never pattern-checked) and the final instance-type/vmSize value (always pattern-checked) share this one
    /// capped read.</summary>
    private static async Task<string?> ReadCappedValueAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using (stream.ConfigureAwait(false))
        {
            var buffer = new byte[MaxResponseBytes + 1];
            var total = 0;
            int read;
            while (total < buffer.Length
                   && (read = await stream.ReadAsync(buffer.AsMemory(total, buffer.Length - total), cancellationToken).ConfigureAwait(false)) > 0)
            {
                total += read;
            }

            if (total > MaxResponseBytes)
            {
                return null;
            }

            var text = Encoding.UTF8.GetString(buffer, 0, total).Trim();
            return text.Length == 0 ? null : text;
        }
    }
}
