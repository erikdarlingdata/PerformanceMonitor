/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Net;
using Amazon.Runtime;
using Amazon.SecurityToken.Model;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>Why a per-server AWS role could not be used (#5452).</summary>
public enum AwsRoleAssumeKind
{
    /// <summary>The role is not on the allow list in darling.json, so it was never assumed.</summary>
    NotAllowed,

    /// <summary>The role's AWS partition is not the partition of the target's region.</summary>
    PartitionMismatch,

    /// <summary>STS refused: the role, its trust policy, the external ID or the host's permission is wrong.</summary>
    Denied,

    /// <summary>STS is not active in the target's region for the host's account.</summary>
    RegionDisabled,

    /// <summary>The host has no AWS credentials to assume the role with.</summary>
    NoSourceIdentity,

    /// <summary>AWS rejected the host's own credentials (expired, unknown, or a skewed clock).</summary>
    SourceCredentials,

    /// <summary>Any other failure: a timeout, a network error, an STS fault.</summary>
    Transient,
}

/// <summary>
/// The one exception every failure to assume a per-server AWS role comes out as (#5452). The credentials class turns
/// every STS or credential failure into this type, so the raw SDK exception, which can repeat the request, never
/// travels further.
///
/// <para><b>The external ID is never in it.</b> The message is either written here from the role ARN, the region and
/// <c>ExternalId = set|none</c>, or it carries SDK text with the external ID scrubbed out first. The raw SDK exception
/// is dropped (no <see cref="Exception.InnerException"/>); only its error code and type name are kept. A fresh instance
/// is made for every throw, including one that serves a cached failure, so no two throws share a stack trace.
/// <see cref="ToString"/> is written here and prints the kind, the message and the stack, nothing else.</para>
///
/// <para><see cref="Exception.Message"/> is the text for the collection log and the warning log.
/// <see cref="Find"/> walks the inner-exception chain, so an ingestor that wraps the failure still lets the worker see it.</para>
/// </summary>
public sealed class AwsRoleAssumeException : Exception
{
    private const int MaxChainDepth = 16;

    private AwsRoleAssumeException(
        AwsRoleAssumeKind kind, string roleArn, string region, bool externalIdSet, string message,
        string? sourceErrorCode, string? sourceExceptionType)
        : base(message)
    {
        Kind = kind;
        RoleArn = roleArn;
        Region = region;
        ExternalIdSet = externalIdSet;
        SourceErrorCode = sourceErrorCode;
        SourceExceptionType = sourceExceptionType;
    }

    /// <summary>What went wrong.</summary>
    public AwsRoleAssumeKind Kind { get; }

    /// <summary>The role ARN the server uses.</summary>
    public string RoleArn { get; }

    /// <summary>The target's AWS region.</summary>
    public string Region { get; }

    /// <summary>Whether an external ID was set for the role. The value is never kept.</summary>
    public bool ExternalIdSet { get; }

    /// <summary>The AWS error code of the failure, when AWS gave one. Scrubbed of the external ID.</summary>
    public string? SourceErrorCode { get; }

    /// <summary>The type name of the exception the SDK threw, with no message and no stack.</summary>
    public string? SourceExceptionType { get; }

    /// <summary>
    /// True for a failure the operator fixes by changing configuration: the worker reports it as PERMISSIONS rather
    /// than as a general ERROR. A rejected host credential, a missing one and a transient fault are ERROR.
    /// </summary>
    public bool IsConfiguration => Kind is AwsRoleAssumeKind.NotAllowed or AwsRoleAssumeKind.PartitionMismatch
        or AwsRoleAssumeKind.Denied or AwsRoleAssumeKind.RegionDisabled;

    /// <summary>The first <see cref="AwsRoleAssumeException"/> in <paramref name="ex"/> or its inner exceptions, or null.</summary>
    public static AwsRoleAssumeException? Find(Exception? ex)
    {
        var depth = 0;
        while (ex is not null && depth++ < MaxChainDepth)
        {
            if (ex is AwsRoleAssumeException found)
            {
                return found;
            }

            if (ex is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    var hit = Find(inner);
                    if (hit is not null)
                    {
                        return hit;
                    }
                }
            }

            ex = ex.InnerException;
        }

        return null;
    }

    public override string ToString()
    {
        var text = $"{GetType().FullName}: {Message} (kind {Kind}"
            + (SourceErrorCode is null ? string.Empty : ", error code " + SourceErrorCode)
            + (SourceExceptionType is null ? string.Empty : ", from " + SourceExceptionType)
            + ")";
        return StackTrace is null ? text : text + Environment.NewLine + StackTrace;
    }

    /// <summary>The role is not on the allow list.</summary>
    internal static AwsRoleAssumeException ForNotAllowed(AwsRoleKey key, string region) =>
        new(AwsRoleAssumeKind.NotAllowed, key.RoleArn, region, key.HasExternalId,
            $"The AWS role {key.RoleArn} on this server is not in allowedAwsRoles in darling.json, so it was not used. "
            + "Nothing was read this cycle. Add the role or its account id to the list and restart the service, or clear the role on this server.",
            null, null);

    /// <summary>The role's partition is not the region's.</summary>
    internal static AwsRoleAssumeException ForPartitionMismatch(AwsRoleKey key, string region, string rolePartition, string regionPartition) =>
        new(AwsRoleAssumeKind.PartitionMismatch, key.RoleArn, region, key.HasExternalId,
            $"Role {key.RoleArn} is in AWS partition {rolePartition}, but this target's region {region} is in partition {regionPartition}. "
            + "A role can be assumed only inside its own partition. Nothing was read this cycle. "
            + "Set a role from the target's partition on this server, or clear the role, or correct the server's host name if it names the wrong region.",
            null, null);

    /// <summary>The host has no AWS credentials to assume the role with.</summary>
    internal static AwsRoleAssumeException ForNoSourceIdentity(AwsRoleKey key, string region, Exception? raw) =>
        FromRaw(AwsRoleAssumeKind.NoSourceIdentity, key, region, raw,
            scrubbed => $"The monitoring host has no AWS credentials to assume role {key.RoleArn} with: {scrubbed}");

    /// <summary>The assume-role call failed. The raw exception is read for its code and type, then dropped.</summary>
    internal static AwsRoleAssumeException ForStsFailure(AwsRoleKey key, string region, Exception raw)
    {
        var code = (raw as AmazonServiceException)?.ErrorCode;
        var kind = raw is RegionDisabledException || StartsWithOrdinalIgnoreCase(code, "RegionDisabled")
            ? AwsRoleAssumeKind.RegionDisabled
            : StartsWithOrdinalIgnoreCase(code, "AccessDenied")
                ? AwsRoleAssumeKind.Denied
                : StartsWithOrdinalIgnoreCase(code, "ExpiredToken") || StartsWithOrdinalIgnoreCase(code, "InvalidClientTokenId")
                    || StartsWithOrdinalIgnoreCase(code, "SignatureDoesNotMatch")
                    ? AwsRoleAssumeKind.SourceCredentials
                    : AwsRoleAssumeKind.Transient;

        return FromRaw(kind, key, region, raw, scrubbed => kind switch
        {
            AwsRoleAssumeKind.RegionDisabled =>
                $"AWS STS is not active in region {region} for the monitoring host's AWS account, so role {key.RoleArn} could not be assumed. "
                + "Nothing was read this cycle. Enable that region for the AWS account that owns the role and for the monitoring host's account "
                + "(AWS account settings, Regions), or set a role in a region that is enabled, or correct the server's host name if it names the wrong region.",
            AwsRoleAssumeKind.Denied =>
                $"AWS refused to let the monitoring host assume role {key.RoleArn} "
                + (key.HasExternalId ? "(with the external ID set on this server)" : "(without an external ID)")
                + ". Nothing was read this cycle. One of these is wrong: the role does not exist, its trust policy does not trust the host's AWS identity, "
                + "the external ID does not match the trust policy's sts:ExternalId condition, or the host's identity has no sts:AssumeRole permission on the role. "
                + "AWS gives the same answer for all four.",
            AwsRoleAssumeKind.SourceCredentials =>
                $"AWS rejected the monitoring host's own credentials while it assumed role {key.RoleArn}"
                + (string.IsNullOrEmpty(code) ? string.Empty : $" ({Scrub(code, key.ExternalId)})")
                + ". The credentials the service runs with are expired or wrong, or the host's clock is off. Nothing was read this cycle. "
                + "Fix the host's credentials or clock; a new attempt is made within a minute.",
            _ => $"Assuming role {key.RoleArn}: {scrubbed}",
        });
    }

    /// <summary>The refresh did not finish inside its time limit.</summary>
    internal static AwsRoleAssumeException ForTimeout(AwsRoleKey key, string region, TimeSpan limit) =>
        new(AwsRoleAssumeKind.Transient, key.RoleArn, region, key.HasExternalId,
            $"Assuming role {key.RoleArn}: AWS STS did not answer within {limit.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture)} seconds.",
            null, typeof(TimeoutException).FullName);

    /// <summary>STS answered without usable credentials.</summary>
    internal static AwsRoleAssumeException ForEmptyResponse(AwsRoleKey key, string region) =>
        new(AwsRoleAssumeKind.Transient, key.RoleArn, region, key.HasExternalId,
            $"Assuming role {key.RoleArn}: AWS STS answered without credentials.", null, null);

    /// <summary>A new exception from the facts of an earlier one, for a throw that serves a cached failure.</summary>
    internal static AwsRoleAssumeException Rebuild(
        AwsRoleAssumeKind kind, AwsRoleKey key, string region, string message, string? code, string? type) =>
        new(kind, key.RoleArn, region, key.HasExternalId, message, code, type);

    /// <summary>
    /// Takes <paramref name="text"/> with every form of <paramref name="externalId"/> removed: as written, and as a URL
    /// or HTML encoder would write it, compared without regard to case. A null or empty ID leaves the text alone.
    /// </summary>
    internal static string Scrub(string? text, string? externalId)
    {
        if (string.IsNullOrEmpty(text) || string.IsNullOrEmpty(externalId))
        {
            return text ?? string.Empty;
        }

        var result = text;
        foreach (var form in new[]
                 {
                     externalId,
                     Uri.EscapeDataString(externalId),
                     WebUtility.UrlEncode(externalId),
                     WebUtility.HtmlEncode(externalId),
                 })
        {
            if (!string.IsNullOrEmpty(form))
            {
                result = result.Replace(form, "[external ID]", StringComparison.OrdinalIgnoreCase);
            }
        }

        return result;
    }

    private static AwsRoleAssumeException FromRaw(
        AwsRoleAssumeKind kind, AwsRoleKey key, string region, Exception? raw, Func<string, string> message)
    {
        var scrubbed = Scrub(raw?.Message, key.ExternalId);
        var code = (raw as AmazonServiceException)?.ErrorCode;
        return new AwsRoleAssumeException(
            kind, key.RoleArn, region, key.HasExternalId, message(scrubbed),
            code is null ? null : Scrub(code, key.ExternalId), raw?.GetType().FullName);
    }

    private static bool StartsWithOrdinalIgnoreCase(string? value, string prefix) =>
        value is not null && value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
}
