/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Security.Authentication;

namespace PerformanceMonitor.Common;

/// <summary>
/// Recognises a SQL Server connection that failed because the server's TLS certificate was rejected, and says so (#9692).
/// SqlClient reports the failure as "A connection was successfully established with the server, but then an error occurred
/// during the pre-login handshake ... error: 35 - An internal exception was caught". The certificate verdict is only in an
/// inner <see cref="AuthenticationException"/> (Linux, macOS, and SqlClient's managed network layer), or in the message itself
/// (Windows' native layer), so the text reads as a network fault. Darling's connection test and connect-failure log and Lite's
/// connection tests all call <see cref="Append"/>, so the wording lives here once.
/// Plain strings and BCL exception types only, so this project needs no SqlClient reference.
/// </summary>
public static class ServerCertificateTrustFailure
{
    /* SqlClient's managed network layer (Microsoft.Data.SqlClient 7). The chain message carries the status text, such as
       "[Status: UntrustedRoot]" or "[Status: NotTimeValid]". */
    private const string ChainValidationFailed = "Certificate failed chain validation";
    private const string NameMismatch = "Certificate name mismatch";

    /* The .NET TLS stack's own wording, when it rejects a certificate chain or its validation callback says no. */
    private const string RemoteCertificateInvalid = "The remote certificate is invalid";
    private const string RemoteCertificateRejected = "The remote certificate was rejected by the provided RemoteCertificateValidationCallback";

    /* Windows' native network layer puts the Schannel text in the SqlException message, as "(provider: SSL Provider, error: 0 - ...)". */
    private const string SslProvider = "SSL Provider";
    private const string WrongPrincipal = "The target principal name is incorrect";
    private const string SspiContext = "SSPI";

    /// <summary>What the exception chain says about the server certificate.</summary>
    private enum Verdict
    {
        None,

        /// <summary>The certificate chain does not end at a CA this machine trusts.</summary>
        UntrustedChain,

        /// <summary>The certificate was not issued to the name used to connect.</summary>
        NameMismatch,

        /// <summary>Both of the above.</summary>
        UntrustedChainAndNameMismatch,

        /// <summary>The certificate was rejected for a reason the text does not name, such as an expired certificate.</summary>
        Rejected,

        /// <summary>A rejection the text does not name, and the name mismatch as well.</summary>
        RejectedAndNameMismatch,
    }

    /// <summary>The way out that skips the check, for an entry that can take it.</summary>
    private const string SkipTheCheck = ", or set trustServerCertificate for this server knowing that it skips the check.";

    /// <summary>
    /// True when <paramref name="exception"/>, or an exception inside it (through <see cref="Exception.InnerException"/> and the
    /// children of an <see cref="AggregateException"/>), says the server certificate was rejected.
    /// </summary>
    public static bool IsMatch(Exception? exception) => Classify(exception) != Verdict.None;

    /// <summary>
    /// The sentence that says the server certificate was rejected and what fixes it, or null when
    /// <paramref name="exception"/> is some other failure. It names no certificate subject, because none of the exception
    /// shapes carries one. <paramref name="strict"/> is true when the connection used <c>encrypt_mode</c> Strict, which ignores
    /// trustServerCertificate, so the sentence names only the fixes that work under Strict. The connection tests add
    /// <see cref="StrictEncryptionHint"/>'s line for that case, and it names the way out (Mandatory with trustServerCertificate).
    /// </summary>
    public static string? Describe(Exception? exception, bool strict = false)
    {
        var problemAndFix = Classify(exception) switch
        {
            Verdict.UntrustedChain =>
                "The server's certificate chain is not trusted by this machine, so add the issuing CA to the system trust store",
            Verdict.NameMismatch =>
                "The server's certificate was not issued to the name used to connect, so connect with a name the certificate covers",
            Verdict.UntrustedChainAndNameMismatch =>
                "The server's certificate chain is not trusted by this machine and it was not issued to the name used to connect, "
                + "so add the issuing CA to the system trust store and connect with a name the certificate covers",
            Verdict.Rejected =>
                "This machine rejected the server's certificate, so check that it is current and issued by a CA in the system trust store",
            Verdict.RejectedAndNameMismatch =>
                "This machine rejected the server's certificate and it was not issued to the name used to connect, "
                + "so check that it is current and issued by a CA in the system trust store and connect with a name the certificate covers",
            _ => null,
        };

        return problemAndFix is null ? null : problemAndFix + (strict ? "." : SkipTheCheck);
    }

    /// <summary>
    /// <paramref name="failureText"/> with <see cref="Describe"/>'s sentence after it on the same line, or
    /// <paramref name="failureText"/> unchanged when the exception is some other failure.
    /// </summary>
    public static string Append(string failureText, Exception? exception, bool strict = false)
    {
        var sentence = Describe(exception, strict);
        return sentence is null ? failureText : failureText + " " + sentence;
    }

    private static Verdict Classify(Exception? exception)
    {
        var untrusted = false;
        var nameMismatch = false;
        var chainFailed = false;
        var callbackRejected = false;
        foreach (var message in MessagesIn(exception))
        {
            var mentionsCertificate = message.Contains("certificate", StringComparison.OrdinalIgnoreCase);
            if (mentionsCertificate
                && (message.Contains("UntrustedRoot", StringComparison.Ordinal)
                    || message.Contains("PartialChain", StringComparison.Ordinal)
                    || message.Contains("not trusted", StringComparison.OrdinalIgnoreCase)))
            {
                untrusted = true;
            }

            if (message.Contains(NameMismatch, StringComparison.Ordinal)
                || (message.Contains(SslProvider, StringComparison.Ordinal)
                    && message.Contains(WrongPrincipal, StringComparison.Ordinal)
                    && !message.Contains(SspiContext, StringComparison.Ordinal)))
            {
                nameMismatch = true;
            }

            if (message.Contains(ChainValidationFailed, StringComparison.Ordinal)
                || message.Contains(RemoteCertificateInvalid, StringComparison.Ordinal))
            {
                chainFailed = true;
            }

            /* The .NET TLS stack's wording whenever the validation callback says no, whatever the reason, so it names a
               rejection only when nothing more specific does. */
            if (message.Contains(RemoteCertificateRejected, StringComparison.Ordinal))
            {
                callbackRejected = true;
            }
        }

        if (untrusted)
        {
            return nameMismatch ? Verdict.UntrustedChainAndNameMismatch : Verdict.UntrustedChain;
        }

        if (chainFailed)
        {
            return nameMismatch ? Verdict.RejectedAndNameMismatch : Verdict.Rejected;
        }

        if (nameMismatch)
        {
            return Verdict.NameMismatch;
        }

        return callbackRejected ? Verdict.Rejected : Verdict.None;
    }

    /// <summary>
    /// The message of every exception in the chain, outermost first: each <see cref="Exception.InnerException"/>, and each child
    /// of an <see cref="AggregateException"/>. An <see cref="AuthenticationException"/> that carries none of the certificate
    /// wordings (a TLS version or cipher mismatch, for example) matches nothing, so its failure keeps its own text.
    /// </summary>
    private static IEnumerable<string> MessagesIn(Exception? exception)
    {
        var pending = new Stack<Exception>();
        if (exception is not null)
        {
            pending.Push(exception);
        }

        while (pending.Count > 0)
        {
            var current = pending.Pop();
            yield return current.Message ?? string.Empty;

            if (current is AggregateException aggregate)
            {
                foreach (var child in aggregate.InnerExceptions)
                {
                    pending.Push(child);
                }
            }
            else if (current.InnerException is not null)
            {
                pending.Push(current.InnerException);
            }
        }
    }
}
