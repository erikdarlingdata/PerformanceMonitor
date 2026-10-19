/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>Why a remediation login could not be resolved (#5632, step 6). Each has its own code and its own sentence, and no sentence repeats the
/// text of a slot or a secret.</summary>
public enum RemediationLoginError
{
    None = 0,

    /// <summary>The server has no remediation login name.</summary>
    LoginMissing,

    /// <summary>The server has a login name but nothing in the secret slot.</summary>
    SecretSlotMissing,

    /// <summary>The secret slot holds something other than an <c>env:</c> or <c>file:</c> reference: a stored (sealed or plain) password. Refused, never used.</summary>
    StoredPasswordRefused,

    /// <summary>The reference names no variable or path, or its target is not there or cannot be read.</summary>
    SecretUnreadable,

    /// <summary>The reference resolved, to nothing or to blanks.</summary>
    SecretEmpty,
}

/// <summary>A resolved remediation login. The secret is held in one field that no formatter, <c>ToString</c> or serializer reaches.</summary>
public sealed class RemediationLogin
{
    private readonly string _secret;

    internal RemediationLogin(string userName, string secret)
    {
        UserName = userName;
        _secret = secret;
    }

    public string UserName { get; }

    /// <summary>The secret, for the connection string and for scrubbing a message. Internal on purpose: nothing outside this assembly reads it.</summary>
    internal string Secret => _secret;

    public override string ToString() => $"RemediationLogin({UserName})";
}

/// <summary>The outcome of <see cref="RemediationLoginResolver.Resolve"/>: a login, or the one reason there is none.</summary>
public sealed class RemediationLoginResolution
{
    private RemediationLoginResolution(RemediationLogin? login, RemediationLoginError error, string? message)
    {
        Login = login;
        Error = error;
        Message = message;
    }

    public RemediationLogin? Login { get; }

    public RemediationLoginError Error { get; }

    /// <summary>A sentence an operator can act on. It names the login's presence or absence, never a secret or a reference's target text.</summary>
    public string? Message { get; }

    internal static RemediationLoginResolution Ok(RemediationLogin login) => new(login, RemediationLoginError.None, null);

    internal static RemediationLoginResolution Fail(RemediationLoginError error, string message) => new(null, error, message);
}

/// <summary>
/// Resolves a server's remediation login (#5632, step 6; #5240). The store holds the login NAME and a REFERENCE (<c>env:NAME</c> or
/// <c>file:path</c>, read on the Darling host); the secret itself never sits in the store. This resolves the reference when an action runs.
/// <para>It is the same two prefixes, matched case-sensitively at the start of the slot, and the same trimming of a file's text as the
/// service's <c>DarlingSecretSource</c>. It is restated here, not reused, because that type lives in the service assembly and this assembly
/// never references the service. A test pins that the two agree on what is a reference.</para>
/// </summary>
public static class RemediationLoginResolver
{
    private const string EnvPrefix = "env:";
    private const string FilePrefix = "file:";

    public static RemediationLoginResolution Resolve(string? userName, string? secretSlot)
    {
        if (string.IsNullOrWhiteSpace(userName))
            return RemediationLoginResolution.Fail(RemediationLoginError.LoginMissing,
                "This server has no remediation login name.");
        if (string.IsNullOrWhiteSpace(secretSlot))
            return RemediationLoginResolution.Fail(RemediationLoginError.SecretSlotMissing,
                "This server's remediation login has no secret reference. Give it an env: or file: reference.");

        string? secret;
        if (secretSlot.StartsWith(EnvPrefix, StringComparison.Ordinal))
        {
            var name = secretSlot.Substring(EnvPrefix.Length).Trim();
            if (name.Length == 0)
                return Unreadable("The env: reference names no environment variable.");
            secret = Environment.GetEnvironmentVariable(name);
        }
        else if (secretSlot.StartsWith(FilePrefix, StringComparison.Ordinal))
        {
            var path = secretSlot.Substring(FilePrefix.Length).Trim();
            if (path.Length == 0)
                return Unreadable("The file: reference names no path.");
            try
            {
                secret = File.ReadAllText(path);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
            {
                /* The reason is the exception's type, not its text: the text can carry the path, and a path is as private as the slot. */
                return Unreadable("The file: reference could not be read (" + ex.GetType().Name + ").");
            }
        }
        else
        {
            return RemediationLoginResolution.Fail(RemediationLoginError.StoredPasswordRefused,
                "The remediation secret is a stored password. Server actions use an env: or file: reference only, and never use a stored password.");
        }

        var trimmed = secret?.Trim();
        if (string.IsNullOrEmpty(trimmed))
            return RemediationLoginResolution.Fail(RemediationLoginError.SecretEmpty,
                "The remediation secret resolved to nothing (the variable is not set, or the file is empty or blank).");
        return RemediationLoginResolution.Ok(new RemediationLogin(userName.Trim(), trimmed));

        static RemediationLoginResolution Unreadable(string message) =>
            RemediationLoginResolution.Fail(RemediationLoginError.SecretUnreadable, message);
    }
}
