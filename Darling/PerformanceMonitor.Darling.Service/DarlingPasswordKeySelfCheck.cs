/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The hidden <c>--self-check-password-key &lt;vector-path&gt;</c> verb (#5366): runs the real password key code in a
/// fresh temporary directory and exits non-zero on any failure. The Linux build job runs it inside the built image,
/// because the test project runs on Windows and so never reaches the Unix owner, link-count and file-identity calls.
///
/// <para>Three groups of checks. The key file: generate it, read the mode, owner and link count back, reload it to the
/// same key id, and see a wider mode and a second name each refused. The file identity: a file and its hard link give
/// one identity and two files give two. The vector: a sealed value made on another platform opens here to the expected
/// text, and stops opening when a bound field changes. A check that cannot be made counts as a failure, never as a
/// pass.</para>
///
/// <para>The vector is a JSON file kept in the repository and labelled as a test vector. Its key belongs to no
/// installation and protects nothing.</para>
/// </summary>
internal static class DarlingPasswordKeySelfCheck
{
    private const string Verb = "--self-check-password-key";

    /// <summary>What the verb prints and returns for a call with the wrong arguments or on a platform it cannot check.</summary>
    internal const string UsageLine = Verb + " <vector-path>";

    /// <summary>
    /// Runs every check and prints one line each. Returns 0 only when all of them passed.
    /// </summary>
    public static int Run(string[] rest, TextWriter output, TextWriter error)
    {
        ArgumentNullException.ThrowIfNull(rest);
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(error);

        if (rest.Length != 1 || string.IsNullOrWhiteSpace(rest[0]))
        {
            error.WriteLine("Usage: " + UsageLine);
            return 1;
        }

        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS())
        {
            error.WriteLine(Verb + " checks the Unix file calls, so it runs on Linux or macOS only.");
            return 1;
        }

        var failures = 0;
        void Report(string name, string? failure)
        {
            if (failure is null)
            {
                output.WriteLine("ok: " + name);
            }
            else
            {
                failures++;
                error.WriteLine("FAILED: " + name + ": " + failure);
            }
        }

        string? work = null;
        try
        {
            work = Directory.CreateTempSubdirectory("password-key-self-check-").FullName;
            Report("key file", Guarded(() => OperatingSystem.IsWindows() ? "not run on Windows" : CheckKeyFile(Path.Combine(work, "keys"))));
            Report("file identity", Guarded(() => CheckFileIdentity(Path.Combine(work, "identity"))));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Report("working directory", ex.Message);
        }
        finally
        {
            TryDelete(work);
        }

        Report("vector", Guarded(() => CheckVector(rest[0])));

        if (failures > 0)
        {
            error.WriteLine($"The password key self-check failed ({failures} of 3 checks).");
            return 1;
        }

        output.WriteLine("The password key self-check passed.");
        return 0;
    }

    /// <summary>Runs a check and turns anything it throws into a failure sentence.</summary>
    private static string? Guarded(Func<string?> check)
    {
        try
        {
            return check();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return ex.GetType().Name + ": " + ex.Message;
        }
    }

    private static void TryDelete(string? directory)
    {
        if (directory is null)
        {
            return;
        }

        try
        {
            Directory.Delete(directory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A temporary directory that stays behind does not change what the checks found. */
        }
    }

    private static byte[] NewKeyBytes()
    {
        using var key = PasswordPrivateKey.Generate();
        return key.ExportPkcs8();
    }

    private static string KeyIdOf(byte[] pkcs8)
    {
        using var key = PasswordPrivateKey.FromPkcs8(pkcs8);
        return key.PublicKey.KeyId;
    }

    /// <summary>The key file on this Unix system: written owner-only, one name, reloads to the same key, and a wider
    /// mode or a second name is refused. Returns null when all of it held, otherwise the first thing that did not.</summary>
    [UnsupportedOSPlatform("windows")]
    internal static string? CheckKeyFile(string directory)
    {
        var logger = NullLogger.Instance;
        Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);

        var generated = DarlingPasswordKeyFile.Generate(directory, NewKeyBytes, logger);
        if (generated.Pkcs8 is null || generated.Untrusted || generated.Refusal is not null)
        {
            return "the key file was not written: " + (generated.Refusal ?? "no key came back");
        }

        string keyId;
        try
        {
            keyId = KeyIdOf(generated.Pkcs8);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(generated.Pkcs8);
        }

        var expectedMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        var mode = File.GetUnixFileMode(path);
        if (mode != expectedMode)
        {
            return $"the key file mode is {Convert.ToString((int)mode, 8)}, not 600";
        }

        var owner = FileIdentity.UnixOwnerOf(path);
        if (owner is null)
        {
            return "the owner of the key file could not be read";
        }

        if (owner.Value.UserId != FileIdentity.EffectiveUserId())
        {
            return "the key file is not owned by the user the process runs as";
        }

        if (owner.Value.Links != 1)
        {
            return $"the key file has {owner.Value.Links} names, not one";
        }

        var reloadFailure = LoadMustSucceed(directory, keyId, logger, "after it was written");
        if (reloadFailure is not null)
        {
            return reloadFailure;
        }

        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        var wide = LoadMustBeRefused(directory, logger, "with mode 644");
        File.SetUnixFileMode(path, expectedMode);
        if (wide is not null)
        {
            return wide;
        }

        reloadFailure = LoadMustSucceed(directory, keyId, logger, "after the mode went back to 600");
        if (reloadFailure is not null)
        {
            return reloadFailure;
        }

        var extraName = Path.Combine(directory, "second-name");
        if (!TryHardLink(path, extraName))
        {
            return "a second name for the key file could not be made, so the link check could not run";
        }

        try
        {
            var linked = LoadMustBeRefused(directory, logger, "with a second name");
            if (linked is not null)
            {
                return linked;
            }

            if (FileIdentity.UnixOwnerOf(path)?.Links != 2)
            {
                return "the link count did not report the second name";
            }
        }
        finally
        {
            File.Delete(extraName);
        }

        return LoadMustSucceed(directory, keyId, logger, "after the second name was removed");
    }

    private static string? LoadMustSucceed(string directory, string keyId, Microsoft.Extensions.Logging.ILogger logger, string when)
    {
        var load = DarlingPasswordKeyFile.Load(directory, logger);
        if (load.Pkcs8 is null)
        {
            return $"the key file was not read {when}: " + (load.Refusal ?? "no key came back");
        }

        try
        {
            return load.Untrusted || KeyIdOf(load.Pkcs8) != keyId
                ? $"the key file did not reload to the same key id {when}"
                : null;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
        }
    }

    private static string? LoadMustBeRefused(string directory, Microsoft.Extensions.Logging.ILogger logger, string when)
    {
        var load = DarlingPasswordKeyFile.Load(directory, logger);
        if (load.Pkcs8 is not null)
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
            return $"the key file was read {when}";
        }

        return load.Untrusted && load.Refusal is not null
            ? null
            : $"the key file was not refused as untrusted {when}";
    }

    /// <summary>The file identity: a file and its hard link give one identity, two files give two, and a missing path
    /// gives none. Returns null when all of it held.</summary>
    internal static string? CheckFileIdentity(string directory)
    {
        Directory.CreateDirectory(directory);
        var first = Path.Combine(directory, "first");
        var second = Path.Combine(directory, "second");
        var firstName = Path.Combine(directory, "first-other-name");
        File.WriteAllText(first, "one");
        File.WriteAllText(second, "two");

        if (!TryHardLink(first, firstName))
        {
            return "a second name for a file could not be made, so the identity check could not run";
        }

        var a = FileIdentity.Of(first);
        var b = FileIdentity.Of(firstName);
        var c = FileIdentity.Of(second);
        if (a is null || b is null || c is null)
        {
            return "the identity of an existing file could not be read";
        }

        if (a.Value != b.Value)
        {
            return "a file and its second name gave different identities";
        }

        if (a.Value == c.Value)
        {
            return "two different files gave the same identity";
        }

        return FileIdentity.Of(Path.Combine(directory, "missing")) is null
            ? null
            : "a path that does not exist gave an identity";
    }

    /// <summary>The sealed value in the vector file opens to its plaintext with the vector's own private key, a value
    /// sealed here opens too, and a changed bound field stops the vector's value opening. Platform-neutral. Returns null
    /// when all of it held.</summary>
    internal static string? CheckVector(string vectorPath)
    {
        if (!File.Exists(vectorPath))
        {
            return "the vector file was not found";
        }

        using var document = JsonDocument.Parse(File.ReadAllText(vectorPath));
        var root = document.RootElement;

        var label = Text(root, "label");
        if (label is null || !label.Contains("TEST VECTOR", StringComparison.Ordinal))
        {
            return "the vector file is not labelled as a test vector";
        }

        if (Text(root, "purpose") != PasswordBinding.ServerPurpose)
        {
            return "the vector file does not carry a server password";
        }

        var plaintext = Text(root, "plaintext");
        var sealedText = Text(root, "sealed");
        var keyText = Text(root, "testOnlyPrivateKeyPkcs8Base64");
        if (plaintext is null || sealedText is null || keyText is null || !root.TryGetProperty("binding", out var b))
        {
            return "the vector file is missing a field";
        }

        var identity = IdentityFrom(b, usernameSuffix: "");
        var changed = IdentityFrom(b, usernameSuffix: "-changed");
        if (identity is null || changed is null)
        {
            return "the vector file's binding is missing a field";
        }

        var pkcs8 = Convert.FromBase64String(keyText);
        try
        {
            using var key = PasswordPrivateKey.FromPkcs8(pkcs8);
            if (PasswordSeal.KeyIdOf(sealedText) != key.PublicKey.KeyId)
            {
                return "the vector's sealed value names a different key than the vector's private key";
            }

            string opened;
            try
            {
                opened = PasswordSeal.Open(sealedText, key, PasswordBinding.ForServer(identity.Value));
            }
            catch (PasswordSealException ex)
            {
                return $"the vector's sealed value did not open ({ex.Kind})";
            }

            if (!string.Equals(opened, plaintext, StringComparison.Ordinal))
            {
                return "the vector's sealed value opened to different text";
            }

            try
            {
                PasswordSeal.Open(sealedText, key, PasswordBinding.ForServer(changed.Value));
                return "the vector's sealed value opened for a changed username";
            }
            catch (PasswordSealException)
            {
                /* Expected: the value is bound to the fields it was sealed for. */
            }

            var again = PasswordSeal.Seal(plaintext, key.PublicKey, PasswordBinding.ForServer(identity.Value));
            return string.Equals(PasswordSeal.Open(again, key, PasswordBinding.ForServer(identity.Value)), plaintext, StringComparison.Ordinal)
                ? null
                : "a value sealed on this system did not open to the same text";
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
        }
    }

    private static string? Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

    private static ServerConnectionIdentity? IdentityFrom(JsonElement b, string usernameSuffix)
    {
        var host = Text(b, "host");
        var engine = Text(b, "engine");
        var auth = Text(b, "auth");
        var username = Text(b, "username");
        var encrypt = Text(b, "encryptMode");
        if (host is null || engine is null || auth is null || username is null || encrypt is null
            || !b.TryGetProperty("port", out var port) || port.ValueKind != JsonValueKind.Number
            || !Flag(b, "readOnlyIntent", out var readOnly) || !Flag(b, "trustServerCertificate", out var trust)
            || !Flag(b, "multiSubnetFailover", out var multiSubnet))
        {
            return null;
        }

        return new ServerConnectionIdentity(
            host, port.GetInt32(), engine, Text(b, "database"), readOnly, auth, username + usernameSuffix, encrypt, trust, multiSubnet);
    }

    private static bool Flag(JsonElement element, string name, out bool value)
    {
        value = false;
        if (!element.TryGetProperty(name, out var found) || found.ValueKind is not (JsonValueKind.True or JsonValueKind.False))
        {
            return false;
        }

        value = found.GetBoolean();
        return true;
    }

    /// <summary>Makes a second name for a file. Unix only; false when the call is not there or fails.</summary>
    private static bool TryHardLink(string existing, string newName)
    {
        if (OperatingSystem.IsWindows())
        {
            return false;
        }

        try
        {
            return Link(existing, newName) == 0;
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException)
        {
            return false;
        }
    }

#pragma warning disable CA2101 // libc takes UTF-8; LPUTF8Str is correct and CharSet.Unicode would be wrong
    [DllImport("libc", EntryPoint = "link", SetLastError = true)]
    private static extern int Link([MarshalAs(UnmanagedType.LPUTF8Str)] string existing, [MarshalAs(UnmanagedType.LPUTF8Str)] string newName);
#pragma warning restore CA2101
}
