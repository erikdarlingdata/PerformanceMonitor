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
/// fresh temporary directory and exits non-zero on any failure. The Linux store workflow runs it inside the built image,
/// because the test project runs on Windows and so never reaches the Unix owner, link-count and file-identity calls.
///
/// <para>Four groups of checks. The key file: generate it, read the mode, owner and link count back, reload it to the
/// same key id, and see a wider mode and a second name each refused. The external key (#8006): the <c>passwordKey</c>
/// loader reads one key to the same key id from a base64 file, a PEM file, a Kubernetes-style <c>..data</c> link chain
/// and an environment variable, warns for a 0644 file and one another user (not root) owns, and refuses a 0664 file,
/// <c>/dev/zero</c>, a FIFO, a pipe behind a <c>/proc/self/fd</c> link, a PKCS#1 PEM, a 2048-bit key, a link into a credentials directory and a literal. The file
/// identity: a file and its hard link give
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
            Report("external key", Guarded(() => OperatingSystem.IsWindows() ? "not run on Windows" : CheckExternalKey(Path.Combine(work, "external"))));
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
            error.WriteLine($"The password key self-check failed ({failures} of 4 checks).");
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

    /// <summary>The variable the env: case sets for the length of the check and clears after. It names no setting the
    /// service reads, so it can never stand in for a real key.</summary>
    private const string ExternalKeyVariable = "DARLING_SELF_CHECK_PASSWORD_KEY";

    /// <summary>The <c>passwordKey</c> source (#8006) on this Unix system: one generated key loads to the same key id from
    /// a base64 file, a PEM file, a Kubernetes-style secret mount (a link to <c>..data/password-key</c>, where
    /// <c>..data</c> links to a timestamped directory holding the file at mode 440) and an environment variable with
    /// whitespace around the base64; the file checks hold (<see cref="CheckFileChecks"/>: a 0644 file warns, a 0664 file,
    /// <c>/dev/zero</c>, a FIFO and a pipe behind a <c>/proc/self/fd</c> link are refused, a file another user (not root)
    /// owns warns and a root-owned one does not); a PKCS#1 PEM, a 2048-bit key, a symbolic
    /// link from outside a credentials directory to the key file inside it, and a literal are each refused. Returns null
    /// when all of it held, otherwise the first thing that did not.</summary>
    [UnsupportedOSPlatform("windows")]
    internal static string? CheckExternalKey(string directory)
    {
        var logger = NullLogger.Instance;
        Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

        byte[]? pkcs8 = null;
        char[]? base64 = null;
        try
        {
            pkcs8 = NewKeyBytes();
            var keyId = KeyIdOf(pkcs8);
            base64 = ToBase64Chars(pkcs8);

            var base64File = Path.Combine(directory, "password-key.b64");
            WriteKeyText(base64File, base64);
            var failure = ExternalMustLoad("file:" + base64File, keyId, logger, "a base64 file");
            if (failure is not null)
            {
                return failure;
            }

            var pemFile = Path.Combine(directory, "password-key.pem");
            var pem = PemEncoding.Write("PRIVATE KEY", pkcs8);
            try
            {
                WriteKeyText(pemFile, pem);
            }
            finally
            {
                Array.Clear(pem);
            }

            failure = ExternalMustLoad("file:" + pemFile, keyId, logger, "a PEM file");
            if (failure is not null)
            {
                return failure;
            }

            /* The layout the kubelet gives a secret volume: the name the pod sees is a link through ..data, which is itself
               a link the kubelet swaps atomically to a new timestamped directory on every update. */
            var mount = Path.Combine(directory, "k8s");
            var version = "..2026_01_01_00_00_00.000000001";
            Directory.CreateDirectory(Path.Combine(mount, version));
            var realFile = Path.Combine(mount, version, "password-key");
            WriteKeyText(realFile, base64);
            File.SetUnixFileMode(realFile, UnixFileMode.UserRead | UnixFileMode.GroupRead);
            Directory.CreateSymbolicLink(Path.Combine(mount, "..data"), version);
            File.CreateSymbolicLink(Path.Combine(mount, "password-key"), Path.Combine("..data", "password-key"));
            failure = ExternalMustLoad("file:" + Path.Combine(mount, "password-key"), keyId, logger, "a secret mount link");
            if (failure is not null)
            {
                return failure;
            }

            failure = CheckFileChecks(base64, keyId, directory, logger);
            if (failure is not null)
            {
                return failure;
            }

            try
            {
                Environment.SetEnvironmentVariable(ExternalKeyVariable, " " + new string(base64) + "\n");
                failure = ExternalMustLoad("env:" + ExternalKeyVariable, keyId, logger, "an environment variable");
            }
            finally
            {
                Environment.SetEnvironmentVariable(ExternalKeyVariable, null);
            }

            if (failure is not null)
            {
                return failure;
            }

            failure = CheckPkcs1Refused(pkcs8, Path.Combine(directory, "pkcs1.pem"), logger);
            if (failure is not null)
            {
                return failure;
            }

            failure = CheckSmallKeyRefused(Path.Combine(directory, "rsa-2048.b64"), logger);
            if (failure is not null)
            {
                return failure;
            }

            failure = CheckLinkIntoCredentialsRefused(base64, directory, logger);
            if (failure is not null)
            {
                return failure;
            }

            var literal = DarlingPasswordKeySource.LoadFromSetting(new string(base64), credentialsDirectory: null, logger);
            return ExternalMustBeRefused(literal, "a literal");
        }
        finally
        {
            if (pkcs8 is not null)
            {
                CryptographicOperations.ZeroMemory(pkcs8);
            }

            if (base64 is not null)
            {
                Array.Clear(base64);
            }
        }
    }

    /// <summary>The <c>file:</c> checks (#8006 review, request 1) through the real Unix calls: a 0644 file loads with the
    /// warning that others can read it; a 0664 file is refused; <c>/dev/zero</c>, a FIFO and a pipe behind a
    /// <c>/proc/self/fd</c> link are refused as not regular files, at once, without being opened; and, when this runs as
    /// root (as the workflow's first run does, with <c>--user 0:0</c>, since the image runs as user 1654), a file given
    /// to another user (not root) loads with the owner warning, and the same file given to root loads without it. Run as
    /// another user, which cannot give a file away, those cases are skipped.
    /// Returns null when all of it held.</summary>
    [UnsupportedOSPlatform("windows")]
    private static string? CheckFileChecks(char[] base64, string keyId, string directory, Microsoft.Extensions.Logging.ILogger logger)
    {
        const UnixFileMode ownerReadWrite = UnixFileMode.UserRead | UnixFileMode.UserWrite;

        var open = Path.Combine(directory, "password-key-0644");
        WriteKeyText(open, base64);
        File.SetUnixFileMode(open, ownerReadWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
        var failure = ExternalMustLoadWithWarning(open, keyId, logger, "a 0644 file", DarlingPasswordKeyFileReader.OthersReadTemplate);
        if (failure is not null)
        {
            return failure;
        }

        var groupWritable = Path.Combine(directory, "password-key-0664");
        WriteKeyText(groupWritable, base64);
        File.SetUnixFileMode(groupWritable, ownerReadWrite | UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.OtherRead);
        failure = ExternalMustBeRefusedFor("file:" + groupWritable, logger, "a 0664 file", "(mode 664)");
        if (failure is not null)
        {
            return failure;
        }

        /* Read with File.ReadAllText, /dev/zero never ends and a FIFO waits for a writer: either held the start. */
        var clock = System.Diagnostics.Stopwatch.StartNew();
        failure = ExternalMustBeRefusedFor("file:/dev/zero", logger, "/dev/zero", "is not a regular file");
        if (failure is not null)
        {
            return failure;
        }

        if (clock.Elapsed > TimeSpan.FromSeconds(5))
        {
            return $"passwordKey took {clock.Elapsed.TotalSeconds:F1} s to refuse /dev/zero";
        }

        var fifo = Path.Combine(directory, "password-key-fifo");
        if (MakeFifo(fifo, Convert.ToUInt32("600", 8)) != 0)
        {
            return "a FIFO could not be made, so the FIFO check could not run";
        }

        failure = ExternalMustBeRefusedFor("file:" + fifo, logger, "a FIFO", "is not a regular file");
        if (failure is not null)
        {
            return failure;
        }

        failure = CheckPipeBehindAnFdLinkRefused(logger);
        if (failure is not null)
        {
            return failure;
        }

        if (FileIdentity.EffectiveUserId() != 0)
        {
            /* Only root can give a file to another user; the workflow runs the image once as root (--user 0:0; its
               default user is 1654, #8141), so there it always runs. */
            return null;
        }

        var foreign = Path.Combine(directory, "password-key-foreign");
        WriteKeyText(foreign, base64);
        const uint nobody = 65534;
        if (ChangeOwner(foreign, nobody, nobody) != 0)
        {
            return "the key file could not be given to another user, so the owner check could not run";
        }

        failure = ExternalMustLoadWithWarning(foreign, keyId, logger, "a file another user owns", DarlingPasswordKeyFileReader.OwnerTemplate);
        if (failure is not null)
        {
            return failure;
        }

        /* The same file given to root gives no owner warning: root can replace any file anyway. Running as root, this is
           also the service's own user; a non-root service reading a root-owned secret is pinned by the Judge cases in
           Darling.Tests. */
        if (ChangeOwner(foreign, 0, 0) != 0)
        {
            return "the key file could not be given back to root, so the root-owner case could not run";
        }

        var rootOwned = new List<PasswordKeyFileWarning>();
        var load = DarlingPasswordKeySource.Load("file:" + foreign, credentialsDirectory: null, logger, rootOwned);
        if (load.Pkcs8 is null)
        {
            return "passwordKey did not read the key from a root-owned file: " + (load.Refusal ?? "no key came back");
        }

        CryptographicOperations.ZeroMemory(load.Pkcs8);
        return rootOwned.Contains(new PasswordKeyFileWarning(DarlingPasswordKeyFileReader.OwnerTemplate, foreign))
            ? "passwordKey warned about the owner of a root-owned file"
            : null;
    }

    /// <summary>A pipe reached through a <c>/proc/self/fd</c> link, as <c>file:/dev/stdin</c> reaches one under
    /// <c>docker run -i</c>: the link's text is <c>pipe:[N]</c>, so only the kernel's own resolution shows a pipe. Its
    /// write end stays open, so a read would wait for ever; the refusal must come before the open, within five seconds.
    /// If it does not, the write end is closed, which ends the read, and the check fails.</summary>
    [UnsupportedOSPlatform("windows")]
    private static string? CheckPipeBehindAnFdLinkRefused(Microsoft.Extensions.Logging.ILogger logger)
    {
        using var writer = new System.IO.Pipes.AnonymousPipeServerStream(System.IO.Pipes.PipeDirection.Out, HandleInheritability.None);
        var reference = "file:/proc/self/fd/" + writer.GetClientHandleAsString();
        var refusal = System.Threading.Tasks.Task.Run(() => ExternalMustBeRefusedFor(reference, logger, "a pipe behind a /proc/self/fd link", "is not a regular file"));
        if (!refusal.Wait(TimeSpan.FromSeconds(5)))
        {
            writer.Dispose();
            refusal.Wait(TimeSpan.FromSeconds(5));
            return "passwordKey opened a pipe behind a /proc/self/fd link and waited for its writer, instead of refusing it";
        }

        return refusal.Result;
    }

    private static string? ExternalMustLoadWithWarning(string path, string keyId, Microsoft.Extensions.Logging.ILogger logger, string from, string template)
    {
        var warnings = new List<PasswordKeyFileWarning>();
        var load = DarlingPasswordKeySource.Load("file:" + path, credentialsDirectory: null, logger, warnings);
        if (load.Pkcs8 is null)
        {
            return $"passwordKey did not read the key from {from}: " + (load.Refusal ?? "no key came back");
        }

        try
        {
            if (load.Untrusted || load.Refusal is not null || KeyIdOf(load.Pkcs8) != keyId)
            {
                return $"passwordKey did not load the same key id from {from}";
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
        }

        return warnings.Contains(new PasswordKeyFileWarning(template, path))
            ? null
            : $"passwordKey loaded {from} without the warning expected for it";
    }

    private static string? ExternalMustBeRefusedFor(string reference, Microsoft.Extensions.Logging.ILogger logger, string what, string because)
    {
        var load = DarlingPasswordKeySource.LoadFromSetting(reference, credentialsDirectory: null, logger);
        var refusal = load.Refusal;
        var failure = ExternalMustBeRefused(load, what);
        if (failure is not null)
        {
            return failure;
        }

        return refusal!.Contains(because, StringComparison.Ordinal)
            ? null
            : $"passwordKey refused {what}, but not because it {because}";
    }

    [UnsupportedOSPlatform("windows")]
    private static string? CheckPkcs1Refused(byte[] pkcs8, string path, Microsoft.Extensions.Logging.ILogger logger)
    {
        using (var rsa = RSA.Create())
        {
            rsa.ImportPkcs8PrivateKey(pkcs8, out _);
            var pkcs1 = rsa.ExportRSAPrivateKey();
            char[]? pem = null;
            try
            {
                pem = PemEncoding.Write("RSA PRIVATE KEY", pkcs1);
                WriteKeyText(path, pem);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(pkcs1);
                if (pem is not null)
                {
                    Array.Clear(pem);
                }
            }
        }

        return ExternalMustBeRefused(DarlingPasswordKeySource.LoadFromSetting("file:" + path, credentialsDirectory: null, logger), "a PKCS#1 PEM");
    }

    [UnsupportedOSPlatform("windows")]
    private static string? CheckSmallKeyRefused(string path, Microsoft.Extensions.Logging.ILogger logger)
    {
        using (var rsa = RSA.Create(2048))
        {
            var der = rsa.ExportPkcs8PrivateKey();
            char[]? text = null;
            try
            {
                text = ToBase64Chars(der);
                WriteKeyText(path, text);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(der);
                if (text is not null)
                {
                    Array.Clear(text);
                }
            }
        }

        return ExternalMustBeRefused(DarlingPasswordKeySource.LoadFromSetting("file:" + path, credentialsDirectory: null, logger), "a 2048-bit key");
    }

    /// <summary>A symbolic link outside a credentials directory that points at the <c>password-key</c> file inside it. The
    /// Windows tests cover the link refusal with junctions and hard links only, and every other case here passes no
    /// credentials directory, so this is the one run of the real-path and file-identity comparison on Unix. The key in the
    /// file is usable, so a refusal can only be the directory's; the sentence must also say so. Returns null when the link
    /// was refused for reaching the credentials directory.</summary>
    [UnsupportedOSPlatform("windows")]
    private static string? CheckLinkIntoCredentialsRefused(char[] base64, string directory, Microsoft.Extensions.Logging.ILogger logger)
    {
        const string what = "a link into the credentials directory";
        var credentials = Path.Combine(directory, "credentials");
        Directory.CreateDirectory(credentials, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        var keyPath = Path.Combine(credentials, DarlingPasswordKeyFile.UnixFileName);
        WriteKeyText(keyPath, base64);
        var link = Path.Combine(directory, "credentials-key-link");
        File.CreateSymbolicLink(link, keyPath);

        var load = DarlingPasswordKeySource.LoadFromSetting("file:" + link, credentials, logger);
        var refusal = load.Refusal;
        var failure = ExternalMustBeRefused(load, what);
        if (failure is not null)
        {
            return failure;
        }

        return refusal!.Contains("credentials directory", StringComparison.Ordinal)
            ? null
            : $"passwordKey refused {what}, but not for reaching the credentials directory";
    }

    private static string? ExternalMustLoad(string reference, string keyId, Microsoft.Extensions.Logging.ILogger logger, string from)
    {
        var load = DarlingPasswordKeySource.LoadFromSetting(reference, credentialsDirectory: null, logger);
        if (load.Pkcs8 is null)
        {
            return $"passwordKey did not read the key from {from}: " + (load.Refusal ?? "no key came back");
        }

        try
        {
            return load.Untrusted || load.Refusal is not null || KeyIdOf(load.Pkcs8) != keyId
                ? $"passwordKey did not load the same key id from {from}"
                : null;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
        }
    }

    private static string? ExternalMustBeRefused(PasswordKeyFileLoad load, string what)
    {
        if (load.Pkcs8 is not null)
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
            return $"passwordKey read {what}";
        }

        return load.Untrusted && load.Refusal is not null
            ? null
            : $"passwordKey did not refuse {what} as untrusted";
    }

    private static char[] ToBase64Chars(byte[] bytes)
    {
        var chars = new char[(bytes.Length + 2) / 3 * 4];
        Convert.ToBase64CharArray(bytes, 0, bytes.Length, chars, 0);
        return chars;
    }

    /// <summary>Writes key text owner-only, through a byte buffer that is zeroed after, so no copy of the key is left in a
    /// string.</summary>
    [UnsupportedOSPlatform("windows")]
    private static void WriteKeyText(string path, char[] text)
    {
        var bytes = System.Text.Encoding.ASCII.GetBytes(text);
        try
        {
            using var stream = new FileStream(path, new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
            });
            stream.Write(bytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
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
            host, port.GetInt32(), engine, Text(b, "database"), readOnly, auth, username + usernameSuffix, encrypt, trust, multiSubnet,
            Text(b, "queryStoreReplica"));
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

    [DllImport("libc", EntryPoint = "mkfifo", SetLastError = true)]
    private static extern int MakeFifo([MarshalAs(UnmanagedType.LPUTF8Str)] string path, uint mode);

    [DllImport("libc", EntryPoint = "chown", SetLastError = true)]
    private static extern int ChangeOwner([MarshalAs(UnmanagedType.LPUTF8Str)] string path, uint owner, uint group);
#pragma warning restore CA2101
}
