using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Resolves the PostgreSQL password for the store connection. Never logs, prints or returns
/// the password embedded in any other string (callers must build the connection string
/// themselves and never pass it to a logger).
/// </summary>
public static class CredentialResolver
{
    /// <summary>
    /// Darling's own DPAPI entropy string for this credential family. Must match what
    /// Darling used to encrypt the blob, or Unprotect fails.
    /// </summary>
    private const string DpapiEntropy = "PerformanceMonitor.Darling.v1";

    public const string PasswordEnvVar = "DARLING_EXPORT_PG_PASSWORD";

    /// <summary>
    /// Resolves the password: the env var wins if set (non-empty), otherwise the DPAPI
    /// credential file is decrypted. Throws if neither source is usable.
    /// </summary>
    public static string Resolve(string? credentialFilePath) =>
        Resolve(credentialFilePath ?? ExportOptions.DefaultCredentialFile, PasswordEnvVar);

    /// <summary>
    /// Resolves a credential named by a config file (`store.password` or `store.connection_string_env`). One source, no
    /// fallback: a missing env var, a missing or empty file, or an unusable DPAPI file throws a
    /// <see cref="CredentialException"/> (exit 2). It never tries another source or another login.
    /// </summary>
    public static string Resolve(CredentialSpec spec, TextWriter? warnings = null)
    {
        switch (spec.Source)
        {
            case CredentialSources.Env:
            case CredentialSources.ConnectionStringEnv:
            {
                var key = spec.Source == CredentialSources.Env ? "store.password" : "store.connection_string_env";
                var value = Environment.GetEnvironmentVariable(spec.Name ?? "");
                return string.IsNullOrEmpty(value)
                    ? throw new CredentialException($"{key}: env var '{spec.Name}' is not set or is empty")
                    : value;
            }

            case CredentialSources.File:
                return ReadPlainFile(spec.Path ?? "", warnings ?? Console.Error);

            case CredentialSources.DpapiFile:
                if (!OperatingSystem.IsWindows())
                {
                    throw new CredentialException("store.password.source: dpapi_file can only be used on Windows");
                }

                if (!File.Exists(spec.Path))
                {
                    throw new CredentialException($"store.password.path: DPAPI credential file '{spec.Path}' was not found");
                }

                try
                {
                    return DecryptDpapiFile(spec.Path!);
                }
                catch (Exception ex) when (ex is CryptographicException or FormatException or IOException)
                {
                    throw new CredentialException($"store.password.path: DPAPI credential file '{spec.Path}' could not be decrypted ({ex.GetType().Name})");
                }

            default:
                throw new CredentialException($"store.password.source: unknown source '{spec.Source}'");
        }
    }

    private static string ReadPlainFile(string path, TextWriter warnings)
    {
        if (!File.Exists(path))
        {
            throw new CredentialException($"store.password.path: credential file '{path}' was not found");
        }

        if (!OperatingSystem.IsWindows())
        {
            var mode = File.GetUnixFileMode(path);
            const UnixFileMode groupOrWorld = UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute
                | UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;
            if ((mode & groupOrWorld) != 0)
            {
                warnings.WriteLine($"Warning: credential file '{path}' is readable by group or others; run chmod 600 on it.");
            }
        }

        var password = File.ReadAllText(path).Trim();
        return password.Length == 0
            ? throw new CredentialException($"store.password.path: credential file '{path}' is empty")
            : password;
    }

    private static string Resolve(string path, string envVar)
    {
        var fromEnv = Environment.GetEnvironmentVariable(envVar);
        if (!string.IsNullOrEmpty(fromEnv))
        {
            return fromEnv;
        }

        if (!OperatingSystem.IsWindows())
        {
            throw new CredentialException(
                $"No {envVar} env var is set, and DPAPI credential files can only be decrypted on Windows. " +
                $"Set {envVar} or run this on the Windows monitoring box.");
        }

        if (!File.Exists(path))
        {
            throw new CredentialException(
                $"No {envVar} env var is set, and the DPAPI credential file was not found at '{path}'.");
        }

        return DecryptDpapiFile(path);
    }

    [SupportedOSPlatform("windows")]
    private static string DecryptDpapiFile(string path)
    {
        var base64Text = File.ReadAllText(path).Trim();
        var encryptedBytes = Convert.FromBase64String(base64Text);
        var entropy = Encoding.UTF8.GetBytes(DpapiEntropy);
        var decryptedBytes = ProtectedData.Unprotect(encryptedBytes, entropy, DataProtectionScope.LocalMachine);
        return Encoding.UTF8.GetString(decryptedBytes);
    }
}
