namespace PerformanceMonitor.Exporter;

/// <summary>The four places a store password can come from (config `store.password.source`, or `store.connection_string_env`).</summary>
public static class CredentialSources
{
    public const string Env = "env";
    public const string File = "file";
    public const string DpapiFile = "dpapi_file";
    public const string ConnectionStringEnv = "connection_string_env";

    /// <summary>The sources valid under `store.password.source`. `connection_string_env` is its own `store` key.</summary>
    public static readonly string[] PasswordSources = [Env, File, DpapiFile];
}

/// <summary>
/// Where the store credential is read from. <paramref name="Name"/> is an env var name (`env`, `connection_string_env`);
/// <paramref name="Path"/> is a file path (`file`, `dpapi_file`). Never holds the secret itself.
/// </summary>
public sealed record CredentialSpec(string Source, string? Name = null, string? Path = null);

/// <summary>A missing or invalid credential. The CLI maps it to exit 2; it never falls back to another login.</summary>
public sealed class CredentialException(string message) : Exception(message);
