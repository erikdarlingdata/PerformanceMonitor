using System.Text.Json;

namespace PerformanceMonitor.Exporter.Upload;

/// <summary>How the S3 uploader checksums request payloads.</summary>
public enum S3Checksum
{
    /// <summary>Send a SHA-256 checksum with every upload (what the exporter has always done).</summary>
    Sha256,

    /// <summary>Send no explicit checksum. For S3-compatible stores that reject one.</summary>
    None,
}

/// <summary>Where the S3 uploader gets its credentials.</summary>
public abstract record S3Credentials
{
    private S3Credentials() { }

    /// <summary>The default AWS chain: environment, shared profile, instance role.</summary>
    public sealed record Default : S3Credentials;

    /// <summary>A named profile from the shared credentials file.</summary>
    public sealed record Profile(string Name) : S3Credentials;

    /// <summary>Keys held in environment variables. The settings name the variables; they never hold the values.</summary>
    public sealed record EnvKeys(string AccessKeyIdEnv, string SecretAccessKeyEnv, string? SessionTokenEnv) : S3Credentials;
}

/// <summary>How the Azure uploader authenticates.</summary>
public abstract record AzureAuth
{
    private AzureAuth() { }

    /// <summary>Azure.Identity's default credential chain (environment, managed identity, developer sign-in).</summary>
    public sealed record DefaultCredential : AzureAuth;

    /// <summary>A storage connection string held in the named environment variable.</summary>
    public sealed record ConnectionStringEnv(string Name) : AzureAuth;

    /// <summary>
    /// A SAS held in the named environment variable: a full SAS URL or just the token. Only the token (the part after
    /// '?') is used; the account and container come from the settings.
    /// </summary>
    public sealed record SasUrlEnv(string Name) : AzureAuth;
}

/// <summary>
/// Where an export uploads. Parsed from a config object (<see cref="FromJson"/>) or from the <c>--upload</c> flag
/// (<see cref="FromUri"/>). Parsing validates the shape only; <see cref="UploaderFactory.Create"/> reads environment
/// variables and builds the client.
/// </summary>
public abstract record UploadSettings
{
    private UploadSettings() { }

    /// <summary>The key layout (prefix and store folder style) for this destination.</summary>
    public abstract UploadLayout Layout { get; }

    /// <summary>The destination root for the log line: <c>s3://bucket</c>, <c>azure://account/container</c>, <c>file://path</c>.</summary>
    public abstract string Display { get; }

    /// <summary>Upload to a local or network folder.</summary>
    public sealed record Local(string Path, string Prefix = "", bool HiveStoreFolder = false) : UploadSettings
    {
        public override UploadLayout Layout => new(Prefix, HiveStoreFolder);

        public override string Display => "file://" + Path.Replace('\\', '/');
    }

    /// <summary>Upload to an S3 bucket, or an S3-compatible store when <see cref="EndpointUrl"/> is set.</summary>
    public sealed record S3(
        string Bucket,
        string Prefix = "",
        string? Region = null,
        string? EndpointUrl = null,
        bool ForcePathStyle = false,
        S3Checksum Checksum = S3Checksum.Sha256,
        S3Credentials? Credentials = null,
        bool HiveStoreFolder = false) : UploadSettings
    {
        public override UploadLayout Layout => new(Prefix, HiveStoreFolder);

        public override string Display => "s3://" + Bucket;
    }

    /// <summary>Upload to an Azure Blob container.</summary>
    public sealed record AzureBlob(
        string? AccountUrl,
        string Container,
        string Prefix,
        AzureAuth Auth,
        bool HiveStoreFolder = false) : UploadSettings
    {
        public override UploadLayout Layout => new(Prefix, HiveStoreFolder);

        public override string Display => "azure://" + (AccountUrl is null ? "" : new Uri(AccountUrl).Host.Split('.')[0] + "/") + Container;
    }

    /// <summary>The signing region for a custom endpoint, and the `run` command's S3 region, when none is given.</summary>
    public const string DefaultRegion = "us-east-1";

    /// <summary>
    /// Parses the <c>--upload</c> flag: <c>s3://bucket/prefix</c> (the prefix may be empty and a trailing '/' is
    /// stripped), <c>file:///path</c>, or a bare local or UNC path. <paramref name="region"/> applies to S3 only.
    /// </summary>
    public static UploadSettings FromUri(string uri, string? region)
    {
        if (string.IsNullOrWhiteSpace(uri))
        {
            throw new ArgumentException("--upload is empty");
        }

        const string s3Scheme = "s3://";
        if (uri.StartsWith(s3Scheme, StringComparison.Ordinal))
        {
            var withoutScheme = uri[s3Scheme.Length..];
            var slashIndex = withoutScheme.IndexOf('/');
            var bucket = slashIndex < 0 ? withoutScheme : withoutScheme[..slashIndex];
            var prefix = slashIndex < 0 ? "" : withoutScheme[(slashIndex + 1)..].TrimEnd('/');
            if (string.IsNullOrWhiteSpace(bucket))
            {
                throw new ArgumentException($"--upload has no bucket name: '{uri}'");
            }

            return new S3(bucket, prefix, region);
        }

        if (uri.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            if (!Uri.TryCreate(uri, UriKind.Absolute, out var fileUri) || !fileUri.IsFile)
            {
                throw new ArgumentException($"--upload is not a valid file URI: '{uri}'");
            }

            return new Local(fileUri.LocalPath);
        }

        // Any other "scheme://" is a destination we cannot upload to; a Windows path such as C:\x has no "://".
        if (uri.Contains("://", StringComparison.Ordinal))
        {
            throw new ArgumentException($"--upload must be 's3://bucket/prefix', 'file:///path' or a folder path, got '{uri}'");
        }

        return new Local(uri);
    }

    /// <summary>
    /// Parses a destination object: <c>{"type":"local","path":…,"prefix":…}</c>, <c>{"type":"s3",…}</c> or
    /// <c>{"type":"azure_blob",…}</c>. Unknown fields are an error, so a misspelt option cannot be silently ignored.
    /// </summary>
    public static UploadSettings FromJson(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("destination must be an object");
        }

        var type = RequiredString(element, "type", "destination");
        var hive = OptionalBool(element, "hive_store_folder", false);

        switch (type)
        {
            case "local":
                RejectUnknown(element, "destination", "type", "path", "prefix", "hive_store_folder");
                return new Local(RequiredString(element, "path", "destination"), OptionalPrefix(element), hive);

            case "s3":
                RejectUnknown(element, "destination", "type", "bucket", "prefix", "region", "endpoint_url", "force_path_style", "checksum", "credentials", "hive_store_folder");
                var checksumText = OptionalString(element, "checksum") ?? "sha256";
                var checksum = checksumText switch
                {
                    "sha256" => S3Checksum.Sha256,
                    "none" => S3Checksum.None,
                    _ => throw new ArgumentException($"destination.checksum must be 'sha256' or 'none', got '{checksumText}'"),
                };
                return new S3(
                    RequiredString(element, "bucket", "destination"),
                    OptionalPrefix(element),
                    OptionalString(element, "region"),
                    OptionalString(element, "endpoint_url"),
                    OptionalBool(element, "force_path_style", false),
                    checksum,
                    ParseS3Credentials(element),
                    hive);

            case "azure_blob":
                RejectUnknown(element, "destination", "type", "account_url", "container", "prefix", "auth", "hive_store_folder");
                var auth = ParseAzureAuth(element);
                var accountUrl = OptionalString(element, "account_url");
                if (accountUrl is null && auth is not AzureAuth.ConnectionStringEnv)
                {
                    throw new ArgumentException("destination.account_url is required unless auth.method is 'connection_string_env'");
                }

                if (accountUrl is not null && !Uri.TryCreate(accountUrl, UriKind.Absolute, out _))
                {
                    throw new ArgumentException("destination.account_url is not a valid URL");
                }

                return new AzureBlob(accountUrl?.TrimEnd('/'), RequiredString(element, "container", "destination"), OptionalPrefix(element), auth, hive);

            default:
                throw new ArgumentException($"destination.type must be 'local', 's3' or 'azure_blob', got '{type}'");
        }
    }

    private static S3Credentials ParseS3Credentials(JsonElement destination)
    {
        if (!destination.TryGetProperty("credentials", out var credentials) || credentials.ValueKind == JsonValueKind.Null)
        {
            return new S3Credentials.Default();
        }

        if (credentials.ValueKind == JsonValueKind.String)
        {
            return credentials.GetString() == "default"
                ? new S3Credentials.Default()
                : throw new ArgumentException("destination.credentials must be \"default\" or an object");
        }

        if (credentials.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("destination.credentials must be \"default\" or an object");
        }

        if (credentials.TryGetProperty("profile", out _))
        {
            RejectUnknown(credentials, "destination.credentials", "profile");
            return new S3Credentials.Profile(RequiredString(credentials, "profile", "destination.credentials"));
        }

        RejectUnknown(credentials, "destination.credentials", "access_key_id_env", "secret_access_key_env", "session_token_env");
        return new S3Credentials.EnvKeys(
            RequiredString(credentials, "access_key_id_env", "destination.credentials"),
            RequiredString(credentials, "secret_access_key_env", "destination.credentials"),
            OptionalString(credentials, "session_token_env"));
    }

    private static AzureAuth ParseAzureAuth(JsonElement destination)
    {
        if (!destination.TryGetProperty("auth", out var auth) || auth.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException("destination.auth is required and must be an object");
        }

        var method = RequiredString(auth, "method", "destination.auth");
        switch (method)
        {
            case "default_credential":
                RejectUnknown(auth, "destination.auth", "method");
                return new AzureAuth.DefaultCredential();
            case "connection_string_env":
                RejectUnknown(auth, "destination.auth", "method", "name");
                return new AzureAuth.ConnectionStringEnv(RequiredString(auth, "name", "destination.auth"));
            case "sas_url_env":
                RejectUnknown(auth, "destination.auth", "method", "name");
                return new AzureAuth.SasUrlEnv(RequiredString(auth, "name", "destination.auth"));
            default:
                throw new ArgumentException($"destination.auth.method must be 'default_credential', 'connection_string_env' or 'sas_url_env', got '{method}'");
        }
    }

    private static string OptionalPrefix(JsonElement element) => (OptionalString(element, "prefix") ?? "").Trim('/');

    private static string RequiredString(JsonElement element, string name, string where)
    {
        var value = OptionalString(element, name);
        return string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{where}.{name} is required") : value;
    }

    private static string? OptionalString(JsonElement element, string name)
    {
        if (!element.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return value.ValueKind == JsonValueKind.String ? value.GetString() : throw new ArgumentException($"{name} must be a string");
    }

    private static bool OptionalBool(JsonElement element, string name, bool fallback)
    {
        if (!element.TryGetProperty(name, out var value) || value.ValueKind == JsonValueKind.Null)
        {
            return fallback;
        }

        return value.ValueKind is JsonValueKind.True or JsonValueKind.False ? value.GetBoolean() : throw new ArgumentException($"{name} must be true or false");
    }

    private static void RejectUnknown(JsonElement element, string where, params string[] allowed)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (Array.IndexOf(allowed, property.Name) < 0)
            {
                throw new ArgumentException($"{where} has an unknown field '{property.Name}'");
            }
        }
    }
}
