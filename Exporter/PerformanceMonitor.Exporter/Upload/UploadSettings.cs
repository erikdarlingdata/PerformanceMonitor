using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;

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

            CheckBucket(bucket, "--upload bucket");
            CheckPrefix(prefix, "--upload prefix", local: false);
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
                return new Local(RequiredString(element, "path", "destination"), OptionalPrefix(element, local: true), hive);

            case "s3":
                RejectUnknown(element, "destination", "type", "bucket", "prefix", "region", "endpoint_url", "force_path_style", "checksum", "credentials", "hive_store_folder");
                var checksumText = OptionalString(element, "checksum") ?? "sha256";
                var checksum = checksumText switch
                {
                    "sha256" => S3Checksum.Sha256,
                    "none" => S3Checksum.None,
                    _ => throw new ArgumentException($"destination.checksum must be 'sha256' or 'none', got '{checksumText}'"),
                };
                var endpointUrl = OptionalString(element, "endpoint_url");
                if (endpointUrl is not null)
                {
                    if (!Uri.TryCreate(endpointUrl, UriKind.Absolute, out var endpoint) || (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps))
                    {
                        throw new ArgumentException("destination.endpoint_url is not a valid http or https URL");
                    }

                    RejectSecretInUrl(endpointUrl, endpoint, "endpoint_url", "name the keys with destination.credentials");
                    RejectLinkLocalHost(endpoint, "endpoint_url");
                }

                var bucket = RequiredString(element, "bucket", "destination");
                CheckBucket(bucket, "destination.bucket");
                return new S3(
                    bucket,
                    OptionalPrefix(element),
                    OptionalString(element, "region"),
                    endpointUrl,
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

                if (accountUrl is not null)
                {
                    if (!Uri.TryCreate(accountUrl, UriKind.Absolute, out var account))
                    {
                        throw new ArgumentException("destination.account_url is not a valid URL");
                    }

                    RejectSecretInUrl(accountUrl, account, "account_url", "put the SAS in an environment variable and use auth.method 'sas_url_env'");
                    RejectLinkLocalHost(account, "account_url");
                    if (!IsHttpsOrLoopbackHttp(account))
                    {
                        throw new ArgumentException("destination.account_url must be an https URL; http is allowed only for a loopback address (localhost, 127.0.0.0/8 or ::1), such as Azurite");
                    }
                }

                var container = RequiredString(element, "container", "destination");
                if (container is not ("$root" or "$web") && (!AzureContainerName.IsMatch(container) || container.Contains("--", StringComparison.Ordinal)))
                {
                    throw new ArgumentException("destination.container must be 3 to 63 characters: lowercase letters, digits and hyphens, starting and ending with a letter or digit, with no two hyphens in a row, or the special container '$root' or '$web'");
                }

                return new AzureBlob(accountUrl?.TrimEnd('/'), container, OptionalPrefix(element), auth, hive);

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
            RequiredVariableName(credentials, "access_key_id_env", "destination.credentials"),
            RequiredVariableName(credentials, "secret_access_key_env", "destination.credentials"),
            OptionalString(credentials, "session_token_env") is null ? null : RequiredVariableName(credentials, "session_token_env", "destination.credentials"));
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
                return new AzureAuth.ConnectionStringEnv(RequiredVariableName(auth, "name", "destination.auth"));
            case "sas_url_env":
                RejectUnknown(auth, "destination.auth", "method", "name");
                return new AzureAuth.SasUrlEnv(RequiredVariableName(auth, "name", "destination.auth"));
            default:
                throw new ArgumentException($"destination.auth.method must be 'default_credential', 'connection_string_env' or 'sas_url_env', got '{method}'");
        }
    }

    /// <summary>
    /// A URL in the config is printed by `check-config` and in the upload log lines, so it must not carry a key: no user
    /// name or password, no query string or fragment, and no path that holds a query or a SAS in disguise (a percent-encoded
    /// '?', or a segment with '=' or '&amp;'). A plain path such as Azurite's /devstoreaccount1 is allowed. The message never
    /// repeats the URL.
    /// </summary>
    private static void RejectSecretInUrl(string text, Uri url, string name, string instead)
    {
        var path = url.AbsolutePath;
        var pathCarriesQuery = path.Contains("%3F", StringComparison.OrdinalIgnoreCase)
            || path.Split('/').Any(segment => Uri.UnescapeDataString(segment).AsSpan().IndexOfAny('=', '&') >= 0);
        if (url.UserInfo.Length > 0 || url.Query.Length > 0 || url.Fragment.Length > 0 || text.Contains('?') || text.Contains('#') || pathCarriesQuery)
        {
            throw new ArgumentException($"destination.{name} must not carry a user name, a password, a query string, a fragment or a SAS in the path; {instead}");
        }
    }

    /// <summary>The cloud metadata range (169.254.0.0/16 and fe80::/10) is never a storage endpoint.</summary>
    private static void RejectLinkLocalHost(Uri url, string name)
    {
        if (TryHostAddress(url, out var address) && (address.IsIPv6LinkLocal || IsIPv4LinkLocal(address)))
        {
            throw new ArgumentException($"destination.{name} must not point at a link-local address (169.254.0.0/16 or fe80::/10)");
        }
    }

    private static bool IsIPv4LinkLocal(IPAddress address)
    {
        var v4 = address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;
        var bytes = v4.GetAddressBytes();
        return bytes.Length == 4 && bytes[0] == 169 && bytes[1] == 254;
    }

    /// <summary>A host that is an IP literal, with the brackets of an IPv6 literal and any zone id removed.</summary>
    private static bool TryHostAddress(Uri url, out IPAddress address)
    {
        var host = url.Host;
        if (host.StartsWith('[') && host.EndsWith(']'))
        {
            host = host[1..^1];
        }

        var zone = host.IndexOf('%');
        return IPAddress.TryParse(zone >= 0 ? host[..zone] : host, out address!);
    }

    /// <summary>https for any host, or http for a loopback host only (localhost, 127.0.0.0/8 or ::1), so a SAS never travels in the clear.</summary>
    private static bool IsHttpsOrLoopbackHttp(Uri url)
    {
        if (url.Scheme == Uri.UriSchemeHttps)
        {
            return true;
        }

        if (url.Scheme != Uri.UriSchemeHttp)
        {
            return false;
        }

        if (string.Equals(url.Host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return TryHostAddress(url, out var address) && IPAddress.IsLoopback(address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address);
    }

    private static readonly Regex AzureContainerName = new("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    private static readonly Regex DriveLetter = new("^[A-Za-z]:", RegexOptions.CultureInvariant);

    /// <summary>
    /// An S3 bucket name is a single path element once it reaches the request path, so a name with a path separator or the
    /// name '.' or '..' is refused. Legacy bucket names with other characters stay legal.
    /// </summary>
    private static void CheckBucket(string bucket, string where)
    {
        if (bucket.Contains('/') || bucket.Contains('\\') || bucket is "." or "..")
        {
            throw new ArgumentException($"{where} must be a bucket name, not a path: no '/' or '\\' and not '.' or '..'");
        }
    }

    /// <summary>
    /// A prefix is joined onto the destination, so it must stay inside it: no backslash, no '.' or '..' segment (a segment of
    /// only dots and spaces counts, because Windows ignores the trailing ones), and for a local destination no drive letter
    /// (which makes Path.Combine drop the destination path). The message never repeats the prefix.
    /// </summary>
    private static void CheckPrefix(string prefix, string where, bool local)
    {
        if (prefix.Contains('\\'))
        {
            throw new ArgumentException($"{where} must not contain a backslash; separate folders with '/'");
        }

        if (prefix.Split('/').Any(segment => segment.Length > 0 && segment.Trim(' ', '.').Length == 0))
        {
            throw new ArgumentException($"{where} must not contain a '.' or '..' segment");
        }

        if (local && DriveLetter.IsMatch(prefix))
        {
            throw new ArgumentException($"{where} must be a relative folder, not a drive");
        }
    }

    private static string OptionalPrefix(JsonElement element, bool local = false)
    {
        var prefix = (OptionalString(element, "prefix") ?? "").Trim('/');
        CheckPrefix(prefix, "destination.prefix", local);
        return prefix;
    }

    private static string RequiredString(JsonElement element, string name, string where)
    {
        var value = OptionalString(element, name);
        return string.IsNullOrWhiteSpace(value) ? throw new ArgumentException($"{where}.{name} is required") : value;
    }

    /// <summary>
    /// A field that names an environment variable. The name is printed by `check-config`, so a value that is not a plain name
    /// (a SAS, a connection string, or a key with '/', '+' or '=' pasted in its place) is refused without repeating it. A key made
    /// only of letters, digits and underscores looks like a name and is accepted, so no message repeats a variable name: they
    /// name the config key instead.
    /// </summary>
    private static string RequiredVariableName(JsonElement element, string name, string where)
    {
        var value = RequiredString(element, name, where);
        var plain = (char.IsAsciiLetter(value[0]) || value[0] == '_') && value.All(c => char.IsAsciiLetterOrDigit(c) || c == '_');
        return plain ? value : throw new ArgumentException($"{where}.{name} must be the name of an environment variable (letters, digits and underscores), not the key itself");
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
