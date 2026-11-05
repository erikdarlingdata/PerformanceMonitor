using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

public class ConfigPrinterTests
{
    [Fact]
    public void Secrets_are_not_printed_and_destination_secrets_are_redacted()
    {
        var cfg = ConfigLoader.Parse("""
            { "config_version": 1,
              "store": { "name": "store_a", "password": { "source": "env", "name": "MY_PW_VAR" } },
              "paths": { "out": "/o", "state": "/s", "logs": "/l" },
              "destination": { "type": "s3", "bucket": "b", "secret_access_key": "AKIA-SECRET", "auth": { "sas_url": "https://x?sig=SECRET2", "name": "ENV_NAME_ok", "connection_string_env": "CS_VAR" } } }
            """, false);
        var text = ConfigPrinter.Render(cfg, credentialResolved: true);
        Assert.DoesNotContain("AKIA-SECRET", text);
        Assert.DoesNotContain("SECRET2", text);
        Assert.Contains(ConfigPrinter.Redacted, text);
        Assert.Contains("MY_PW_VAR", text);   // the env var NAME is shown; its value is never read here
        Assert.Contains("CS_VAR", text);
        Assert.Contains("\"bucket\": \"b\"", text);
    }
}
