namespace PerformanceMonitor.Darling.Service.Mcp;

// Where the host's GCF filter gets its format from (#5320). The real service registers none,
// so GcfCallToolFilter reads DARLING_OUTPUT_FORMAT through GcfOutput.Enabled on each call,
// exactly as before. A test host registers one to pin the format, so a test class that sets
// the environment variable (or a live class running beside it) cannot re-encode another
// class's answer mid-run.
public interface IGcfOutputFormat
{
    bool Enabled { get; }
}

// A fixed answer, for a host that must not follow the environment.
public sealed class FixedGcfOutputFormat : IGcfOutputFormat
{
    public FixedGcfOutputFormat(bool enabled) => Enabled = enabled;

    public bool Enabled { get; }
}
