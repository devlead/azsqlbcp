namespace AzSqlBcp.Services.Checkpoint;

public sealed class ChangeTrackingInfo
{
    public bool Enabled { get; set; }

    public long? SyncStartVersion { get; set; }

    public DateTimeOffset? CapturedUtc { get; set; }
}
