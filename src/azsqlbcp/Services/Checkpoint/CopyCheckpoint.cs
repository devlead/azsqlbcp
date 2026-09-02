namespace AzSqlBcp.Services.Checkpoint;

public sealed class CopyCheckpoint
{
    public const int CurrentVersion = 1;

    public int Version { get; set; } = CurrentVersion;

    public CopyCheckpointJob Job { get; set; } = null!;

    public ChangeTrackingInfo ChangeTracking { get; set; } = new();

    public CopyCheckpointPlan Plan { get; set; } = null!;
}
