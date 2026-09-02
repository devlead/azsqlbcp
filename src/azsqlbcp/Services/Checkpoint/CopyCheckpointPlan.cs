namespace AzSqlBcp.Services.Checkpoint;

public sealed class CopyCheckpointPlan
{
    public long TotalRowCount { get; set; }

    public DateTimeOffset CreatedUtc { get; set; }

    public List<PartitionPlanEntry> Ranges { get; set; } = [];
}
