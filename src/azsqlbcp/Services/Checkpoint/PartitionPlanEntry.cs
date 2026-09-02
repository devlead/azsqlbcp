namespace AzSqlBcp.Services.Checkpoint;

public sealed class PartitionPlanEntry
{
    public int Index { get; set; }

    public long Lo { get; set; }

    public long Hi { get; set; }

    public long ExpectedRows { get; set; }

    public PartitionStatus Status { get; set; } = PartitionStatus.Pending;

    public long RowsCopied { get; set; }

    public int Attempts { get; set; }

    public DateTimeOffset? CompletedUtc { get; set; }

    public BulkCopyService.IdRange ToIdRange() => new(Index, Lo, Hi);
}
