using AzSqlBcp.Commands;
using AzSqlBcp.Services;
using Xunit;

namespace AzSqlBcp.Tests;

public class BuildRangesTests
{
    [Fact]
    public void BuildRanges_SplitsMinMaxIntoEqualIdSpans()
    {
        var ranges = BulkCopyService.BuildRanges(1, 618, 8);

        Assert.Equal(8, ranges.Count);
        Assert.Equal(1, ranges[0].Lo);
        Assert.Equal(618, ranges[^1].Hi);
        Assert.Equal(ranges[0].Hi + 1, ranges[1].Lo);
    }

    [Fact]
    public void BuildRanges_CapsPartitionsToIdSpan()
    {
        var ranges = BulkCopyService.BuildRanges(10, 12, 8);

        Assert.Equal(3, ranges.Count);
        Assert.Equal(10, ranges[0].Lo);
        Assert.Equal(12, ranges[^1].Hi);
    }
}

public class RowBalancedPlanTests
{
    [Fact]
    public void BuildFromHistogram_DistributesRowsAcrossPartitions()
    {
        var histogram = new List<(long Key, long Count)>
        {
            (1, 100),
            (2, 200),
            (3, 700)
        };

        var entries = CopyPartitionPlan.BuildFromHistogram(histogram, partitions: 2, totalRowCount: 1000);

        Assert.Equal(2, entries.Count);
        Assert.Equal(300, entries[0].ExpectedRows);
        Assert.Equal(700, entries[1].ExpectedRows);
        Assert.Equal(1, entries[0].Lo);
        Assert.Equal(2, entries[0].Hi);
        Assert.Equal(3, entries[1].Lo);
        Assert.Equal(3, entries[1].Hi);
    }
}

public class CopyCheckpointTests
{
    [Fact]
    public void Fingerprint_ChangesWhenJobParametersChange()
    {
        var jobA = CreateJob(partitions: 8);
        var jobB = CreateJob(partitions: 16);

        Assert.NotEqual(jobA.ComputeFingerprint(), jobB.ComputeFingerprint());
    }

    [Fact]
    public void Fingerprint_IgnoresBatchSizeAndParallelism()
    {
        var jobA = CreateJob(partitions: 8, parallelism: 4, batchSize: 500_000);
        var jobB = CreateJob(partitions: 8, parallelism: 8, batchSize: 251_231);

        Assert.Equal(jobA.ComputeFingerprint(), jobB.ComputeFingerprint());
    }

    [Fact]
    public void Store_RoundTripsCheckpointWithChangeTracking()
    {
        var path = Path.Combine(Path.GetTempPath(), $"azsqlbcp-test-{Guid.NewGuid():N}.json");

        try
        {
            CopyCheckpoint checkpoint;
            using (var store = CopyCheckpointStore.Open(path))
            {
                checkpoint = new CopyCheckpoint
                {
                    Job = CreateJob(partitions: 4),
                    ChangeTracking = new ChangeTrackingInfo
                    {
                        Enabled = true,
                        SyncStartVersion = 12345,
                        CapturedUtc = DateTimeOffset.UtcNow
                    },
                    Plan = new CopyCheckpointPlan
                    {
                        TotalRowCount = 1000,
                        CreatedUtc = DateTimeOffset.UtcNow,
                        Ranges =
                        [
                            new PartitionPlanEntry
                            {
                                Index = 0,
                                Lo = 1,
                                Hi = 10,
                                ExpectedRows = 1000,
                                Status = PartitionStatus.Pending
                            }
                        ]
                    }
                };

                store.Save(checkpoint);
            }

            using (var store = CopyCheckpointStore.Open(path))
            {
                var loaded = store.Load();
                Assert.True(loaded.ChangeTracking.Enabled);
                Assert.Equal(12345, loaded.ChangeTracking.SyncStartVersion);
                Assert.Single(loaded.Plan.Ranges);
                Assert.Equal(1000, loaded.Plan.Ranges[0].ExpectedRows);
            }
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);

            var lockPath = path + ".lock";
            if (File.Exists(lockPath))
                File.Delete(lockPath);
        }
    }

    private static CopyCheckpointJob CreateJob(
        int partitions,
        int parallelism = 4,
        int batchSize = 1_000_000) => new()
    {
        SourceServer = "source.database.windows.net",
        SourceDatabase = "SourceDb",
        SourceTable = "dbo.Table",
        TargetServer = "target.database.windows.net",
        TargetDatabase = "TargetDb",
        TargetTable = "bak.Table",
        PartitionColumn = "Id",
        PartitionStrategy = PartitionStrategy.IdRange,
        Partitions = partitions,
        Parallelism = parallelism,
        BatchSize = batchSize
    };
}

public class SqlTransientTests
{
    [Fact]
    public void IsTransient_ReturnsTrueForIOException()
    {
        Assert.True(SqlTransient.IsTransient(new IOException("connection reset")));
    }

    [Fact]
    public void IsTransient_ReturnsFalseForArgumentException()
    {
        Assert.False(SqlTransient.IsTransient(new ArgumentException("bad arg")));
    }
}
