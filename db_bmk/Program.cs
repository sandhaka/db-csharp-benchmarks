using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using BenchmarkDotNet.Order;
using db_bmk.benchmarks;

internal class Program
{
    private static void Main(string[] args)
    {
        var config = ManualConfig.Create(DefaultConfig.Instance)
            .AddJob(Job.Default.WithToolchain(InProcessEmitToolchain.Instance))
            .WithOrderer(DefaultOrderer.Instance); // Maintains method declaration order

        // Run benchmarks in order: Insert tests first, then Read tests
        // BenchmarkRunner.Run<MsSQL>(config);
        // BenchmarkRunner.Run<Scylla>(config);
        // BenchmarkRunner.Run<Mongo>(config);
        // BenchmarkRunner.Run<ClickHouseDB>(config);
        BenchmarkRunner.Run<Elasticsearch>(config);
    }
}