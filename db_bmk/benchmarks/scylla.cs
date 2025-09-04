using BenchmarkDotNet.Attributes;
using Cassandra;

namespace db_bmk.benchmarks;

[MemoryDiagnoser]
public class Scylla : Base
{
    private ISession _session;
    private ICluster _cluster;
    private PreparedStatement _is;
    private PreparedStatement _rs;

    private const string InsertStatement = "INSERT INTO benchmarkkeyspace.testdata (id, name, value) VALUES (uuid(), ?, ?);";
    private const string ReadStatement = "SELECT * FROM benchmarkkeyspace.testdata WHERE value = ? ALLOW FILTERING;";

    [GlobalSetup(Targets = [nameof(EvalInsertAsync), nameof(EvalBulkInsertAsync)])]
    public void SetupInsert()
    {
        _cluster = Cluster.Builder()
            .AddContactPoint("127.0.0.1")
            .WithPoolingOptions(new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 100))
            .WithQueryOptions(new QueryOptions()
                .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
            .Build();

        _session = _cluster.Connect();

        _session.Execute(new SimpleStatement(@"
            DROP KEYSPACE IF EXISTS benchmarkkeyspace;
        "));

        _session.Execute(new SimpleStatement(@"
            CREATE KEYSPACE benchmarkkeyspace
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        "));

        // Table design matching other databases: id as primary key, value as indexed column
        _session.Execute(new SimpleStatement(@"
            CREATE TABLE benchmarkkeyspace.testdata (
                id UUID PRIMARY KEY,
                name text,
                value int
            );
        "));

        // Create secondary index on value column for query performance (ScyllaDB specific optimization)
        _session.Execute(new SimpleStatement(@"
            CREATE INDEX IF NOT EXISTS idx_value 
            ON benchmarkkeyspace.testdata (value);
        "));
        _is = _session.Prepare(InsertStatement);
    }

    [GlobalSetup(Targets = [nameof(EvalQueryAsync)])]
    public void SetupRead()
    {
        _cluster = Cluster.Builder()
            .AddContactPoint("127.0.0.1")
            .WithPoolingOptions(new PoolingOptions()
                .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                .SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, 100))
            .WithQueryOptions(new QueryOptions()
                .SetConsistencyLevel(ConsistencyLevel.LocalOne)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
            .Build();

        _session = _cluster.Connect();

        _rs = _session.Prepare(ReadStatement);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _session.Dispose();
        _cluster.Dispose();
    }

    [Benchmark]
    [BenchmarkCategory("Insert")]
    public async Task EvalInsertAsync() => await MainInsertsLoopAsync(Insert);

    [Benchmark]
    [BenchmarkCategory("Insert")]
    public async Task EvalBulkInsertAsync() => await MainInsertsLoopAsync(BulkInsert);

    [Benchmark]
    [BenchmarkCategory("read")]
    public async Task<object> EvalQueryAsync() => await MainReadLoopAsync(Read);

    private async Task Insert(int i, int count)
    {
        foreach (var c in Enumerable.Range(i, count))
        {
            var bs = _is.Bind(string.Empty, c);
            await _session.ExecuteAsync(bs).ConfigureAwait(false);
        }
    }

    private async Task BulkInsert(int i, int count)
    {
        var batch = new BatchStatement()
            .SetBatchType(BatchType.Unlogged);

        foreach (var c in Enumerable.Range(i, count))
        {
            batch.Add(_is.Bind(string.Empty, c));
        }

        await _session.ExecuteAsync(batch).ConfigureAwait(false);
    }

    private async Task<object> Read(int i)
    {
        var bs = _rs.Bind(i);
        var r = await _session.ExecuteAsync(bs).ConfigureAwait(false);
        
        var row = r.FirstOrDefault();
        if (row == null)
            throw new InvalidOperationException($"Value {i} not found");
        return new
        {
            value = row.GetValue<int>("value"),
            name = row.GetValue<string>("name")
        };
    }
}