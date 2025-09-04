using BenchmarkDotNet.Attributes;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Driver.Utility;

namespace db_bmk.benchmarks;

[MemoryDiagnoser]
public class ClickHouseDB : Base
{
    private const string ConnectionString = "Host=localhost;Protocol=http;Database=default;Username=default;Password=;";
    private const string ReadStatement = "SELECT id, name, value FROM benchmark.testdata WHERE value = {value:Int32}";
    private const string WriteStatement = "INSERT INTO benchmark.testdata (name, value) VALUES ({name:String}, {value:Int32})";

    [GlobalSetup(Targets = [nameof(EvalInsertAsync), nameof(EvalBulkInsertAsync)])]
    public void SetupInsert()
    {
        using var connection = new ClickHouseConnection(ConnectionString);
        connection.Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE DATABASE IF NOT EXISTS benchmark";
            command.ExecuteNonQuery();
        }

        connection.ChangeDatabase("benchmark");

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "DROP TABLE IF EXISTS testdata";
            command.ExecuteNonQuery();
        }

        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
                CREATE TABLE testdata (
                    id String DEFAULT generateUUIDv4(),
                    name String,
                    value Int32
                ) ENGINE = MergeTree()
                ORDER BY id";
            command.ExecuteNonQuery();
        }

        // Create index on value column for better query performance
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "ALTER TABLE testdata ADD INDEX idx_value value TYPE bloom_filter GRANULARITY 1";
            command.ExecuteNonQuery();
        }

        connection.Close();
    }

    [Benchmark]
    [BenchmarkCategory("Insert")]
    public async Task EvalInsertAsync() => await MainInsertsLoopAsync(Insert);

    [Benchmark]
    [BenchmarkCategory("Insert")]
    public async Task EvalBulkInsertAsync() => await MainInsertsLoopAsync(BulkInsert);

    [Benchmark]
    [BenchmarkCategory("Read")]
    public async Task<object> EvalQueryAsync() => await MainReadLoopAsync(Read);

    private async Task Insert(int i, int count)
    {
        using var connection = new ClickHouseConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        connection.ChangeDatabase("benchmark");

        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = WriteStatement;
            cmd.AddParameter("value", "Int32");
            cmd.AddParameter("name", "String");

            foreach (var c in Enumerable.Range(i, count))
            {
                cmd.Parameters["value"].Value = c;
                cmd.Parameters["name"].Value = string.Empty;
                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        await connection.CloseAsync().ConfigureAwait(false);
    }

    private async Task BulkInsert(int i, int count)
    {
        using var connection = new ClickHouseConnection(ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        connection.ChangeDatabase("benchmark");

        using var bulk = new ClickHouseBulkCopy(connection)
        {
            DestinationTableName = "benchmark.testdata",
            MaxDegreeOfParallelism = Base.MaxConcurrencyLevel,
            BatchSize = 100,
            ColumnNames = ["id", "name", "value"]
        };

        await bulk.InitAsync().ConfigureAwait(false);
        var values = Enumerable.Range(i, count).Select(i => new object[] { Guid.NewGuid(), string.Empty, i });
        await bulk.WriteToServerAsync(values).ConfigureAwait(false);
        await connection.CloseAsync().ConfigureAwait(false);
    }

    private async Task<object> Read(int i)
    {
        using var connection = new ClickHouseConnection(ConnectionString);
        await connection.OpenAsync();
        connection.ChangeDatabase("benchmark");
        
        using var command = connection.CreateCommand();
        var parameter = command.CreateParameter();
        parameter.ParameterName = "value";
        parameter.Value = i;
        command.Parameters.Add(parameter);
        
        command.CommandText = ReadStatement;
        using var reader = command.ExecuteReader();

        if (reader.Read())
        {
            return Task.FromResult<object>(new
            {
                name = reader.GetString(1),
                value = reader.GetInt32(2)
            });
        }
        
        throw new InvalidOperationException($"Value {i} not found.");
    }
}