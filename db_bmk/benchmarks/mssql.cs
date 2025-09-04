using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using System.Data;

namespace db_bmk.benchmarks;

[MemoryDiagnoser]
public class MsSQL : Base
{   
    private const string ConnectionString = "Data Source=localhost;Persist Security Info=True;Password=Password$4;User ID=sa;Initial Catalog=Board_Local;TrustServerCertificate=true;Encrypt=false;MultipleActiveResultSets=true;";
    private const string InsertStatement = "INSERT INTO testdata (name, value) VALUES (@name, @value);";
    private const string ReadStatement = "SELECT * FROM testdata WHERE value = @value;";

    [GlobalSetup(Targets = [nameof(EvalInsertAsync), nameof(EvalBulkInsertAsync)])]
    public void SetupInsert()
    {
        using var connection = new SqlConnection(ConnectionString);
        connection.Open();

        using var createDbCmd = new SqlCommand(@"
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'benchmark')
            CREATE DATABASE benchmark;", connection);
        createDbCmd.ExecuteNonQuery();

        connection.ChangeDatabase("benchmark");

        using var dropCmd = new SqlCommand(@"
            IF OBJECT_ID('testdata', 'U') IS NOT NULL 
            DROP TABLE testdata;", connection);
        dropCmd.ExecuteNonQuery();

        using var createCmd = new SqlCommand(@"
            CREATE TABLE testdata (
                id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
                name NVARCHAR(255),
                value INT
            );", connection);
        createCmd.ExecuteNonQuery();

        using var indexCmd = new SqlCommand(@"
            CREATE INDEX IX_testdata_value ON testdata (value);", connection);
        indexCmd.ExecuteNonQuery();

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
        using var connection = new SqlConnection(ConnectionString);
        connection.Open();

        connection.ChangeDatabase("benchmark");

        var insertCommand = new SqlCommand(InsertStatement, connection);
        insertCommand.Parameters.Add("@name", SqlDbType.NVarChar, 255);
        insertCommand.Parameters.Add("@value", SqlDbType.Int);
        insertCommand.Prepare();

        foreach (var c in Enumerable.Range(i, count))
        {
            insertCommand.Parameters["@name"].Value = string.Empty;
            insertCommand.Parameters["@value"].Value = c;
            var result = await insertCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            if (result <= 0) throw new InvalidOperationException("Write failure");
        }

        connection.Close();
    }

    private async Task BulkInsert(int i, int count)
    {
        using var connection = new SqlConnection(ConnectionString);
        connection.Open();
        connection.ChangeDatabase("benchmark");

        using var transaction = connection.BeginTransaction();
        using var insertCommand = new SqlCommand(InsertStatement, connection);
        insertCommand.Parameters.Add("@name", SqlDbType.NVarChar, 255);
        insertCommand.Parameters.Add("@value", SqlDbType.Int);
        insertCommand.Transaction = transaction;
        insertCommand.Prepare();

        foreach (var c in Enumerable.Range(i, count))
        {
            insertCommand.Parameters["@name"].Value = string.Empty;
            insertCommand.Parameters["@value"].Value = c;
            var result = await insertCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
            if (result <= 0) throw new InvalidOperationException("Write failure");
        }

        await transaction.CommitAsync().ConfigureAwait(false);

        connection.Close();
    }

    private async Task<object> Read(int i)
    {
        using var readConnection = new SqlConnection(ConnectionString);
        await readConnection.OpenAsync().ConfigureAwait(false);
        readConnection.ChangeDatabase("benchmark");
        
        using var readCommand = new SqlCommand(ReadStatement, readConnection);
        readCommand.Parameters.Add("@value", SqlDbType.Int).Value = i;
        
        using var reader = await readCommand.ExecuteReaderAsync().ConfigureAwait(false);
        
        if (await reader.ReadAsync())
        {
            return new
            {
                id = reader["id"],
                name = reader["name"],
                value = reader["value"]
            };
        }

        throw new InvalidOperationException($"Value {i} not found");
    }
}