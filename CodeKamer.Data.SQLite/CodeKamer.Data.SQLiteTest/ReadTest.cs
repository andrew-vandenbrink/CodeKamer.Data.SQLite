using CodeKamer.Data.SQLite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace CodeKamer.Data.SQLiteTest
{
    public class ReadTest
    {
        private readonly ITestOutputHelper output;

        public ReadTest(ITestOutputHelper output)
        {
            this.output = output;
        }

        [Fact]
        public async Task NullTest()
        {
            SQLiteAPI.EnableSharedCache();
            var connection = SQLiteAPI.InMemorySharedConnectionString();

            output.WriteLine("ConnectionString");
            output.WriteLine(connection.Item1);

            output.WriteLine("Opening Connection");
            using (var connectionHandle = SQLiteAPI.OpenConnection(connection.Item1, connection.Item2))
            {

                output.WriteLine("Creating Table");
                var createTable = await SQLiteAPI.ExecuteNonQueryAsync(connection.Item1,
                    connection.Item2,
                    "CREATE TABLE person2(name text,age int)");
                output.WriteLine(createTable.ToString());
                Assert.Equal((int)SQLiteResultCode.SQLITE_OK, createTable);

                output.WriteLine("Inserting Values");
                var insertValue = await SQLiteAPI.ExecuteNonQueryAsync(connection.Item1,
                    connection.Item2,
                     "INSERT INTO person2 VALUES('andrew',null)");
                output.WriteLine(insertValue.ToString());
                Assert.Equal((int)SQLiteResultCode.SQLITE_OK, insertValue);

               
                var queryValue = await SQLiteAPI.ReadAsync(connection.Item1, connection.Item2,
                    "SELECT * FROM person2", false);
                Assert.True(queryValue.Count == 1);                

                Assert.Equal(queryValue[0][0].ToString(),"andrew");
                Assert.True(queryValue[0][1] == null);               
            }
        }
    }
}
