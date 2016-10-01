using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace Hadley
{
    public class NorgateProcessor
    {
        private readonly SQLiteConnection _Connection;

        public NorgateProcessor()
        {
            var dbFilePath = "./Database.sqlite";
            if (!File.Exists(dbFilePath))
            {
                SQLiteConnection.CreateFile(dbFilePath);
            }
            _Connection = new SQLiteConnection(string.Format("Data Source={0};Version=3;datetimeformat=CurrentCulture", dbFilePath));
            _Connection.Open();

            SeedDatabase();

            PurgeData();
        }

        private void SeedDatabase()
        {
            _Connection.Execute(@"
CREATE TABLE IF NOT EXISTS Contract (
    Name      VARCHAR (50),
    Year      INTEGER,
    MonthCode VARCHAR (50),
    Id        INTEGER      PRIMARY KEY AUTOINCREMENT
                           UNIQUE
)");
            
            _Connection.Execute(@"
CREATE TABLE IF NOT EXISTS [ContractPrice] (
    ContractId   INTEGER         REFERENCES Contract (Id),
    Date         DATETIME,
    Close        DECIMAL (18, 4),
    OpenInterest INTEGER
)");            
        }

        public void Run()
        {            
            string location = @"C:\Data\Corn C\";

            var files = Directory.GetFiles(location);

            foreach (var file in files)
            {
                Console.WriteLine(file);

                // GET CONTRACT ID
                var fileInfo = new FileInfo(file);
                var name = fileInfo.Name.Replace(fileInfo.Extension, string.Empty);
                name = name.Substring(name.Length - 5);
                var year = int.Parse(name.Substring(0, 4));
                var monthCode = name.Last().ToString();

                var contractId = AddContract(name, year, monthCode);

                var lines = File.ReadAllLines(file).Skip(1);

                _Connection.Execute("BEGIN");

                foreach (var line in lines)
                {
                    var data = line.Split(',');

                    var date = DateTime.ParseExact(data[0].Replace("\"", string.Empty), "yyyyMMdd", null);
                    var close = decimal.Parse(data[4].Replace("\"", string.Empty));
                    var openinterest = int.Parse(data[6].Replace("\"", string.Empty));

                    AddContractPrice(contractId, date, close, openinterest);
                }

                _Connection.Execute("END");
            }
        }

        private void PurgeData()
        {
            _Connection.Execute("DELETE FROM Contract; DELETE FROM ContractPrice");
        }

        private long AddContract(string name, int year, string monthCode)
        {
            var id = _Connection.Query<long>(@"
INSERT INTO Contract 
(Name, Year, MonthCode)
VALUES (@Name, @Year, @MonthCode);
select last_insert_rowid()", new { Name = name, Year = year, MonthCode = monthCode }).Single();

            return id;
        }

        private long AddContractPrice(long contractId, DateTime date, decimal close, int openInterest)
        {
            var id = _Connection.Query<long>(@"
INSERT INTO ContractPrice 
(ContractId, Date, Close, OpenInterest)
VALUES (@ContractId, @Date, @Close, @OpenInterest);
select last_insert_rowid()", new { ContractId = contractId, Date = date, Close = close, OpenInterest = openInterest }).Single();

            return id;
        }
    }
}
