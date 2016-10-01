﻿using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace HadleyCapitalManagement.Norgate
{
    public class NorgateDataProcessor
    {
        private SQLiteConnection _Connection;

        public NorgateDataProcessor()
        {
            var dbFilePath = @"C:/Code/hadleycapitalmanagement/Data/Database.db";
            if (!File.Exists(dbFilePath))
            {
                SQLiteConnection.CreateFile(dbFilePath);
            }
            _Connection = new SQLiteConnection(string.Format("Data Source={0};Version=3;datetimeformat=CurrentCulture", dbFilePath));
            _Connection.Open();
        }

        public void Run()
        {
            //PurgeData();

            //ExtractDataIntoDatabase();

            //GenerateAdjustedCurve();                               
        }

        private void ExtractDataIntoDatabase()
        {
            string datafileslocation = @"C:\Trading Data\Extracted\";

            var extractedDirectories = Directory.GetDirectories(datafileslocation);

            var commodities = GetAllCommodities();

            foreach (var directory in extractedDirectories)
            {
                var directoryInfo = new DirectoryInfo(directory);

                var commodity = commodities.SingleOrDefault(c => c.FolderName == directoryInfo.Name);

                if (commodity == null) continue;

                var dataFiles = Directory.GetFiles(directory).Where(f => !f.Contains("names.txt"));

                foreach (var dataFile in dataFiles)
                {
                    var fileInfo = new FileInfo(dataFile);
                    var fullContractName = fileInfo.Name.Replace(fileInfo.Extension, string.Empty);

                    //C2___1979H
                    var contractName = fullContractName.Substring(fullContractName.Length - 5);
                    var monthCode = contractName.Last().ToString();
                    var year = int.Parse(contractName.Substring(0, 4));

                    Console.WriteLine(contractName);

                    var contractId = AddContract(contractName, monthCode, year, commodity.Id);

                    var lines = File.ReadAllLines(dataFile).Skip(1);

                    _Connection.Execute("BEGIN");

                    foreach (var line in lines)
                    {
                        var data = line.Replace("\"", string.Empty).Split(',');

                        var date = DateTime.ParseExact(data[0], "yyyyMMdd", null);
                        var open = decimal.Parse(data[1]);
                        var high = decimal.Parse(data[2]);
                        var low = decimal.Parse(data[3]);
                        var close = decimal.Parse(data[4]);
                        var volume = int.Parse(data[5]);
                        var openInterest = int.Parse(data[6]);

                        AddContractPrice(contractId, date, open, high, low, close, openInterest, volume);
                    }

                    _Connection.Execute("END");
                }
            }            
        }

        private void GenerateAdjustedCurve(NorgateCommodity commodity)
        {
            var curveDates = GetCurveDates(commodity.Id);
            var currentDate = curveDates.First();
            var currentContract = GetCurrentContract(currentDate, commodity.Id);
            var contracts = GetAllContracts(commodity.Id);
            var adjustmentFactor = 0m;
            var adjustedClose = 0m;

            foreach (var curveDate in curveDates)
            {
                var prices = GetContractPricesForDate(curveDate, commodity.Id);

                var currentContractClose = prices.FirstOrDefault(p => p.ContractId == currentContract.Id).Close;

                var liquidContractPrice = prices.FirstOrDefault(c => c.OpenInterest == prices.Max(p => p.OpenInterest));

                if (liquidContractPrice.ContractId == currentContract.Id)
                {
                    adjustedClose = currentContractClose + adjustmentFactor;
                }
                else
                {
                    currentContract = contracts.FirstOrDefault(c => c.Id == liquidContractPrice.ContractId);

                    adjustmentFactor = adjustmentFactor + (currentContractClose - liquidContractPrice.Close);

                    adjustedClose = liquidContractPrice.Close + adjustmentFactor;
                }

                AddAdjustedPrice(curveDate, currentContract.Name, currentContract.Id, adjustedClose, adjustmentFactor, liquidContractPrice.OpenInterest);

                Console.WriteLine(string.Format("{0},{1},{2},{3},{4}", curveDate.ToString("dd/MM/yy"), currentContract.Name, liquidContractPrice.OpenInterest, adjustedClose, adjustmentFactor));
            }
        }

        private List<NorgateCommodity> GetAllCommodities()
        {
            return _Connection.Query<NorgateCommodity>("SELECT Id, Name, MonthCodes, FirstContract, FolderName FROM NorgateCommodity;").ToList();
        }

        private List<NorgateContract> GetAllContracts(int commodityId)
        {
            return _Connection.Query<NorgateContract>("SELECT Id, Name, MonthCode, Year, CommodityId FROM NorgateContract WHERE CommodityId = @CommodityId", new { CommodityId = commodityId }).ToList();
        }

        private void PurgeData()
        {
            _Connection.Execute(@"
DELETE FROM NorgateContract; 
DELETE FROM NorgateContractPrice;
DELETE FROM NorgateAdjustedPrice;");
        }

        private NorgateContract GetCurrentContract(DateTime date, int commodityId)
        {
            return _Connection.Query<NorgateContract>(@"
select c.Id, c.Name, c.Year, c.MonthCode, c.CommodityId 
from NorgateContractPrice p, NorgateContract c 
where c.Id = p.ContractId 
and Date = @Date
and CommodityId = @CommodityId
and OpenInterest = (select Max(OpenInterest) from NorgateContractPrice where Date = @Date and CommodityId = @CommodityId)", new { Date = date, CommodityId = commodityId }).Single();
        }

        public IEnumerable<NorgateContractPrice> GetContractPricesForDate(DateTime date, int commodityId)
        {
            return _Connection.Query<NorgateContractPrice>(@"
SELECT p.Id, p.ContractId, p.Date, p.Open, p.High, p.Low, p.Close, p.OpenInterest, p.Volume, c.Name as ContractName 
FROM NorgateContractPrice p, NorgateContract c
WHERE p.Date = @Date
AND c.Id = p.ContractId
and c.CommodityId = @CommodityId", 
new { Date = date, CommodityId = commodityId });
        }

        private IEnumerable<DateTime> GetCurveDates(int commodityId)
        {
            return _Connection.Query<DateTime>(@"
select p.Date
from NorgateContractPrice p, NorgateContract c
where p.ContractId = c.Id and p.OpenInterest > 0 and c.CommodityId = commodityId
group by Date 
order by Date desc
", new { CommodityId = commodityId });
        }

        private DateTime GetEarliestDate(int commodityId)
        {
            return _Connection.ExecuteScalar<DateTime>("select min(Date) from NorgateContractPrice where ContractId in (select distinct Id from NorgateContract where CommodityId = 1)", new { CommodityId = commodityId });
        }

        private DateTime GetLatestDate(int commodityId)
        {
            return _Connection.ExecuteScalar<DateTime>("select Max(Date) from NorgateContractPrice where ContractId in (select distinct Id from NorgateContract where CommodityId = 1)", new { CommodityId = commodityId });
        }

        private int AddContract(string name, string monthCode, int year, int commodityId)
        {
            var id = _Connection.Query<int>(@"
INSERT INTO NorgateContract 
(CommodityId, Name, MonthCode, Year) 
VALUES (@CommodityId, @Name, @MonthCode, @Year); 
select last_insert_rowid()", new { Name = name, MonthCode = monthCode, Year = year, CommodityId = commodityId }).Single();

            return id;
        }

        private int AddContractPrice(int contractId, DateTime date, decimal open, decimal high, decimal low, decimal close, int openInterest, int volume)
        {
            var id = _Connection.Query<int>(@"
INSERT INTO NorgateContractPrice
(ContractId, Date, Open, High, Low, Close, OpenInterest, Volume) 
VALUES (@ContractId, @Date, @Open, @High, @Low, @Close, @OpenInterest, @Volume) ; 
select last_insert_rowid()", 
new { ContractId = contractId, Date = date, Open = open, High = high, Low = low, Close = close, OpenInterest = openInterest, volume = volume }).Single();

            return id;
        }

        private void AddAdjustedPrice(DateTime date, string contractName, int contractId, decimal adjustedClose, decimal adjustmentFactor, int openInterest)
        {
            _Connection.Execute(@"
INSERT INTO NorgateAdjustedPrice
(Date, ContractName, ContractId, AdjustedClose, AdjustmentFactor, OpenInterest)
VALUES (@Date, @ContractName, @ContractId, @AdjustedClose, @AdjustmentFactor, @OpenInterest)
", new { Date = date, ContractName = contractName, ContractId = contractId, AdjustedClose = adjustedClose, AdjustmentFactor = adjustmentFactor, OpenInterest = openInterest });
        }
    }
}
