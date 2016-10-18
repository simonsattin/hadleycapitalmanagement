using HadleyCapitalManagement.DataAccess;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using HtmlAgilityPack;
using System.Globalization;

namespace HadleyCapitalManagement.Quandl
{
    public class QuandlPriceHistoryDownloader
    {
        private SQLiteConnection _Connection;
        private string _SqliteDatabaseLocation;

        public QuandlPriceHistoryDownloader()
        {
            _SqliteDatabaseLocation = ConfigurationManager.AppSettings[Environment.MachineName + "_SqliteDatabaseLocation"];
            //var dbFilePath = @"C:/Code/hadleycapitalmanagement/Data/Database.db";
            if (!File.Exists(_SqliteDatabaseLocation))
            {
                throw new Exception();
            }
            _Connection = new SQLiteConnection(string.Format("Data Source={0};Version=3;datetimeformat=CurrentCulture", _SqliteDatabaseLocation));
            _Connection.Open();
        }

        public void Run()
        {
            //PurgeData();

            var commodityList = GetAllCommodities();

            foreach (var commodity in commodityList)
            {
                Console.WriteLine(commodity.Name);

                // FindAndExtractCommodityContractsAndPrices(commodity);

                //GenerateAdjustedCurve(commodity);
                //GenerateContractRoll(commodity);

                GenerateBackAdjustedContinuousContract(commodity);
            }

            Console.WriteLine("Completed");
        }   

        private void DownloadAllSCFAssets()
        {
            var downloadClient = new WebClient();
            string downloadFolder = @"D:\TradingData\SCF\{0}.csv";
            string urlseed = "https://www.quandl.com/api/v3/datasets/{0}.csv?api_key=MXrRoowvUAjrUCFEp63J";
            var codes = GetSCFCodes();

            foreach (var code in codes)
            {
                var url = string.Format(urlseed, code);
                var downloadLocation = string.Format(downloadFolder, code.Replace("SCF/", string.Empty));

                Console.WriteLine(code);

                downloadClient.DownloadFile(url, downloadLocation);
            }
        }
        
        private void FindAndExtractCommodityContractsAndPrices(QuandlCommodity commodity)
        {
            bool hasDoneFirstContract = false;
            var downloadClient = new WebClient();
            var contractsListWeb = new HtmlWeb();
            var contractsListDocument = contractsListWeb.Load(commodity.ContractListUrl);

            //.//h2[@id='All+Individual+Contracts']/following-sibling::table//td/a[@href]
            //.//h2[@id='Historical+Contracts']/following-sibling::table//td/a[@href]
            var contractNodes = contractsListDocument.DocumentNode.SelectNodes(commodity.ContractListRegex);
            //var contractNodes = contractsListDocument.DocumentNode.SelectNodes(".//h2[@id='All+Individual+Contracts']/following-sibling::table//td/a[@href]");

            for (int i = 0; i < contractNodes.Count; i++)
            {
                var contractNode = contractNodes[i];
                var contractName = contractNode.InnerText;

                if (!string.IsNullOrEmpty(commodity.FirstContract))
                {
                    if (!hasDoneFirstContract)
                    {
                        if (contractName == commodity.FirstContract)
                        {
                            hasDoneFirstContract = true;
                        }
                        else
                        {
                            continue;
                        }
                    }
                }             

                Console.WriteLine(string.Format("{0} {1}%", contractName, Math.Round(((decimal)i / (decimal)contractNodes.Count)*100,2)));

                var year = contractName.Substring(contractName.Length - 4);
                var monthCode = contractName.Substring(commodity.Code.Length, 1);

                //add to database
                var contractId = AddContract(contractName, monthCode, year, commodity.Id);

                var contractUrl = contractNode.GetAttributeValue("href", string.Empty);

                var dataUrl = string.Format("https://www.quandl.com/api/v1/datasets{0}.csv?api_key=MXrRoowvUAjrUCFEp63J", contractUrl);

                var pricedata = downloadClient.DownloadString(dataUrl);                

                var pricerows = pricedata.Split('\n');

                var firstRow = pricerows.First();

                var columnHeaders = firstRow.Split(',');
                var settleColumnIndex = FindColumnHeaderIndex(columnHeaders, "Settle");
                var openInterestColumnIndex = FindColumnHeaderIndex(columnHeaders, "Interest");
                var volumeColumnIndex = FindColumnHeaderIndex(columnHeaders, "Volume");

                _Connection.Execute("BEGIN");

                foreach (var row in pricerows.Skip(1))
                {
                    var cells = row.Split(',');

                    if (cells.Count() > 1)
                    {
                        if (cells[0] == string.Empty ||
                            cells[1] == string.Empty ||
                            cells[2] == string.Empty ||
                            cells[3] == string.Empty ||
                            cells[volumeColumnIndex] == string.Empty ||
                            cells[openInterestColumnIndex] == string.Empty ||
                            cells[settleColumnIndex] == string.Empty)
                        {
                            continue;
                        }

                        var date = DateTime.ParseExact(cells[0], "yyyy-MM-dd", null);
                        var open = TryGetValue(cells[1]);
                        var high = TryGetValue(cells[2]);
                        var low = TryGetValue(cells[3]);
                        var close = TryGetValue(cells[settleColumnIndex]);
                        var volume = int.Parse(cells[volumeColumnIndex], NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands);
                        var openInterest = int.Parse(cells[openInterestColumnIndex], NumberStyles.AllowDecimalPoint | NumberStyles.AllowThousands);

                        AddContractPrice(contractId, date, open, high, low, close, openInterest, volume);
                    }
                }

                _Connection.Execute("END");
            }
        }

        private void GenerateBackAdjustedContinuousContract(QuandlCommodity commodity)
        {
            var referenceContracts = GetReferenceContracts().ToList();
            var allPrices = GetCommodityPrices(commodity.Id);
            var adjustmentFactor = 0m;
            var adjustedClose = 0m;
            var adjustedOpen = 0m;
            var adjustedHigh = 0m;
            var adjustedLow = 0m;
            var curveDateCount = referenceContracts.Count;
            QuandlContractPrice referenceContractPriceData, previousContractPriceData;
            var contracts = GetAllContracts(commodity.Id);

            _Connection.Execute("DELETE FROM QuandlAdjustedPrice;");
            _Connection.Execute("BEGIN;");

            for (int i = 1; i < curveDateCount; i++)
            {
                var curveDate = referenceContracts[i];
                var yesterdayCurveDate = referenceContracts[i - 1];
                var contract = contracts.FirstOrDefault(c => c.Id == curveDate.ContractId);

                Console.WriteLine(string.Format("{0} {1}%", curveDate.Date.ToString("dd/MM/yyyy"), Math.Round(((decimal)i / (decimal)curveDateCount) * 100, 2)));

                referenceContractPriceData = allPrices.FirstOrDefault(p => p.Date == curveDate.Date && p.ContractId == curveDate.ContractId);                

                if (curveDate.ContractId != yesterdayCurveDate.ContractId)
                {
                    // SWITCH CONTRACT, ADJUST FACTOR
                    previousContractPriceData = allPrices.FirstOrDefault(p => p.Date == curveDate.Date && p.ContractId == yesterdayCurveDate.ContractId);

                    // RECALCULATE THE ADJUSTMENT FACTOR - THE GAP BETWEEN LATEST MOST LIQUID CONTRACT, AND THE PREVIOUS ONE
                    adjustmentFactor = adjustmentFactor + (previousContractPriceData.Close - referenceContractPriceData.Close);

                    adjustedClose = referenceContractPriceData.Close + adjustmentFactor;
                    adjustedOpen = referenceContractPriceData.Open + adjustmentFactor;
                    adjustedHigh = referenceContractPriceData.High + adjustmentFactor;
                    adjustedLow = referenceContractPriceData.Low + adjustmentFactor;
                }
                else
                {                                        
                    adjustedClose = referenceContractPriceData.Close + adjustmentFactor;
                    adjustedOpen = referenceContractPriceData.Open + adjustmentFactor;
                    adjustedHigh = referenceContractPriceData.High + adjustmentFactor;
                    adjustedLow = referenceContractPriceData.Low + adjustmentFactor;
                }

                AddAdjustedPrice(curveDate.Date, contract.Name, contract.Id, adjustmentFactor, adjustedOpen, adjustedHigh, adjustedLow, adjustedClose, referenceContractPriceData.OpenInterest);
            }

            _Connection.Execute("END;");
        }

        private int FindColumnHeaderIndex(string[] columnHeaders, string search)
        {
            for (int i = 0; i < columnHeaders.Length; i++)
            {
                if (columnHeaders[i].Contains(search)) return i;
            }

            return -1;
        }

        private void GenerateContractRoll(QuandlCommodity commodity)
        {
            var curveDates = GetCurveDatesAsc(commodity.Id).ToList();
            var currentDate = curveDates.First();
            var referenceContract = GetCurrentContract(currentDate, commodity.Id);
            var contracts = GetAllContracts(commodity.Id);
            var allPrices = GetCommodityPrices(commodity.Id);
            List<int> previousReferenceContractIds = new List<int>();
            int consecutiveDays = 0;

            var output = new StringBuilder();

            _Connection.Execute("BEGIN;");

            var curveDateCount = curveDates.Count;
            for (int i = 0; i < curveDateCount; i++)
            {
                var curveDate = curveDates[i];

                Console.WriteLine(string.Format("{0} {1}%", curveDate.ToString("dd/MM/yyyy"), Math.Round(((decimal)i / (decimal)curveDateCount) * 100, 2)));

                var todaysPrices = allPrices.Where(p => p.Date == curveDate && !previousReferenceContractIds.Any(x => x == p.ContractId));

                QuandlContractPrice liquidContractPriceData = todaysPrices.FirstOrDefault(c => c.OpenInterest == todaysPrices.Max(p => p.OpenInterest));

                // MAKE SURE THAT TODAY'S PRICES CONTAIN THE CURRENT CONTRACT
                if (!todaysPrices.Any(p => p.ContractId == referenceContract.Id))
                {
                    // WE ARE MISSING THE CURRENT CONTRACT PRICE DATA FOR THIS CURVE DATE
                    // AUTO SWITCH TO NEXT LIQUID CONTRACT
                    consecutiveDays = 0;

                    referenceContract = contracts.FirstOrDefault(c => c.Id == liquidContractPriceData.ContractId);

                    output.AppendLine(string.Format("{0},{1},{2},{3}", curveDate.ToString("dd/MM/yyyy"), referenceContract.Name, consecutiveDays, "AUTO"));

                    continue;
                }
                else
                {
                    if (liquidContractPriceData.ContractId == referenceContract.Id)
                    {
                        // SAME CONTRACT
                        consecutiveDays = 0;

                        output.AppendLine(string.Format("{0},{1},{2},{3}", curveDate.ToString("dd/MM/yyyy"), referenceContract.Name, string.Empty, string.Empty));
                    }
                    else
                    {
                        output.AppendLine(string.Format("{0},{1},{2},{3}", curveDate.ToString("dd/MM/yyyy"), referenceContract.Name, consecutiveDays, string.Empty));

                        // CHANGE OF REFERENCE CONTRACT
                        consecutiveDays++;

                        if (consecutiveDays == 2)
                        {
                            // FIND THE CONTRACT THAT MATCHES MOST LIQUID ONE
                            referenceContract = contracts.FirstOrDefault(c => c.Id == liquidContractPriceData.ContractId);
                        }

                        //previousReferenceContractIds.Add(referenceContract.Id);
                    }
                }

                AddDateReferenceContract(curveDate, referenceContract.Id);
            }

            _Connection.Execute("END;");

            File.WriteAllText("output.csv", output.ToString());
        }

        private void GenerateAdjustedCurve(QuandlCommodity commodity)
        {
            Console.WriteLine("Generating Adjusted Curve for " + commodity.Name + " ");

            var curveDates = GetCurveDates(commodity.Id).ToList();
            var currentDate = curveDates.First();
            var referenceContract = GetCurrentContract(currentDate, commodity.Id);            
            QuandlContractPrice referenceContractPriceData = null;
            var contracts = GetAllContracts(commodity.Id);
            var allPrices = GetCommodityPrices(commodity.Id);
            List<int> previousReferenceContractIds = new List<int>();         
            var adjustmentFactor = 0m;
            var adjustedClose = 0m;
            var adjustedOpen = 0m;
            var adjustedHigh = 0m;
            var adjustedLow = 0m;
            
            _Connection.Execute("BEGIN");

            var curveDateCount = curveDates.Count;
            for (int i = 0; i < curveDateCount; i++)
            {
                QuandlContractPrice liquidContractPriceData;
                var curveDate = curveDates[i];

                Console.WriteLine(string.Format("{0} {1}%", curveDate.ToString("dd/MM/yyyy"), Math.Round(((decimal)i / (decimal)curveDateCount) * 100, 2)));

                // GET PRICES FOR CURRENT CURVE DATE, BUT IGNORE THOSE FROM PREVIOUS REFERENCE CONTRACT SO THAT WE DONT SWITCH BACK
                var todaysPrices = allPrices.Where(p => p.Date == curveDate && !previousReferenceContractIds.Any(x => x == p.ContractId));

                // GET THE MOST LIQUID CONTRACT FOR TODAY
                liquidContractPriceData = todaysPrices.FirstOrDefault(c => c.OpenInterest == todaysPrices.Max(p => p.OpenInterest));                

                // MAKE SURE THAT TODAY'S PRICES CONTAIN THE CURRENT CONTRACT
                if (!todaysPrices.Any(p => p.ContractId == referenceContract.Id))
                {
                    // WE ARE MISSING THE CURRENT CONTRACT PRICE DATA FOR THIS CURVE DATE
                    continue;
                }
                
                // GET THE PRICE DATA FOR CURRENT CONTRACT FOR TODAY
                referenceContractPriceData = todaysPrices.FirstOrDefault(p => p.ContractId == referenceContract.Id);

                // IS THE CURRENT CONTRACT STILL THE MOST LIQUID ONE?
                if (liquidContractPriceData.ContractId == referenceContract.Id)
                {                                        
                    // YES SO USE CURRENT CONTRACT DATA
                    adjustedClose = referenceContractPriceData.Close + adjustmentFactor;
                    adjustedOpen = referenceContractPriceData.Open + adjustmentFactor;
                    adjustedHigh = referenceContractPriceData.High + adjustmentFactor;
                    adjustedLow = referenceContractPriceData.Low + adjustmentFactor;
                }
                else
                {
                    // FIND THE CONTRACT THAT MATCHES MOST LIQUID ONE
                    referenceContract = contracts.FirstOrDefault(c => c.Id == liquidContractPriceData.ContractId);

                    // RECALCULATE THE ADJUSTMENT FACTOR - THE GAP BETWEEN LATEST MOST LIQUID CONTRACT, AND THE PREVIOUS ONE
                    adjustmentFactor = adjustmentFactor + (referenceContractPriceData.Close - liquidContractPriceData.Close);

                    // APPLY ADJUSTMENT FACTOR TO MOST LIQUID PRICE I.E. NEW REFERENCE DATA FOR THE DAY
                    adjustedClose = liquidContractPriceData.Close + adjustmentFactor;
                    adjustedOpen = liquidContractPriceData.Open + adjustmentFactor;
                    adjustedHigh = liquidContractPriceData.High + adjustmentFactor;
                    adjustedLow = liquidContractPriceData.Low + adjustmentFactor;

                    // SAVE THE PREVIOUS REFERENCE CONTRACT TO MAKE SURE WE DONT GO BACK TO THAT ONE
                    //previousReferenceContractId = referenceContractPriceData.ContractId;
                    previousReferenceContractIds.Add(referenceContractPriceData.ContractId);
                }

                AddAdjustedPrice(curveDate, referenceContract.Name, referenceContract.Id, adjustmentFactor, adjustedOpen, adjustedHigh, adjustedLow, adjustedClose, liquidContractPriceData.OpenInterest);             
            }

            _Connection.Execute("END");

            Console.WriteLine("DONE");
        }

        private IEnumerable<string> GetSCFCodes()
        {
            return _Connection.Query<string>("SELECT Code FROM QuandlSCFAsset;");
        }

        private IEnumerable<QuandlDateReferenceContract> GetReferenceContracts()
        {
            return _Connection.Query<QuandlDateReferenceContract>("SELECT Date, ContractId FROM QuandlDateReferenceContract order by Date DESC;");
        }

        private void AddDateReferenceContract(DateTime date, int contractId)
        {
            _Connection.Execute("INSERT INTO QuandlDateReferenceContract (Date, ContractId) VALUES (@Date, @ContractId)", new { Date = date, ContractId = contractId });
        }

        private List<QuandlContract> GetAllContracts(int commodityId)
        {
            return _Connection.Query<QuandlContract>("SELECT Id, Name, MonthCode, Year, QuandlCommodityId FROM QuandlContract WHERE QuandlCommodityId = @CommodityId", new { CommodityId = commodityId }).ToList();
        }

        public IEnumerable<QuandlContractPrice> GetCommodityPrices(int commodityId)
        {
            return _Connection.Query<QuandlContractPrice>(@"
SELECT p.ROWID as Id, p.ContractId, p.Date, p.Open, p.High, p.Low, p.Close, p.OpenInterest, p.Volume, c.Name as ContractName 
FROM QuandlContractPrice p, QuandlContract c
WHERE c.Id = p.ContractId
and c.QuandlCommodityId = @CommodityId",
new { CommodityId = commodityId });
        }

        private QuandlContract GetCurrentContract(DateTime date, int commodityId)
        {
            return _Connection.Query<QuandlContract>(@"
select c.Id, c.Name, c.Year, c.MonthCode, c.QuandlCommodityId 
from QuandlContractPrice p, QuandlContract c 
where c.Id = p.ContractId 
and Date = @Date
and QuandlCommodityId = @CommodityId
and OpenInterest = (select Max(pr.OpenInterest) from QuandlContractPrice pr, QuandlContract cr where pr.Date = @Date and cr.QuandlCommodityId = @CommodityId and cr.Id = pr.ContractId);", new { Date = date, CommodityId = commodityId }).Single();
        }

        private IEnumerable<DateTime> GetCurveDates(int commodityId)
        {
            return _Connection.Query<DateTime>(@"
select distinct p.Date
from QuandlContractPrice p, QuandlContract c
where p.ContractId = c.Id
and c.QuandlCommodityId = @CommodityId
and p.OpenInterest > 0
order by p.Date desc
", new { CommodityId = commodityId });
        }

        private IEnumerable<DateTime> GetCurveDatesAsc(int commodityId)
        {
            return _Connection.Query<DateTime>(@"
select distinct p.Date
from QuandlContractPrice p, QuandlContract c
where p.ContractId = c.Id
and c.QuandlCommodityId = @CommodityId
and p.OpenInterest > 0
order by p.Date asc;
", new { CommodityId = commodityId });
        }

        private decimal TryGetValue(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return 0;
            }

            return decimal.Parse(value);
        }

        private void PurgeData()
        {
            _Connection.Execute(@"
DELETE FROM QuandlContract; 
DELETE FROM QuandlContractPrice;
DELETE FROM QuandlAdjustedPrice;
VACUUM;");
        }

        private List<QuandlCommodity> GetAllCommodities()
        {
            return _Connection.Query<QuandlCommodity>("SELECT Id, Name, Code, ContractListUrl, ContractListRegex, FirstContract FROM QuandlCommodity WHERE Active = 1;").ToList();
        }

        private int AddContract(string name, string monthCode, string year, int commodityId)
        {
            var id = _Connection.Query<int>(@"
INSERT INTO QuandlContract 
(QuandlCommodityId, Name, MonthCode, Year) 
VALUES (@QuandlCommodityId, @Name, @MonthCode, @Year); 
select last_insert_rowid()", new { Name = name, MonthCode = monthCode, Year = year, QuandlCommodityId = commodityId }).Single();

            return id;
        }

        private int AddContractPrice(int contractId, DateTime date, decimal open, decimal high, decimal low, decimal close, int openInterest, int volume)
        {
            var id = _Connection.Query<int>(@"
INSERT INTO QuandlContractPrice
(ContractId, Date, Open, High, Low, Close, OpenInterest, Volume) 
VALUES (@ContractId, @Date, @Open, @High, @Low, @Close, @OpenInterest, @Volume) ; 
select last_insert_rowid()",
new { ContractId = contractId, Date = date, Open = open, High = high, Low = low, Close = close, OpenInterest = openInterest, volume = volume }).Single();

            return id;
        }

        private void AddAdjustedPrice(DateTime date, string contractName, int contractId, decimal adjustmentFactor, decimal adjustedOpen, decimal adjustedHigh, decimal adjustedLow, decimal adjustedClose, int openInterest)
        {
            _Connection.Execute(@"
INSERT INTO QuandlAdjustedPrice
(Date, ContractName, ContractId, AdjustmentFactor, AdjustedOpen, AdjustedHigh, AdjustedLow, AdjustedClose, OpenInterest)
VALUES (@Date, @ContractName, @ContractId, @AdjustmentFactor, @AdjustedOpen, @AdjustedHigh, @AdjustedLow, @AdjustedClose, @OpenInterest)
", new { Date = date, ContractName = contractName, ContractId = contractId, AdjustedClose = adjustedClose, AdjustmentFactor = adjustmentFactor, OpenInterest = openInterest, AdjustedOpen = adjustedOpen, AdjustedHigh = adjustedHigh, AdjustedLow = adjustedLow });
        }
    }
}
