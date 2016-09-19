using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement
{
    public class QuandlPriceHistoryDownloader
    {
        public void Run()
        {
            const string apikey = "MXrRoowvUAjrUCFEp63J";
            const string url = "https://www.quandl.com/api/v1/datasets/CME/{0}.csv?api_key=MXrRoowvUAjrUCFEp63J";
            
            var dataAccess = new DataAccess();

            var commodityList = dataAccess.GetCommodityList();

            // GENERATE CONTRACT LIST
            foreach (var commodity in commodityList)
            {
                commodity.Contracts = new List<Contract>();
                var monthCodes = commodity.MonthCodes.Split(',');

                var fromYear = int.Parse(commodity.FirstContract.Substring(1));
                
                for (int year = fromYear; year <= DateTime.Today.AddYears(1).Year; year++)
                {
                    foreach (var monthCode in monthCodes)
                    {
                        string contractName = string.Format("{0}{1}{2}", commodity.Code, monthCode, year);

                        var contract = new Contract
                        {
                            Commodity = commodity,
                            CommodityId = commodity.Id,
                            MonthCode = monthCode,
                            Name = contractName
                        };

                        Console.WriteLine(contractName);

                        commodity.Contracts.Add(contract);
                    }
                }

                var firstContract = commodity.Contracts.FirstOrDefault(c => c.Name.Contains(commodity.FirstContract));

                var indexOfFirstContract = commodity.Contracts.IndexOf(firstContract);

                commodity.Contracts = commodity.Contracts.Skip(indexOfFirstContract).ToList();
            }

            var downloadClient = new WebClient();

            foreach (var commodity in commodityList)
            {
                foreach (var contract in commodity.Contracts)
                {
                    string contractUrl = string.Format(url, contract.Name);

                    Console.WriteLine(contractUrl);

                    var pricedata = downloadClient.DownloadString(contractUrl);

                    var pricerows = pricedata.Split('\n').Skip(1);

                    foreach (var row in pricerows)
                    {
                        var cells = row.Split(',');

                        if (cells.Count() > 1)
                        {
                            var contractPrice = new ContractPrice
                            {
                                Contract = contract,
                                Date = DateTime.ParseExact(cells[0], "yyyy-MM-dd", null),
                                Open = decimal.Parse(cells[1]),
                                High = decimal.Parse(cells[2]),
                                Low = decimal.Parse(cells[3]),
                                Close = decimal.Parse(cells[6]),
                                OpenInterest = decimal.Parse(cells[8]),
                            };
                        }                        
                    }
                }
            }

            

            //const string CODE = "C";
            //string[] MONTHCODES = { "H", "K", "N", "U", "Z" };
            //string firstContract = "H1960";

            //string apikey = "MXrRoowvUAjrUCFEp63J";
            //var contracts = new List<string>();

            //var fromYear = 1960;
            //var toYear = 2016;

            //// GENERATE CONTRACT LIST
            //for (int year = fromYear; year < toYear; year++)
            //{
            //    foreach (var monthCode in MONTHCODES)
            //    {
            //        //https://www.quandl.com/api/v1/datasets/CME/CH2013.csv
            //        string contract = string.Format("{0}{1}{2}", CODE, monthCode, year);

            //        contracts.Add(contract);
            //        Console.WriteLine(contract);
            //    }
            //}


            //var downloadClient = new WebClient();
            //foreach (var contract in contracts)
            //{
            //    string contractUrl = string.Format(url, contract);

            //    downloadClient.DownloadFile(contractUrl, contract + ".csv");
            //}
        }
    }
}
