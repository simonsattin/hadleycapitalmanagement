using HadleyCapitalManagement.Quandl;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.DataAccess
{
    public class SqliteDataAccess : IDataAccess
    {
        private readonly SQLiteConnection _Connection;
        private readonly string _DatabaseLocation;

        public SqliteDataAccess()
        {
            _DatabaseLocation = ConfigurationManager.AppSettings["SqliteLocation"];
            var dbFilePath = _DatabaseLocation;
            if (!File.Exists(dbFilePath))
            {
                SQLiteConnection.CreateFile(dbFilePath);
            }
            _Connection = new SQLiteConnection(string.Format("Data Source={0};Version=3;datetimeformat=CurrentCulture", dbFilePath));
            _Connection.Open();
        }

        public void AddContract(QuandlContract contract)
        {
            throw new NotImplementedException();
        }

        public List<QuandlCommodity> GetCommodityList()
        {
            throw new NotImplementedException();
        }

        public List<QuandlContract> GetContractList(int commodityId)
        {
            throw new NotImplementedException();
        }

        public List<QuandlContractPrice> GetContractPriceList(int contractId)
        {
            throw new NotImplementedException();
        }
    }
}
