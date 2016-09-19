using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HadleyCapitalManagement.DataAccess
{
    public class SqliteDataAccess : IDataAccess
    {
        private SQLiteConnection _Connection;

        public SqliteDataAccess()
        {
            var dbFilePath = "./Database.sqlite";
            if (!File.Exists(dbFilePath))
            {
                SQLiteConnection.CreateFile(dbFilePath);
            }
            _Connection = new SQLiteConnection(string.Format("Data Source={0};Version=3;datetimeformat=CurrentCulture", dbFilePath));
            _Connection.Open();
        }

        public void AddContract(Contract contract)
        {
            throw new NotImplementedException();
        }

        public List<Commodity> GetCommodityList()
        {
            throw new NotImplementedException();
        }

        public List<Contract> GetContractList(int commodityId)
        {
            throw new NotImplementedException();
        }

        public List<ContractPrice> GetContractPriceList(int contractId)
        {
            throw new NotImplementedException();
        }
    }
}
