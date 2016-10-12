using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data.SqlClient;
using HadleyCapitalManagement.Quandl;

namespace HadleyCapitalManagement.DataAccess
{
    public class SqlServerDataAccess : IDataAccess
    {
        private string _ConnectionString;

        public SqlServerDataAccess()
        {
            _ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["LocalConnection"].ConnectionString;
        }

        public List<QuandlCommodity> GetCommodityList()
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<QuandlCommodity>("SELECT * FROM dbo.Commodity").ToList();
            }
        }

        public List<QuandlContract> GetContractList(int commodityId)
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<QuandlContract>("SELECT * FROM dbo.Contract WHERE CommodityId = @CommodityId", new { CommodityId = commodityId }).ToList();
            }
        }

        public List<QuandlContractPrice> GetContractPriceList(int contractId)
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<QuandlContractPrice>("SELECT * FROM dbo.ContractPrice WHERE ContractId = @ContractId", new { ContractId = contractId }).ToList();
            }
        }

        public void AddContract(QuandlContract contract)
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                string sql = @"
                    INSERT INTO dbo.Contract (Name, CommodityId, MonthCode) VALUES (@Name, @CommodityId, @MonthCode);
                    SELECT CAST(SCOPE_IDENTITY() as int)";

                var id = connection.Query<int>(sql, contract).Single();
                contract.Id = id;
            }
        }
    }
}
