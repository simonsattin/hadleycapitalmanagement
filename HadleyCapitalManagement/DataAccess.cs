using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data.SqlClient;

namespace HadleyCapitalManagement
{
    public class DataAccess
    {
        private string _ConnectionString;

        public DataAccess()
        {
            _ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["LocalConnection"].ConnectionString;
        }

        public List<Commodity> GetCommodityList()
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<Commodity>("SELECT * FROM dbo.Commodity").ToList();
            }
        }

        public List<Contract> GetContractList(int commodityId)
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<Contract>("SELECT * FROM dbo.Contract WHERE CommodityId = @CommodityId", new { CommodityId = commodityId }).ToList();
            }
        }

        public List<ContractPrice> GetContractPriceList(int contractId)
        {
            using (var connection = new SqlConnection(_ConnectionString))
            {
                return connection.Query<ContractPrice>("SELECT * FROM dbo.ContractPrice WHERE ContractId = @ContractId", new { ContractId = contractId }).ToList();
            }
        }

        public void AddContract(Contract contract)
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
