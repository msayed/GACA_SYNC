using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GACASYNC
{
    /// <summary>
    /// Repository for interacting with AIMS and GACA databases.
    /// </summary>
    public class GacaRepository
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<GacaRepository> _logger;
        private readonly string _aimsScript;
        private readonly string _aimsCancelScript;

        /// <summary>
        /// Initializes a new instance of the <see cref="GacaRepository"/> class.
        /// </summary>
        /// <param name="configuration">The configuration.</param>
        /// <param name="logger">The logger.</param>
        public GacaRepository(IConfiguration configuration, ILogger<GacaRepository> logger)
        {
            _configuration = configuration;
            _logger = logger;

            // 1) Cache SQL script files
            try
            {
                string scriptPath = Path.Combine(AppContext.BaseDirectory, "App_Data", "TavAutoProcScript.sql");
                _aimsScript = File.ReadAllText(scriptPath);

                string cancelScriptPath = Path.Combine(AppContext.BaseDirectory, "App_Data", "TavAutoProc_CancelScript.sql");
                _aimsCancelScript = File.ReadAllText(cancelScriptPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load SQL scripts in GacaRepository constructor.");
                throw;
            }
        }

        private string GetConnectionString(string name)
        {
            return _configuration.GetConnectionString(name);
        }

        #region AIMS Data Retrieval

        /// <summary>
        /// Retrieves flight data from AIMS for the specified date range.
        /// </summary>
        public DataTable RetrieveDataFromAIMS(DateTime fromDate, DateTime toDate)
        {
            DataTable tb = new DataTable();
            string connectionString = GetConnectionString("AimsConnection");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.CommandTimeout = 99000;
                    cmd.CommandText = _aimsScript;
                    cmd.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime) { Value = fromDate });
                    cmd.Parameters.Add(new SqlParameter("@ENDDate", SqlDbType.DateTime) { Value = toDate });
                    cmd.Connection = conn;

                    _logger.LogInformation("Executing AIMS import query from {FromDate} to {ToDate}", fromDate, toDate);
                    try
                    {
                        using (SqlDataAdapter db = new SqlDataAdapter(cmd))
                        {
                            db.Fill(tb);
                        }
                        _logger.LogInformation("AIMS import query returned {RowCount} rows", tb.Rows.Count);

                        // Clean up DBNulls in string columns
                        foreach (DataRow row in tb.Rows)
                        {
                            foreach (DataColumn col in tb.Columns)
                            {
                                if (col.DataType == typeof(string) && row[col] == DBNull.Value)
                                {
                                    row[col] = string.Empty;
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to execute AIMS import query from {FromDate} to {ToDate}", fromDate, toDate);
                        throw;
                    }
                }
            }
            return tb;
        }

        /// <summary>
        /// Retrieves cancelled flight data from AIMS for the specified date range.
        /// </summary>
        public DataTable RetrieveCancelDataFromAIMS(DateTime fromDate, DateTime toDate)
        {
            DataTable tb = new DataTable();
            string connectionString = GetConnectionString("AimsConnection");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.CommandText = _aimsCancelScript;
                    cmd.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime) { Value = fromDate });
                    cmd.Parameters.Add(new SqlParameter("@ENDDate", SqlDbType.DateTime) { Value = toDate });
                    cmd.Connection = conn;

                    _logger.LogInformation("Executing AIMS cancel query from {FromDate} to {ToDate}", fromDate, toDate);
                    try
                    {
                        using (SqlDataAdapter db = new SqlDataAdapter(cmd))
                        {
                            db.Fill(tb);
                        }
                        _logger.LogInformation("AIMS cancel query returned {RowCount} rows", tb.Rows.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to execute AIMS cancel query from {FromDate} to {ToDate}", fromDate, toDate);
                        throw;
                    }
                }
            }
            return tb;
        }

        /// <summary>
        /// Retrieves crew counts for a range of dates.
        /// </summary>
        /// <returns>A dictionary keyed by (SectorDate, FlightNumber, DepartureStation) containing the crew count.</returns>
        public Dictionary<(DateTime, string, string), int> RetrieveCrewCountsRange(DateTime fromDate, DateTime toDate)
        {
            var counts = new Dictionary<(DateTime, string, string), int>();
            string connectionString = GetConnectionString("AimsConnection");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                // 3) Optimize query formatting and use typed parameters
                string query = @"
                    SELECT 
                        [dbo].[fn_ToDate](cl.LEG_DAY) as SecDate, 
                        cl.LEG_FLT, 
                        cl.LEG_DEP, 
                        COUNT(*) as Cnt 
                    FROM ROSTER cl 
                    WHERE [dbo].[fn_ToDate](cl.LEG_DAY) >= @FromDate AND [dbo].[fn_ToDate](cl.LEG_DAY) <= @ToDate
                    GROUP BY [dbo].[fn_ToDate](cl.LEG_DAY), cl.LEG_FLT, cl.LEG_DEP";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandTimeout = 99000; // Set timeout to 3 minutes (180 seconds)
                    cmd.Parameters.Add(new SqlParameter("@FromDate", SqlDbType.DateTime) { Value = fromDate });
                    cmd.Parameters.Add(new SqlParameter("@ToDate", SqlDbType.DateTime) { Value = toDate });

                    try
                    {
                        conn.Open();
                        _logger.LogInformation("Executing bulk crew count query from {FromDate} to {ToDate}", fromDate, toDate);
                        using (SqlDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                DateTime date = reader["SecDate"] != DBNull.Value ? Convert.ToDateTime(reader["SecDate"]) : DateTime.MinValue;
                                string flt = reader["LEG_FLT"]?.ToString() ?? "";
                                string dep = reader["LEG_DEP"]?.ToString() ?? "";
                                int count = reader["Cnt"] != DBNull.Value ? Convert.ToInt32(reader["Cnt"]) : 0;

                                counts[(date, flt, dep)] = count;
                            }
                        }
                        _logger.LogInformation("Bulk crew count query returned {Count} entries", counts.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error retrieving bulk crew counts from {FromDate} to {ToDate}", fromDate, toDate);
                    }
                }
            }
            return counts;
        }

        #endregion

        #region GACA Data Retrieval

        /// <summary>
        /// Retrieves GACA flight data for a specific range.
        /// </summary>
        public DataTable RetrieveGacaRange(DateTime fromDate, DateTime toDate)
        {
            DataTable tb = new DataTable();
            string connectionString = GetConnectionString("GacaCS");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                // 2) Explicitly select only columns used by Worker.cs
                string query = @"
                    SELECT 
                        FlightNumber, FlightFrom, FlightTo, FlightStartDate, FlightEndDate,
                        DayOfWeek, ScheduledTimeDep, ScheduledTimeArr, ActTimeDep, ActTimeArr,
                        AircraftRegNo, FleetIdentifier, AdultCount, ChildCount, CrewCount,
                        LegScheduledTimeDep, LegScheduledTimeArr, EstimatedTime, DepDelay,
                        ArrDelay, ServiceType, ScheduleCode, OperationComments, SectorDate
                    FROM FlightTable 
                    WHERE SectorDate >= @FromDate AND SectorDate <= @ToDate";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandTimeout = 99000;
                    cmd.Parameters.Add(new SqlParameter("@FromDate", SqlDbType.DateTime) { Value = fromDate });
                    cmd.Parameters.Add(new SqlParameter("@ToDate", SqlDbType.DateTime) { Value = toDate });

                    _logger.LogInformation("Retrieving GACA data range from {FromDate} to {ToDate}", fromDate, toDate);
                    try
                    {
                        using (SqlDataAdapter db = new SqlDataAdapter(cmd))
                        {
                            db.Fill(tb);
                        }
                        _logger.LogInformation("GACA data range retrieval returned {RowCount} rows", tb.Rows.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to retrieve GACA data range from {FromDate} to {ToDate}", fromDate, toDate);
                        throw;
                    }
                }
            }
            return tb;
        }

        #endregion

        #region Bulk Operations

        /// <summary>
        /// Bulk inserts flight models into the FlightTable.
        /// </summary>
        public void BulkInsertGaca(List<GacaFlightModel> rows)
        {
            if (rows.Count == 0) return;

            string connectionString = GetConnectionString("GacaCS");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn))
                {
                    bulkCopy.DestinationTableName = "FlightTable";

                    // Map columns explicitly
                    ConfigureBulkCopyMappings(bulkCopy);

                    // 6) Use specialized overload
                    DataTable dt = ToDataTable(rows);
                    try
                    {
                        _logger.LogInformation("Bulk inserting {Count} rows into FlightTable", rows.Count);
                        bulkCopy.WriteToServer(dt);
                        _logger.LogInformation("Bulk insert completed");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Bulk insert failed");
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Bulk updates flight models in the FlightTable using a temp table and MERGE.
        /// </summary>
        public void BulkUpdateGaca(List<GacaFlightModel> rows)
        {
            if (rows.Count == 0) return;

            string connectionString = GetConnectionString("GacaCS");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                // 4) Wrap in transaction
                using (SqlTransaction tran = conn.BeginTransaction())
                {
                    try
                    {
                        // Create temp table
                        string createTempTable = @"
                            CREATE TABLE #FlightTableUpdates (
                                Identifier nvarchar(50), FlightDirection nvarchar(50), InternationalDomesticIndicator nvarchar(50),
                                AirlineIATACode nvarchar(50), AirlineICAOCode nvarchar(50), FlightNumber nvarchar(50),
                                FlightSuffix nvarchar(50), FlightFrom nvarchar(50), FlightTo nvarchar(50),
                                FlightStartDate nvarchar(50), FlightEndDate nvarchar(50), DayOfWeek nvarchar(50),
                                FlightType nvarchar(50), StationIATACode nvarchar(50), ScheduledTimeDep nvarchar(50),
                                ScheduledTimeArr nvarchar(50), ActTimeDep nvarchar(50), ActTimeArr nvarchar(50),
                                AircraftRegNo nvarchar(50), FleetIdentifier nvarchar(50), AdultCount nvarchar(50),
                                ChildCount nvarchar(50), CrewCount nvarchar(50), LegAirlineIATACode nvarchar(50),
                                LegAirlineFlightNumber nvarchar(50), LegScheduledTimeDep nvarchar(50), LegScheduledTimeArr nvarchar(50),
                                DepDelay nvarchar(50), ArrDelay nvarchar(50), EstimatedTime nvarchar(50),
                                ServiceType nvarchar(50), ScheduleCode nvarchar(50), OperationComments nvarchar(MAX),
                                SectorDate datetime, LastActionTime datetime, LastActionCode nvarchar(50)
                            )";

                        using (SqlCommand cmd = new SqlCommand(createTempTable, conn, tran))
                        {
                            cmd.ExecuteNonQuery();
                        }

                        // Bulk insert into temp table
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran))
                        {
                            bulkCopy.DestinationTableName = "#FlightTableUpdates";
                            ConfigureBulkCopyMappings(bulkCopy);

                            // 6) Use specialized overload
                            DataTable dt = ToDataTable(rows);
                            bulkCopy.WriteToServer(dt);
                        }

                        // Merge from temp table to main table
                        string mergeQuery = @"
                            UPDATE T
                            SET 
                                T.FlightStartDate = S.FlightStartDate,
                                T.FlightEndDate = S.FlightEndDate,
                                T.DayOfWeek = S.DayOfWeek,
                                T.ScheduledTimeDep = S.ScheduledTimeDep,
                                T.ScheduledTimeArr = S.ScheduledTimeArr,
                                T.ActTimeDep = S.ActTimeDep,
                                T.ActTimeArr = S.ActTimeArr,
                                T.AircraftRegNo = S.AircraftRegNo,
                                T.FleetIdentifier = S.FleetIdentifier,
                                T.AdultCount = S.AdultCount,
                                T.ChildCount = S.ChildCount,
                                T.CrewCount = S.CrewCount,
                                T.LegScheduledTimeDep = S.LegScheduledTimeDep,
                                T.LegScheduledTimeArr = S.LegScheduledTimeArr,
                                T.EstimatedTime = S.EstimatedTime,
                                T.DepDelay = S.DepDelay,
                                T.ArrDelay = S.ArrDelay,
                                T.ServiceType = S.ServiceType,
                                T.ScheduleCode = S.ScheduleCode,
                                T.OperationComments = S.OperationComments,
                                T.LastActionTime = S.LastActionTime,
                                T.LastActionCode = S.LastActionCode,
                                T.FlightTo = S.FlightTo
                            FROM FlightTable T
                            INNER JOIN #FlightTableUpdates S ON T.FlightNumber = S.FlightNumber AND T.SectorDate = S.SectorDate AND T.FlightFrom = S.FlightFrom";

                        using (SqlCommand cmd = new SqlCommand(mergeQuery, conn, tran))
                        {
                            cmd.CommandTimeout = 99000;
                            _logger.LogInformation("Executing bulk update merge for {Count} rows", rows.Count);
                            int affected = cmd.ExecuteNonQuery();
                            _logger.LogInformation("Bulk update affected {Affected} rows", affected);
                        }

                        tran.Commit();
                    }
                    catch (Exception ex)
                    {
                        tran.Rollback();
                        _logger.LogError(ex, "Bulk update transaction failed");
                        throw;
                    }
                }
            }
        }

        private void ConfigureBulkCopyMappings(SqlBulkCopy bulkCopy)
        {
            bulkCopy.ColumnMappings.Add("Identifier", "Identifier");
            bulkCopy.ColumnMappings.Add("FlightDirection", "FlightDirection");
            bulkCopy.ColumnMappings.Add("InternationalDomesticIndicator", "InternationalDomesticIndicator");
            bulkCopy.ColumnMappings.Add("AirlineIATACode", "AirlineIATACode");
            bulkCopy.ColumnMappings.Add("AirlineICAOCode", "AirlineICAOCode");
            bulkCopy.ColumnMappings.Add("FlightNumber", "FlightNumber");
            bulkCopy.ColumnMappings.Add("FlightSuffix", "FlightSuffix");
            bulkCopy.ColumnMappings.Add("FlightFrom", "FlightFrom");
            bulkCopy.ColumnMappings.Add("FlightTo", "FlightTo");
            bulkCopy.ColumnMappings.Add("FlightStartDate", "FlightStartDate");
            bulkCopy.ColumnMappings.Add("FlightEndDate", "FlightEndDate");
            bulkCopy.ColumnMappings.Add("DayOfWeek", "DayOfWeek");
            bulkCopy.ColumnMappings.Add("FlightType", "FlightType");
            bulkCopy.ColumnMappings.Add("StationIATACode", "StationIATACode");
            bulkCopy.ColumnMappings.Add("ScheduledTimeDep", "ScheduledTimeDep");
            bulkCopy.ColumnMappings.Add("ScheduledTimeArr", "ScheduledTimeArr");
            bulkCopy.ColumnMappings.Add("ActTimeDep", "ActTimeDep");
            bulkCopy.ColumnMappings.Add("ActTimeArr", "ActTimeArr");
            bulkCopy.ColumnMappings.Add("AircraftRegNo", "AircraftRegNo");
            bulkCopy.ColumnMappings.Add("FleetIdentifier", "FleetIdentifier");
            bulkCopy.ColumnMappings.Add("AdultCount", "AdultCount");
            bulkCopy.ColumnMappings.Add("ChildCount", "ChildCount");
            bulkCopy.ColumnMappings.Add("CrewCount", "CrewCount");
            bulkCopy.ColumnMappings.Add("LegAirlineIATACode", "LegAirlineIATACode");
            bulkCopy.ColumnMappings.Add("LegAirlineFlightNumber", "LegAirlineFlightNumber");
            bulkCopy.ColumnMappings.Add("LegScheduledTimeDep", "LegScheduledTimeDep");
            bulkCopy.ColumnMappings.Add("LegScheduledTimeArr", "LegScheduledTimeArr");
            bulkCopy.ColumnMappings.Add("DepDelay", "DepDelay");
            bulkCopy.ColumnMappings.Add("ArrDelay", "ArrDelay");
            bulkCopy.ColumnMappings.Add("EstimatedTime", "EstimatedTime");
            bulkCopy.ColumnMappings.Add("ServiceType", "ServiceType");
            bulkCopy.ColumnMappings.Add("ScheduleCode", "ScheduleCode");
            bulkCopy.ColumnMappings.Add("OperationComments", "OperationComments");
            bulkCopy.ColumnMappings.Add("SectorDate", "SectorDate");
            bulkCopy.ColumnMappings.Add("LastActionTime", "LastActionTime");
            bulkCopy.ColumnMappings.Add("LastActionCode", "LastActionCode");
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Specialized ToDataTable for GacaFlightModel to avoid reflection overhead.
        /// </summary>
        private DataTable ToDataTable(List<GacaFlightModel> items)
        {
            DataTable dataTable = new DataTable(typeof(GacaFlightModel).Name);

            // Manually add columns
            dataTable.Columns.Add("Identifier", typeof(string));
            dataTable.Columns.Add("FlightDirection", typeof(string));
            dataTable.Columns.Add("InternationalDomesticIndicator", typeof(string));
            dataTable.Columns.Add("AirlineIATACode", typeof(string));
            dataTable.Columns.Add("AirlineICAOCode", typeof(string));
            dataTable.Columns.Add("FlightNumber", typeof(string));
            dataTable.Columns.Add("FlightSuffix", typeof(string));
            dataTable.Columns.Add("FlightFrom", typeof(string));
            dataTable.Columns.Add("FlightTo", typeof(string));
            dataTable.Columns.Add("FlightStartDate", typeof(string));
            dataTable.Columns.Add("FlightEndDate", typeof(string));
            dataTable.Columns.Add("DayOfWeek", typeof(string));
            dataTable.Columns.Add("FlightType", typeof(string));
            dataTable.Columns.Add("StationIATACode", typeof(string));
            dataTable.Columns.Add("ScheduledTimeDep", typeof(string));
            dataTable.Columns.Add("ScheduledTimeArr", typeof(string));
            dataTable.Columns.Add("ActTimeDep", typeof(string));
            dataTable.Columns.Add("ActTimeArr", typeof(string));
            dataTable.Columns.Add("AircraftRegNo", typeof(string));
            dataTable.Columns.Add("FleetIdentifier", typeof(string));
            dataTable.Columns.Add("AdultCount", typeof(string));
            dataTable.Columns.Add("ChildCount", typeof(string));
            dataTable.Columns.Add("CrewCount", typeof(string));
            dataTable.Columns.Add("LegAirlineIATACode", typeof(string));
            dataTable.Columns.Add("LegAirlineFlightNumber", typeof(string));
            dataTable.Columns.Add("LegScheduledTimeDep", typeof(string));
            dataTable.Columns.Add("LegScheduledTimeArr", typeof(string));
            dataTable.Columns.Add("DepDelay", typeof(string));
            dataTable.Columns.Add("ArrDelay", typeof(string));
            dataTable.Columns.Add("EstimatedTime", typeof(string));
            dataTable.Columns.Add("ServiceType", typeof(string));
            dataTable.Columns.Add("ScheduleCode", typeof(string));
            dataTable.Columns.Add("OperationComments", typeof(string));
            dataTable.Columns.Add("SectorDate", typeof(DateTime));
            dataTable.Columns.Add("LastActionTime", typeof(DateTime));
            dataTable.Columns.Add("LastActionCode", typeof(string));

            foreach (var item in items)
            {
                // Manually add rows
                dataTable.Rows.Add(
                    item.Identifier,
                    item.FlightDirection,
                    item.InternationalDomesticIndicator,
                    item.AirlineIATACode,
                    item.AirlineICAOCode,
                    item.FlightNumber,
                    item.FlightSuffix,
                    item.FlightFrom,
                    item.FlightTo,
                    item.FlightStartDate,
                    item.FlightEndDate,
                    item.DayOfWeek,
                    item.FlightType,
                    item.StationIATACode,
                    item.ScheduledTimeDep,
                    item.ScheduledTimeArr,
                    item.ActTimeDep,
                    item.ActTimeArr,
                    item.AircraftRegNo,
                    item.FleetIdentifier,
                    item.AdultCount,
                    item.ChildCount,
                    item.CrewCount,
                    item.LegAirlineIATACode,
                    item.LegAirlineFlightNumber,
                    item.LegScheduledTimeDep,
                    item.LegScheduledTimeArr,
                    item.DepDelay,
                    item.ArrDelay,
                    item.EstimatedTime,
                    item.ServiceType,
                    item.ScheduleCode,
                    item.OperationComments,
                    item.SectorDate,
                    item.LastActionTime,
                    item.LastActionCode
                );
            }

            return dataTable;
        }

        /// <summary>
        /// Generic ToDataTable using reflection (fallback).
        /// </summary>
        private DataTable ToDataTable<T>(List<T> items)
        {
            DataTable dataTable = new DataTable(typeof(T).Name);
            PropertyInfo[] Props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (PropertyInfo prop in Props)
            {
                dataTable.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            }
            foreach (T item in items)
            {
                var values = new object[Props.Length];
                for (int i = 0; i < Props.Length; i++)
                {
                    values[i] = Props[i].GetValue(item, null);
                }
                dataTable.Rows.Add(values);
            }
            return dataTable;
        }

        #endregion

        #region Legacy Methods (Obsolete)

        [Obsolete("Use RetrieveCrewCountsRange instead")]
        public string RetrieveCountFromAIMS(DateTime secDate, string flightNumber, string dep)
        {
            string count = "0";
            string connectionString = GetConnectionString("AimsConnection");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = "select COUNT(*) from ROSTER cl where [dbo].[fn_ToDate](cl.LEG_DAY) = @SecDate and cl.LEG_FLT = @FlightNumber and cl.LEG_DEP = @Dep";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@SecDate", secDate);
                    cmd.Parameters.AddWithValue("@FlightNumber", flightNumber);
                    cmd.Parameters.AddWithValue("@Dep", dep);

                    try
                    {
                        conn.Open();
                        _logger.LogDebug("Executing crew count query for flight {FlightNumber} dep {Dep} on {SecDate}", flightNumber, dep, secDate);
                        object result = cmd.ExecuteScalar();
                        if (result != null)
                        {
                            count = result.ToString();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error retrieving crew count for flight {FlightNumber} dep {Dep} on {SecDate}", flightNumber, dep, secDate);
                    }
                }
            }
            return count;
        }

        [Obsolete("Use RetrieveGacaRange instead")]
        public DataTable RetrieveDataFromGaca(string flightNumber, string flightFrom, DateTime sectorDate)
        {
            DataTable tb = new DataTable();
            string connectionString = GetConnectionString("GacaCS");

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = "SELECT * from FlightTable where (FlightNumber = @FlightNumber and SectorDate = @SectorDate and FlightFrom = @FlightFrom)";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandTimeout = 99000;
                    cmd.Parameters.AddWithValue("@FlightNumber", flightNumber);
                    cmd.Parameters.AddWithValue("@SectorDate", sectorDate);
                    cmd.Parameters.AddWithValue("@FlightFrom", flightFrom);

                    _logger.LogInformation("Retrieving GACA data for flight {FlightNumber} from {FlightFrom} sector date {SectorDate}", flightNumber, flightFrom, sectorDate);
                    try
                    {
                        using (SqlDataAdapter db = new SqlDataAdapter(cmd))
                        {
                            db.Fill(tb);
                        }
                        _logger.LogInformation("GACA data retrieval returned {RowCount} rows", tb.Rows.Count);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to retrieve GACA data for flight {FlightNumber} from {FlightFrom} sector date {SectorDate}", flightNumber, flightFrom, sectorDate);
                        throw;
                    }
                }
            }
            return tb;
        }

        [Obsolete("Use BulkUpdateGaca instead")]
        public bool UpdateGaca(string flightStartDate, string flightEndDate, string dayOfWeek,
            string scheduledTimeDep, string scheduledTimeArr, string actTimeDep,
            string actTimeArr, string aircraftRegNo, string fleetIdentifier,
            string adultCount, string childCount, string crewCount,
            string legScheduledTimeDep, string legScheduledTimeArr, string estimatedTime,
            string depDelay, string arrDelay, string serviceType, string scheduleCode,
            string operationComments, string lastActionCode, string flightNumber, DateTime sectorDate, string flightFrom, string flightTo)
        {
            string connectionString = GetConnectionString("GacaCS");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = @"Update FlightTable set FlightStartDate = @FlightStartDate, FlightEndDate = @FlightEndDate, 
                                DayOfWeek = @DayOfWeek, ScheduledTimeDep = @ScheduledTimeDep, ScheduledTimeArr = @ScheduledTimeArr, 
                                ActTimeDep = @ActTimeDep, ActTimeArr = @ActTimeArr, AircraftRegNo = @AircraftRegNo, 
                                FleetIdentifier = @FleetIdentifier, AdultCount = @AdultCount, ChildCount = @ChildCount, 
                                CrewCount = @CrewCount, LegScheduledTimeDep = @LegScheduledTimeDep, LegScheduledTimeArr = @LegScheduledTimeArr, 
                                EstimatedTime = @EstimatedTime, DepDelay = @DepDelay, ArrDelay = @ArrDelay, ServiceType = @ServiceType, 
                                ScheduleCode = @ScheduleCode, OperationComments = @OperationComments, LastActionTime = @LastActionTime, 
                                LastActionCode = @LastActionCode, FlightTo = @FlightTo 
                                where (FlightNumber = @FlightNumber and SectorDate = @SectorDate and FlightFrom = @FlightFrom)";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.CommandTimeout = 99000;

                    // Add parameters
                    cmd.Parameters.AddWithValue("@FlightStartDate", flightStartDate);
                    cmd.Parameters.AddWithValue("@FlightEndDate", flightEndDate);
                    cmd.Parameters.AddWithValue("@DayOfWeek", dayOfWeek);
                    cmd.Parameters.AddWithValue("@ScheduledTimeDep", scheduledTimeDep);
                    cmd.Parameters.AddWithValue("@ScheduledTimeArr", scheduledTimeArr);
                    cmd.Parameters.AddWithValue("@ActTimeDep", actTimeDep);
                    cmd.Parameters.AddWithValue("@ActTimeArr", actTimeArr);
                    cmd.Parameters.AddWithValue("@AircraftRegNo", aircraftRegNo);
                    cmd.Parameters.AddWithValue("@FleetIdentifier", fleetIdentifier);
                    cmd.Parameters.AddWithValue("@AdultCount", adultCount);
                    cmd.Parameters.AddWithValue("@ChildCount", childCount);
                    cmd.Parameters.AddWithValue("@CrewCount", crewCount);
                    cmd.Parameters.AddWithValue("@LegScheduledTimeDep", legScheduledTimeDep);
                    cmd.Parameters.AddWithValue("@LegScheduledTimeArr", legScheduledTimeArr);
                    cmd.Parameters.AddWithValue("@EstimatedTime", estimatedTime);
                    cmd.Parameters.AddWithValue("@DepDelay", depDelay);
                    cmd.Parameters.AddWithValue("@ArrDelay", arrDelay);
                    cmd.Parameters.AddWithValue("@ServiceType", serviceType);
                    cmd.Parameters.AddWithValue("@ScheduleCode", scheduleCode);
                    cmd.Parameters.AddWithValue("@OperationComments", operationComments);
                    cmd.Parameters.AddWithValue("@LastActionTime", DateTime.Now);
                    cmd.Parameters.AddWithValue("@LastActionCode", lastActionCode);
                    cmd.Parameters.AddWithValue("@FlightTo", flightTo);
                    cmd.Parameters.AddWithValue("@FlightNumber", flightNumber);
                    cmd.Parameters.AddWithValue("@SectorDate", sectorDate);
                    cmd.Parameters.AddWithValue("@FlightFrom", flightFrom);

                    try
                    {
                        _logger.LogInformation("Updating GACA record for flight {FlightNumber} from {FlightFrom} to {FlightTo} on {SectorDate}", flightNumber, flightFrom, flightTo, sectorDate);
                        conn.Open();
                        cmd.ExecuteNonQuery();
                        _logger.LogInformation("Update succeeded for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Update failed for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return false;
                    }
                }
            }
        }

        [Obsolete("Use BulkInsertGaca instead")]
        public bool InsertGaca(string identifier, string flightDirection, string internationalDomesticIndicator,
            string airlineIATACode, string airlineICAOCode, string flightNumber,
            string flightSuffix, string flightFrom, string flightTo,
            string flightStartDate, string flightEndDate, string dayOfWeek, string flightType,
            string stationIATACode, string scheduledTimeDep, string scheduledTimeArr,
            string actTimeDep, string actTimeArr, string aircraftRegNo, string fleetIdentifier, string adultCount,
            string childCount, string crewCount, string legAirlineIATACode, string legAirlineFlightNumber,
            string legScheduledTimeDep, string legScheduledTimeArr, string depDelay, string arrDelay, string estimatedTime,
            string serviceType, string scheduleCode, string operationComments, DateTime sectorDate, string lastActionCode)
        {
            string connectionString = GetConnectionString("GacaCS");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = @"INSERT INTO FlightTable(Identifier, FlightDirection, InternationalDomesticIndicator, AirlineIATACode, AirlineICAOCode, 
                                FlightNumber, FlightSuffix, FlightFrom, FlightTo, FlightStartDate, FlightEndDate, DayOfWeek, FlightType, 
                                StationIATACode, ScheduledTimeDep, ScheduledTimeArr, ActTimeDep, ActTimeArr, AircraftRegNo, FleetIdentifier, 
                                AdultCount, ChildCount, CrewCount, LegAirlineIATACode, LegAirlineFlightNumber, LegScheduledTimeDep, 
                                LegScheduledTimeArr, DepDelay, ArrDelay, EstimatedTime, ServiceType, ScheduleCode, OperationComments, 
                                SectorDate, LastActionTime, LastActionCode) 
                                Values (@Identifier, @FlightDirection, @InternationalDomesticIndicator, @AirlineIATACode, @AirlineICAOCode, 
                                @FlightNumber, @FlightSuffix, @FlightFrom, @FlightTo, @FlightStartDate, @FlightEndDate, @DayOfWeek, @FlightType, 
                                @StationIATACode, @ScheduledTimeDep, @ScheduledTimeArr, @ActTimeDep, @ActTimeArr, @AircraftRegNo, @FleetIdentifier, 
                                @AdultCount, @ChildCount, @CrewCount, @LegAirlineIATACode, @LegAirlineFlightNumber, @LegScheduledTimeDep, 
                                @LegScheduledTimeArr, @DepDelay, @ArrDelay, @EstimatedTime, @ServiceType, @ScheduleCode, @OperationComments, 
                                @SectorDate, @LastActionTime, @LastActionCode)";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@Identifier", identifier);
                    cmd.Parameters.AddWithValue("@FlightDirection", flightDirection);
                    cmd.Parameters.AddWithValue("@InternationalDomesticIndicator", internationalDomesticIndicator);
                    cmd.Parameters.AddWithValue("@AirlineIATACode", airlineIATACode);
                    cmd.Parameters.AddWithValue("@AirlineICAOCode", airlineICAOCode);
                    cmd.Parameters.AddWithValue("@FlightNumber", flightNumber);
                    cmd.Parameters.AddWithValue("@FlightSuffix", flightSuffix);
                    cmd.Parameters.AddWithValue("@FlightFrom", flightFrom);
                    cmd.Parameters.AddWithValue("@FlightTo", flightTo);
                    cmd.Parameters.AddWithValue("@FlightStartDate", flightStartDate);
                    cmd.Parameters.AddWithValue("@FlightEndDate", flightEndDate);
                    cmd.Parameters.AddWithValue("@DayOfWeek", dayOfWeek);
                    cmd.Parameters.AddWithValue("@FlightType", flightType);
                    cmd.Parameters.AddWithValue("@StationIATACode", stationIATACode);
                    cmd.Parameters.AddWithValue("@ScheduledTimeDep", scheduledTimeDep);
                    cmd.Parameters.AddWithValue("@ScheduledTimeArr", scheduledTimeArr);
                    cmd.Parameters.AddWithValue("@ActTimeDep", actTimeDep);
                    cmd.Parameters.AddWithValue("@ActTimeArr", actTimeArr);
                    cmd.Parameters.AddWithValue("@AircraftRegNo", aircraftRegNo);
                    cmd.Parameters.AddWithValue("@FleetIdentifier", fleetIdentifier);
                    cmd.Parameters.AddWithValue("@AdultCount", adultCount);
                    cmd.Parameters.AddWithValue("@ChildCount", childCount);
                    cmd.Parameters.AddWithValue("@CrewCount", crewCount);
                    cmd.Parameters.AddWithValue("@LegAirlineIATACode", legAirlineIATACode);
                    cmd.Parameters.AddWithValue("@LegAirlineFlightNumber", legAirlineFlightNumber);
                    cmd.Parameters.AddWithValue("@LegScheduledTimeDep", legScheduledTimeDep);
                    cmd.Parameters.AddWithValue("@LegScheduledTimeArr", legScheduledTimeArr);
                    cmd.Parameters.AddWithValue("@DepDelay", depDelay);
                    cmd.Parameters.AddWithValue("@ArrDelay", arrDelay);
                    cmd.Parameters.AddWithValue("@EstimatedTime", estimatedTime);
                    cmd.Parameters.AddWithValue("@ServiceType", serviceType);
                    cmd.Parameters.AddWithValue("@ScheduleCode", scheduleCode);
                    cmd.Parameters.AddWithValue("@OperationComments", operationComments);
                    cmd.Parameters.AddWithValue("@SectorDate", sectorDate);
                    cmd.Parameters.AddWithValue("@LastActionTime", DateTime.Now);
                    cmd.Parameters.AddWithValue("@LastActionCode", lastActionCode);

                    try
                    {
                        _logger.LogInformation("Inserting GACA record for flight {FlightNumber} from {FlightFrom} to {FlightTo} sector {SectorDate}", flightNumber, flightFrom, flightTo, sectorDate);
                        conn.Open();
                        cmd.ExecuteNonQuery();
                        _logger.LogInformation("Insert succeeded for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Insert failed for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return false;
                    }
                }
            }
        }

        [Obsolete("Use bulk delete if needed, or manual deletion")]
        public bool DeleteGaca(string flightNumber, DateTime sectorDate, string flightFrom, string flightTo)
        {
            string connectionString = GetConnectionString("GacaCS");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                string query = "delete from FlightTable where FlightNumber = @FlightNumber and SectorDate = @SectorDate and FlightFrom = @FlightFrom and FlightTo = @FlightTo";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@FlightNumber", flightNumber);
                    cmd.Parameters.AddWithValue("@SectorDate", sectorDate);
                    cmd.Parameters.AddWithValue("@FlightFrom", flightFrom);
                    cmd.Parameters.AddWithValue("@FlightTo", flightTo);

                    try
                    {
                        _logger.LogInformation("Deleting GACA record for flight {FlightNumber} from {FlightFrom} to {FlightTo} sector {SectorDate}", flightNumber, flightFrom, flightTo, sectorDate);
                        conn.Open();
                        cmd.ExecuteNonQuery();
                        _logger.LogInformation("Delete succeeded for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Delete failed for flight {FlightNumber} sector {SectorDate}", flightNumber, sectorDate);
                        return false;
                    }
                }
            }
        }

        #endregion
    }
}
