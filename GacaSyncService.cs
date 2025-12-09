using System;
using System.Data;
using System.Net;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace GACASYNC
{
    public interface IGacaSyncService
    {
        void RunOnce(CancellationToken cancellationToken = default);
        void SendEmail(string error);
    }

    public class GacaSyncService : IGacaSyncService
    {
        private readonly ILogger<GacaSyncService> _logger;
        private readonly GacaRepository _repository;
        private readonly IConfiguration _configuration;

        private static readonly System.Collections.Generic.HashSet<string> DomesticRoutes = new System.Collections.Generic.HashSet<string>
        {
            "RUH-DMM", "DMM-RUH", "RUH-JED", "JED-RUH", "RUH-GIZ", "GIZ-RUH", "RUH-AHB", "AHB-RUH",
            "RUH-MED", "MED-RUH", "DMM-JED", "JED-DMM", "YNB-DMM", "DMM-YNB", "RUH-TIF", "TIF-RUH",
            "TIF-DMM", "DMM-TIF", "MED-DMM", "DMM-MED", "AHB-JED", "JED-AHB", "JED-ELQ", "ELQ-JED",
            "JED-TUU", "TUU-JED", "AHB-DMM", "DMM-AHB", "RUH-ELQ", "ELQ-RUH", "RUH-TUU", "TUU-RUH",
            "JED-HAS", "HAS-JED", "RUH-HAS", "HAS-RUH"
        };

        public GacaSyncService(
            ILogger<GacaSyncService> logger,
            GacaRepository repository,
            IConfiguration configuration)
        {
            _logger = logger;
            _repository = repository;
            _configuration = configuration;
        }

        public void RunOnce(CancellationToken cancellationToken = default)
        {
            DateTime fromDate = DateTime.Today.AddDays(-1);
            DateTime toDate = DateTime.Today.AddDays(2);

            _logger.LogInformation("Starting Process_1 for range {From} to {To}", fromDate, toDate);

            // 1. Load AIMS Data
            DataTable importDataTable = _repository.RetrieveDataFromAIMS(fromDate, toDate);
            int aimsCount = importDataTable.Rows.Count;
            _logger.LogInformation("Loaded {Count} rows from AIMS", aimsCount);

            // 2. Load GACA Data Range
            DataTable gacaTable = _repository.RetrieveGacaRange(fromDate, toDate);
            int gacaCount = gacaTable.Rows.Count;
            var gacaDict = new System.Collections.Generic.Dictionary<(string, string, DateTime), DataRow>(gacaCount);

            foreach (DataRow r in gacaTable.Rows)
            {
                string fn = r["FlightNumber"]?.ToString() ?? "";
                string ff = r["FlightFrom"]?.ToString() ?? "";
                DateTime sd = r["SectorDate"] != DBNull.Value ? Convert.ToDateTime(r["SectorDate"]) : DateTime.MinValue;

                // Use a composite key
                var key = (fn, ff, sd);
                if (!gacaDict.ContainsKey(key))
                {
                    gacaDict[key] = r;
                }
            }
            _logger.LogInformation("Loaded {Count} GACA records into memory", gacaDict.Count);

            // 3. Load Crew Counts
            var crewCounts = _repository.RetrieveCrewCountsRange(fromDate, toDate);
            _logger.LogInformation("Loaded {Count} crew count entries", crewCounts.Count);

            // Initialize with capacity to avoid resizing
            var inserts = new System.Collections.Generic.List<GacaFlightModel>(aimsCount);
            var updates = new System.Collections.Generic.List<GacaFlightModel>(aimsCount);

            for (int i = 0; i < aimsCount; i++)
            {
                try
                {
                    DataRow row = importDataTable.Rows[i];

                    // --- 1. Extract and Cache Values ---

                    // DateTime optimizations
                    object depObj = row["DEP"];
                    object arrObj = row["Arr"];
                    DateTime depDt = depObj != DBNull.Value ? Convert.ToDateTime(depObj) : DateTime.MinValue;
                    DateTime arrDt = arrObj != DBNull.Value ? Convert.ToDateTime(arrObj) : DateTime.MinValue;

                    // Strings from DateTimes
                    string flightStartDate = depDt != DateTime.MinValue ? depObj.ToString() : "";
                    string flightEndDate = arrDt != DateTime.MinValue ? arrObj.ToString() : "";
                    string dayOfWeek = depDt != DateTime.MinValue ? depDt.DayOfWeek.ToString() : "";
                    DateTime secDate = depDt.Date; // Use Date part for sector date logic if needed, or just depDt

                    // Other columns
                    string actFrom = row["ActFrom"]?.ToString() ?? "";
                    string actTo = row["ActTo"]?.ToString() ?? "";
                    string flightCol = row["Flight"]?.ToString() ?? "";
                    string start = row["Start"]?.ToString() ?? "";
                    string stop = row["Stop"]?.ToString() ?? "";
                    string rego = row["Rego"]?.ToString() ?? "";
                    string make = row["Make"]?.ToString() ?? "";
                    string actualYClass = row["ActualYClass"]?.ToString() ?? "";
                    string actualInfant = row["ActualInfant"]?.ToString() ?? "";
                    string depDelayVal = row["DepDelay"]?.ToString() ?? "";
                    string arrDelayVal = row["ArrDelay"]?.ToString() ?? "";
                    string estimatedTimeVal = row["EstimatedTime"]?.ToString() ?? "";
                    string serviceTypeVal = row["ServiceType"]?.ToString() ?? "";
                    string nameVal = row["Name"]?.ToString() ?? "";
                    string commentVal = row["Comment"]?.ToString() ?? "";
                    DateTime sectorDate = row["SectorDate"] != DBNull.Value ? Convert.ToDateTime(row["SectorDate"]) : DateTime.MinValue;

                    // Derived values
                    string identifier = "Scheduled";
                    string flightDirection = (actFrom == "RUH" || actFrom == "DMM") ? "Departure" : "Arrival";
                    string internationalDomesticIndicator = IsDomestic(actFrom, actTo) ? "Domestic" : "International";
                    string airlineIATACode = "XY";
                    string airlineICAOCode = "KNE";

                    string flight = flightCol.Contains(".") ? "1" + flightCol.Remove(flightCol.Length - 1, 1) : flightCol;
                    string flightNumber = flight;
                    string flightSuffix = "F";
                    string flightFrom = actFrom.Trim();
                    string flightTo = actTo.Trim();
                    string flightType = "Passenger";
                    string stationIATACode = actTo.Trim();
                    string scheduledTimeDep = flightStartDate;
                    string scheduledTimeArr = flightEndDate;
                    string actTimeDep = start.Length != 0 ? start : "";
                    string actTimeArr = stop.Length != 0 ? stop : "";

                    string aircraftRegNo = rego.Replace("-", "").Replace("=", "").Replace(" ", "").Replace("_", "");
                    string fleetIdentifier = make;
                    string adultCount = actualYClass.Length != 0 ? actualYClass : "0";
                    string childCount = actualInfant.Length != 0 ? actualInfant : "0";

                    // Lookup Crew Count
                    string crewCount = "0";
                    var crewKey = (secDate, flightCol, actFrom);
                    if (crewCounts.TryGetValue(crewKey, out int cCount))
                    {
                        crewCount = cCount.ToString();
                    }

                    string legAirlineIATACode = "XY";
                    string legAirlineFlightNumber = "XY" + flightNumber;
                    string legScheduledTimeDep = flightStartDate;
                    string legScheduledTimeArr = flightEndDate;
                    string depDelay = depDelayVal.Length != 0 ? depDelayVal : "";
                    string arrDelay = arrDelayVal.Length != 0 ? arrDelayVal : "";
                    string estimatedTime = estimatedTimeVal;
                    string serviceType = serviceTypeVal;
                    string scheduleCode = nameVal.Length != 0 ? nameVal : "";
                    string operationComments = commentVal.Replace("'", "");

                    // --- 2. Compare or Create ---

                    var gacaKey = (flightNumber, flightFrom, sectorDate);
                    if (gacaDict.TryGetValue(gacaKey, out DataRow? sqlRow))
                    {
                        // Compare existing
                        string sqlFlightFrom = sqlRow["FlightFrom"]?.ToString()?.Trim() ?? "";
                        string sqlFlightTo = sqlRow["FlightTo"]?.ToString()?.Trim() ?? "";
                        string sqlFlightStartDate = sqlRow["FlightStartDate"]?.ToString() ?? "";
                        string sqlFlightEndDate = sqlRow["FlightEndDate"]?.ToString() ?? "";
                        string sqlDayOfWeek = sqlRow["DayOfWeek"]?.ToString() ?? "";
                        string sqlScheduledTimeDep = sqlRow["ScheduledTimeDep"]?.ToString() ?? "";
                        string sqlScheduledTimeArr = sqlRow["ScheduledTimeArr"]?.ToString() ?? "";
                        string sqlActTimeDep = sqlRow["ActTimeDep"]?.ToString() ?? "";
                        string sqlActTimeArr = sqlRow["ActTimeArr"]?.ToString() ?? "";
                        string sqlAircraftRegNo = sqlRow["AircraftRegNo"]?.ToString() ?? "";
                        string sqlFleetIdentifier = sqlRow["FleetIdentifier"]?.ToString() ?? "";
                        string sqlAdultCount = sqlRow["AdultCount"]?.ToString() ?? "";
                        string sqlChildCount = sqlRow["ChildCount"]?.ToString() ?? "";
                        string sqlCrewCount = sqlRow["CrewCount"]?.ToString() ?? "";
                        string sqlLegScheduledTimeDep = sqlRow["LegScheduledTimeDep"]?.ToString() ?? "";
                        string sqlLegScheduledTimeArr = sqlRow["LegScheduledTimeArr"]?.ToString() ?? "";
                        string sqlEstimatedTime = sqlRow["EstimatedTime"]?.ToString() ?? "";
                        string sqlDepDelay = sqlRow["DepDelay"]?.ToString() ?? "";
                        string sqlArrDelay = sqlRow["ArrDelay"]?.ToString() ?? "";
                        string sqlServiceType = sqlRow["ServiceType"]?.ToString() ?? "";
                        string sqlScheduleCode = sqlRow["ScheduleCode"]?.ToString() ?? "";
                        string sqlOperationComments = sqlRow["OperationComments"]?.ToString() ?? "";

                        bool isChanged =
                            flightFrom != sqlFlightFrom ||
                            flightTo != sqlFlightTo ||
                            flightStartDate != sqlFlightStartDate ||
                            flightEndDate != sqlFlightEndDate ||
                            dayOfWeek != sqlDayOfWeek ||
                            scheduledTimeDep != sqlScheduledTimeDep ||
                            scheduledTimeArr != sqlScheduledTimeArr ||
                            actTimeDep != sqlActTimeDep ||
                            actTimeArr != sqlActTimeArr ||
                            aircraftRegNo != sqlAircraftRegNo ||
                            fleetIdentifier != sqlFleetIdentifier ||
                            adultCount != sqlAdultCount ||
                            childCount != sqlChildCount ||
                            crewCount != sqlCrewCount ||
                            legScheduledTimeDep != sqlLegScheduledTimeDep ||
                            legScheduledTimeArr != sqlLegScheduledTimeArr ||
                            estimatedTime != sqlEstimatedTime ||
                            depDelay != sqlDepDelay ||
                            arrDelay != sqlArrDelay ||
                            serviceType != sqlServiceType ||
                            scheduleCode != sqlScheduleCode ||
                            operationComments != sqlOperationComments;

                        if (isChanged)
                        {
                            // Create Model ONLY if changed
                            var model = new GacaFlightModel
                            {
                                Identifier = identifier,
                                FlightDirection = flightDirection,
                                InternationalDomesticIndicator = internationalDomesticIndicator,
                                AirlineIATACode = airlineIATACode,
                                AirlineICAOCode = airlineICAOCode,
                                FlightNumber = flightNumber,
                                FlightSuffix = flightSuffix,
                                FlightFrom = flightFrom,
                                FlightTo = flightTo,
                                FlightStartDate = flightStartDate,
                                FlightEndDate = flightEndDate,
                                DayOfWeek = dayOfWeek,
                                FlightType = flightType,
                                StationIATACode = stationIATACode,
                                ScheduledTimeDep = scheduledTimeDep,
                                ScheduledTimeArr = scheduledTimeArr,
                                ActTimeDep = actTimeDep,
                                ActTimeArr = actTimeArr,
                                AircraftRegNo = aircraftRegNo,
                                FleetIdentifier = fleetIdentifier,
                                AdultCount = adultCount,
                                ChildCount = childCount,
                                CrewCount = crewCount,
                                LegAirlineIATACode = legAirlineIATACode,
                                LegAirlineFlightNumber = legAirlineFlightNumber,
                                LegScheduledTimeDep = legScheduledTimeDep,
                                LegScheduledTimeArr = legScheduledTimeArr,
                                DepDelay = depDelay,
                                ArrDelay = arrDelay,
                                EstimatedTime = estimatedTime,
                                ServiceType = serviceType,
                                ScheduleCode = scheduleCode,
                                OperationComments = operationComments,
                                SectorDate = sectorDate,
                                LastActionTime = DateTime.Now,
                                LastActionCode = "Update"
                            };
                            updates.Add(model);
                        }
                    }
                    else
                    {
                        // Insert - Create Model
                        var model = new GacaFlightModel
                        {
                            Identifier = identifier,
                            FlightDirection = flightDirection,
                            InternationalDomesticIndicator = internationalDomesticIndicator,
                            AirlineIATACode = airlineIATACode,
                            AirlineICAOCode = airlineICAOCode,
                            FlightNumber = flightNumber,
                            FlightSuffix = flightSuffix,
                            FlightFrom = flightFrom,
                            FlightTo = flightTo,
                            FlightStartDate = flightStartDate,
                            FlightEndDate = flightEndDate,
                            DayOfWeek = dayOfWeek,
                            FlightType = flightType,
                            StationIATACode = stationIATACode,
                            ScheduledTimeDep = scheduledTimeDep,
                            ScheduledTimeArr = scheduledTimeArr,
                            ActTimeDep = actTimeDep,
                            ActTimeArr = actTimeArr,
                            AircraftRegNo = aircraftRegNo,
                            FleetIdentifier = fleetIdentifier,
                            AdultCount = adultCount,
                            ChildCount = childCount,
                            CrewCount = crewCount,
                            LegAirlineIATACode = legAirlineIATACode,
                            LegAirlineFlightNumber = legAirlineFlightNumber,
                            LegScheduledTimeDep = legScheduledTimeDep,
                            LegScheduledTimeArr = legScheduledTimeArr,
                            DepDelay = depDelay,
                            ArrDelay = arrDelay,
                            EstimatedTime = estimatedTime,
                            ServiceType = serviceType,
                            ScheduleCode = scheduleCode,
                            OperationComments = operationComments,
                            SectorDate = sectorDate,
                            LastActionTime = DateTime.Now,
                            LastActionCode = "Add"
                        };
                        inserts.Add(model);
                    }
                }
                catch (Exception ex)
                {
                    SendEmail(ex.Message);
                    break;
                }
            }

            // Execute Bulk Operations
            if (inserts.Count > 0)
            {
                _repository.BulkInsertGaca(inserts);
            }
            if (updates.Count > 0)
            {
                _repository.BulkUpdateGaca(updates);
            }
        }

        private bool IsDomestic(string from, string to)
        {
            return DomesticRoutes.Contains($"{from}-{to}");
        }

        public void SendEmail(string error)
        {
            try
            {
                var smtpSettings = _configuration.GetSection("SmtpSettings");
                string host = smtpSettings["Host"] ?? "nsmtp.Riyadh.nasaviation.com";
                int port = int.Parse(smtpSettings["Port"] ?? "25");
                string from = smtpSettings["From"] ?? "no-reply@flynas.com";
                string to = smtpSettings["To"] ?? "dev-team@flynas.com";

                using (SmtpClient smtpClient = new SmtpClient(host, port))
                {
                    MailMessage mail = new MailMessage();
                    mail.From = new MailAddress(from);
                    mail.To.Add(to);
                    mail.Subject = "TAV Process - Error notification";
                    mail.Body = $"Error : {error}\nThis notification is for your information only.";

                    smtpClient.Send(mail);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to send email");
            }
        }
    }
}
