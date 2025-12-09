using System;

namespace GACASYNC
{
    public class GacaFlightModel
    {
        public string? Identifier { get; set; }
        public string? FlightDirection { get; set; }
        public string? InternationalDomesticIndicator { get; set; }
        public string? AirlineIATACode { get; set; }
        public string? AirlineICAOCode { get; set; }
        public string? FlightNumber { get; set; }
        public string? FlightSuffix { get; set; }
        public string? FlightFrom { get; set; }
        public string? FlightTo { get; set; }
        public string? FlightStartDate { get; set; }
        public string? FlightEndDate { get; set; }
        public string? DayOfWeek { get; set; }
        public string? FlightType { get; set; }
        public string? StationIATACode { get; set; }
        public string? ScheduledTimeDep { get; set; }
        public string? ScheduledTimeArr { get; set; }
        public string? ActTimeDep { get; set; }
        public string? ActTimeArr { get; set; }
        public string? AircraftRegNo { get; set; }
        public string? FleetIdentifier { get; set; }
        public string? AdultCount { get; set; }
        public string? ChildCount { get; set; }
        public string? CrewCount { get; set; }
        public string? LegAirlineIATACode { get; set; }
        public string? LegAirlineFlightNumber { get; set; }
        public string? LegScheduledTimeDep { get; set; }
        public string? LegScheduledTimeArr { get; set; }
        public string? DepDelay { get; set; }
        public string? ArrDelay { get; set; }
        public string? EstimatedTime { get; set; }
        public string? ServiceType { get; set; }
        public string? ScheduleCode { get; set; }
        public string? OperationComments { get; set; }
        public DateTime SectorDate { get; set; }
        public DateTime LastActionTime { get; set; }
        public string? LastActionCode { get; set; }
    }
}
