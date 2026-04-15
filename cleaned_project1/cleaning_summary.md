# Data Cleaning Summary

This folder contains import-ready cleaned tables for the final 8-table schema.

## Files
- clean_region.csv
- clean_airline.csv
- clean_airport.csv
- clean_passenger.csv
- clean_route.csv
- clean_flight.csv
- clean_flight_cabin.csv
- clean_tickets_audit.csv
- region_name_mapping.csv
- added_airports.csv

## What was cleaned

### 1) Region standardization
Used only the regions actually referenced by airline / airport / tickets data.
Generated 36 active standard regions.

Manual region mappings applied:
- DRAGON -> Hong Kong (HK)
- Hong Kong SAR of China -> Hong Kong (HK)
- Republic of Korea -> South Korea (KR)

### 2) Airport supplementation
Added 3 airports referenced by tickets.csv but missing from airport.csv:
- JGS
- LLB
- YZY

Assigned new airport IDs:
- 205, 206, 207

Note:
- altitude is left NULL for LLB
- one original airport row still has missing iata_code, kept as-is

### 3) Passenger cleanup
Kept all 1000 rows.
mobile_number was preserved as text.
gender values were kept as Male / Female / Unknown.

### 4) Ticket normalization
Standardized:
- date -> YYYY-MM-DD
- departure_time -> HH:MM:SS
- arrival_time -> split into:
  - scheduled_arrival_time
  - arrival_day_offset

### 5) Ticket splitting
Generated final import-ready tables:
- clean_route.csv
- clean_flight.csv
- clean_flight_cabin.csv

## Row counts
- clean_region.csv: 36
- clean_airline.csv: 88
- clean_airport.csv: 207
- clean_passenger.csv: 1000
- clean_route.csv: 21764
- clean_flight.csv: 108820
- clean_flight_cabin.csv: 217640
- clean_tickets_audit.csv: 108820

## DDL reminders
1. airport.altitude should allow NULL
2. passenger.gender check should allow Unknown
