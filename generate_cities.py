import csv
import math

cities = [
    ("New York", "NY", "East", 0.25, "Transco Zone 6", 40.71, -74.01),
    ("Chicago", "IL", "Midwest", 0.30, "Chicago Citygate", 41.88, -87.63),
    ("Boston", "MA", "NewEngland", 0.15, "Algonquin Citygate", 42.36, -71.06),
    ("Pittsburgh", "PA", "East", 0.10, "Texas Eastern", 40.44, -79.99),
    ("Seattle", "WA", "West", 0.10, "Sumas / Northwest", 47.61, -122.33),
    ("Houston", "TX", "South", 0.20, "Houston Ship Channel", 29.76, -95.37),
    ("Los Angeles", "CA", "West", 0.15, "SoCal Citygate", 34.05, -118.24),
    ("San Francisco", "CA", "West", 0.10, "PG&E Citygate", 37.77, -122.42),
    ("Washington", "DC", "East", 0.10, "Transco Zone 6", 38.91, -77.04),
    ("Philadelphia", "PA", "East", 0.10, "Transco Zone 6", 39.95, -75.17),
    ("Atlanta", "GA", "South", 0.10, "Transco Zone 4", 33.75, -84.39),
    ("Miami", "FL", "South", 0.05, "FGT Zone 3", 25.76, -80.19),
    ("Dallas", "TX", "South", 0.15, "Waha", 32.78, -96.80),
    ("Phoenix", "AZ", "West", 0.05, "El Paso South Mainline", 33.45, -112.07),
    ("Denver", "CO", "West", 0.05, "CIG", 39.74, -104.99),
    ("Detroit", "MI", "Midwest", 0.10, "MichCon", 42.33, -83.05),
    ("Minneapolis", "MN", "Midwest", 0.10, "Northern Natural", 44.98, -93.27),
    ("San Diego", "CA", "West", 0.05, "SoCal Citygate", 32.72, -117.16),
    ("Tampa", "FL", "South", 0.05, "FGT Zone 3", 27.95, -82.46),
    ("Baltimore", "MD", "East", 0.05, "Transco Zone 6", 39.29, -76.61),
    ("St. Louis", "MO", "Midwest", 0.05, "Panhandle", 38.63, -90.20),
    ("Charlotte", "NC", "South", 0.05, "Transco Zone 5", 35.23, -80.84),
    ("Orlando", "FL", "South", 0.05, "FGT Zone 3", 28.54, -81.38),
    ("San Antonio", "TX", "South", 0.05, "Houston Ship Channel", 29.42, -98.49),
    ("Portland", "OR", "West", 0.05, "Sumas / Northwest", 45.52, -122.68)
]

def round_to_quarter(val):
    return round(val * 4.0) / 4.0

rows = []
for city, state, region, weight, hub, clat, clon in cities:
    base_lat = round_to_quarter(clat)
    base_lon = round_to_quarter(clon)
    
    # Create a 3x3 grid around the base (except for coastal tuning)
    offsets = [-0.25, 0.0, 0.25]
    for dlat in offsets:
        for dlon in offsets:
            lat = base_lat + dlat
            lon = base_lon + dlon
            
            # Very basic coastal/water avoidance for a few notorious ones
            if city == "Chicago" and lon > -87.5: continue # Avoid Lake Michigan
            if city == "Seattle" and lon < -122.5: continue # Avoid Puget Sound
            if city == "New York" and lon > -73.7: continue # Atlantic ocean
            if city == "Boston" and lon > -71.0: continue # Massachusetts Bay
            if city == "Miami" and lon > -80.1: continue # Atlantic
            if city == "San Francisco" and lon < -122.5: continue # Pacific
            
            lat_i = int((lat + 90) * 1000)
            lon_i = int((lon + 180) * 1000)
            
            rows.append({
                'city_name': city,
                'state': state,
                'region': region,
                'market_weight': weight,
                'associated_hub': hub,
                'lat_i': lat_i,
                'lon_i': lon_i,
                'lat': lat,
                'lon': lon
            })

# Overwrite the seed CSV
with open('/home/airflow/dev/wx-alpha-pipeline/physical_meteor/seeds/ref_weather_station.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['city_name', 'state', 'region', 'market_weight', 'associated_hub', 'lat_i', 'lon_i', 'lat', 'lon'])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print(f"Generated {len(rows)} grid points for 25 cities!")
