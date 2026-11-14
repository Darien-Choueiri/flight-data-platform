# Schema

#### Link to source
https://zenodo.org/records/7923702

## Initial Schema

- **`callsign`** — `string`
  The identifier of the flight displayed on ATC screens (usually the first three letters are reserved for an airline: AFR for Air France, DLH for Lufthansa, etc.)

- **`number`** — `string`
  The commercial number of the flight, when available (the matching with the callsign comes from public open API); this field may not be very reliable.

- **`icao24`** — `string` (24-bit hex)
  The transponder unique identification number.

- **`registration`** — `string`
  The aircraft tail number (when available).

- **`typecode`** — `string`
  The aircraft model type (when available).

- **`origin`** — `string` (4-letter ICAO code)
  A four letter code for the origin airport of the flight (when available).

- **`destination`** — `string` (4-letter ICAO code)
  A four letter code for the destination airport of the flight (when available).

- **`firstseen`** — `timestamp` (UTC)
  The UTC timestamp of the first message received by the OpenSky Network.

- **`lastseen`** — `timestamp` (UTC)
  The UTC timestamp of the last message received by the OpenSky Network.

- **`day`** — `date` (UTC)
  The UTC day of the last message received by the OpenSky Network.

#### First Detected Position

- **`latitude_1`** — `double` (degrees)
  Latitude of the first detected position of the aircraft.

- **`longitude_1`** — `double` (degrees)
  Longitude of the first detected position of the aircraft.

- **`altitude_1`** — `double` (meters)
  Altitude of the first detected position of the aircraft.

#### Last Detected Position

- **`latitude_2`** — `double` (degrees)
  Latitude of the last detected position of the aircraft.

- **`longitude_2`** — `double` (degrees)
  Longitude of the last detected position of the aircraft.

- **`altitude_2`** — `double` (meters)
  Altitude of the last detected position of the aircraft.


## Database Schema

