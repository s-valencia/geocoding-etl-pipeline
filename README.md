Geocoding & Reverse Geocoding ETL Pipeline

This project is a **Python-based ETL (Extract, Transform, Load) tool** for geocoding addresses or reverse-geocoding latitude/longitude coordinates in bulk.  
It automatically detects which mode to run based on the input CSV columns and outputs results in both **CSV** and **GeoJSON** formats.

---

Features

- **Dual Mode Detection**:  
  - **Forward Geocoding** (Address ➡ Latitude/Longitude)  
  - **Reverse Geocoding** (Latitude/Longitude ➡ Address)
- **Multiple Geocoders**:  
  Uses ArcGIS World Geocoding Service by default, with automatic fallback to OpenStreetMap Nominatim.
- **Rate Limiting & Retries**:  
  Avoids service blocking and gracefully retries failed requests.
- **Clean Outputs**:  
  - CSV with original data, matched address, coordinates, locator name, and raw payload JSON.  
  - GeoJSON for easy mapping in GIS tools.
- **Portfolio-Friendly**:  
  Works with small demo datasets without requiring an API key.

---
