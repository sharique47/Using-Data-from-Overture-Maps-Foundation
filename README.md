# Using-Data-from-Overture-Maps-Foundation
Using data from Overture gives real-time calculations, which is highly valuable for location-based services, marketing, and logistics analysis. 
#Distance Calculator Using Overture Data
##Overview
This project demonstrates an advanced geospatial data processing pipeline using  **pyspark** and the **Overture Maps Foundations** dataset. The goal of this project is to filter and analyze locations (eg:- Car Dealerships, Hotel chains, Restaurants, etc) based on a geographic area and compute the distance from a given point. The results are then written into a **PostgresSQL** database for further analysis.

### Key Achievements:
- **Integration with Overture Maps Foundation Data**: Overture provides high-quality, detailed geospatial data on places. This project leverages that dataset for real-world geographic analysis.
- **Efficient Geospatial Computations**: The project handles large geospatial datasets efficiently using Pyspark, making it scalable and suitable for big data.
- **Real-Time Distance Calculations**: Using the **H3** library, the distance between the dealership and a reference point is calculated in real time.
- **PostgresSQL Integration**: The results are stored in a structured manner, enabling easy access and further querying through PostgreSQL.

## Overture Data Usage

The Overture Maps Foundation provides open-source, highly detailed place data, ideal for location-based analysis, in this project. 
- Overture data is used to identify car dealerships by filtering places with the category 'car_dealer'.
- The bounding box geometry provided by Overture is used to accurately calculate the centroid (latitude and longitude) of each dealership location.

This dataset allows the project to perform accurate distance calculations and verify the presence of specific car brands (e.g. Toyota, Hyundai, etc.) at various locations.
