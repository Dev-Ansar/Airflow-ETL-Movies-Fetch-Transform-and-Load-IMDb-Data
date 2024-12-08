## 🚀 Setup Instructions  

### 1. Clone the Repository  
```bash
git clone https://github.com/Dev-Ansar/Airflow-ETL-Movies-Fetch-Transform-and-Load-IMDb-Data.git
cd Airflow-ETL-Movies-Fetch-Transform-and-Load-IMDb-Data
``` 

### 2. Configure API Key 
 - Get your API key from the IMDb API via RapidAPI.
 - Open the .env file, replace the placeholder with your API key, and save it
 ```bash
RAPIDAPI_KEY="your_api_key_here"
``` 

### 3. Run Docker Compose
```bash
docker compose up
``` 

### 4. Connect to Postgres Database using pgadmin
 - Host Name/Address: Local IP (if running locally) or Docker IP (if pgAdmin is in a container).
 - Use PostgreSQL database details from the docker-compose file to connect to it via pgAdmin.
 - Create a new database named movies_db.
 - Refer to the screenshot below for guidance.
 ![alt text](<screenshots/pgadmin conn1.jpeg>)
 ![alt text](<screenshots/pgadmin connection.jpeg>)

### 5. Set Up a Connection in Airflow
 - Access the Airflow UI at http://localhost:8080.
 - Navigate to Admin > Connections and create a new connection.
 - Use the same details you used to connect to pgAdmin for setting up the connection in Airflow.
 - Refer to the screenshot below for guidance.
![alt text](<screenshots/airflow connection.jpeg>)



