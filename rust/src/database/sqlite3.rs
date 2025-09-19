use crate::datatypes::station_time::StationTime;

fn create_xml_table(sqlite3_file : &str ) {
   log::debug!("Checking if {} exists", sqlite3_file);
   let sqlite_database_exists = std::fs::exists(sqlite3_file).unwrap();
   if !sqlite_database_exists {
      log::info!("Creating sqlite3 database {}", sqlite3_file);
      let connection
         = rusqlite::Connection::open(sqlite3_file).unwrap();
      connection.execute("CREATE TABLE xml_update (xml_file TEXT, last_modified TEXT)", (), ).unwrap();
      let _ = connection.close();
   }
   else {
      log::debug!("sqlite3 database {} already exists", sqlite3_file);
   }
}

#[allow(dead_code)]
pub fn create_stations(sqlite3_file : &str,
                       stations_to_create : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   create_xml_table(sqlite3_file);
   let mut created_stations : Vec<StationTime> = Vec::new();
   if !stations_to_create.is_empty() {
      let connection 
         = rusqlite::Connection::open(sqlite3_file).unwrap();
      for station in stations_to_create.iter() {
         let time : i64 = station.time as i64;
         let result = connection.execute(
             "INSERT INTO xml_update (xml_file, last_modified) VALUES(?1, DATETIME(?2, 'unixepoch'))",
             (&station.station, &time), );
         match result {
            Ok(result) => {
               log::debug!("Successful insert -> created {} row", &result);
               created_stations.push(station.clone());
            }
            Err(result) => {
               log::warn!("Insert failed -> {}", &result);
            }
         }
      }
      let _ = connection.close();
      log::info!("Created {} out of {} stations in sqlite3 database", 
                 created_stations.len(), stations_to_create.len());
   }
   else {
      log::debug!("No stations to add to sqlite3");
   }
   return Ok(created_stations);
}

#[allow(dead_code)]
pub fn update_stations(sqlite3_file : &str,
                       stations_to_update : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   //assert!(std::fs::exists(sqlite3_file).unwrap()); // Has to exist
   let mut updated_stations : Vec<StationTime> = Vec::new();
   if !stations_to_update.is_empty() {
      let connection
         = rusqlite::Connection::open(sqlite3_file).unwrap();
      assert!(connection.is_autocommit());
      for station in stations_to_update.iter() {
         let time : i64 = station.time as i64;
         let result = connection.execute(
             "UPDATE xml_update SET last_modified = DATETIME(?1, 'unixepoch') WHERE xml_file = ?2",
             (&time, &station.station), );
         match result {
            Ok(result) => {
               log::info!("Successfully updated -> station {} to time {}", &result, time);
               updated_stations.push(station.clone());
            }
            Err(result) => {
               log::warn!("Insert failed -> {}", &result);
            }
         }
      }
      let _ = connection.close();
      log::info!("Updated {} out of {} stations in sqlite3 database",
                 updated_stations.len(), stations_to_update.len());
   }
   else {
      log::debug!("No stations to add to sqlite3");
   }
   return Ok(updated_stations);
}

#[allow(dead_code)]
pub fn get_stations(sqlite3_file : &str) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   // Make sure the sqlite3 file exists
   create_xml_table(sqlite3_file);
   //database::sqlite3::create_xml_table(sqlite3_file);
   assert!(std::fs::exists(sqlite3_file).unwrap());
   let mut stations : Vec<StationTime> = Vec::new();
   let connection = rusqlite::Connection::open(sqlite3_file).unwrap(); 
   let mut statement
       = connection.prepare("SELECT xml_file, unixepoch(last_modified) AS last_modified FROM xml_update")?;
   let station_iter = statement.query_map([], |row| {
      Ok(StationTime {
          station: row.get(0)?, 
          time: row.get(1)?,
        })  
   })?; 

   for s in station_iter {
      let station = s?; 
      log::debug!("Found station {:?} in sqlite3", station);
      stations.push(station.clone());
   }   
   return Ok(stations);
}

