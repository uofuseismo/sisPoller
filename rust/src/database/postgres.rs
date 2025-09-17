use crate::datatypes::station_time::StationTime;

#[allow(dead_code)]
pub fn get_stations(connection_uri : &str, schema : &str) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   let mut stations : Vec<StationTime> = Vec::new();
   let mut client = postgres::Client::connect(connection_uri, postgres::NoTls)?;
   if !schema.is_empty() {
      let _ = client.execute("SET search_path TO production", &[])?;
   }
   for row in client.query("SELECT xml_file, EXTRACT(epoch FROM last_modified)::bigint AS last_modified FROM xml_update", &[])? {
      let station : &str = row.get(0);
      let time : i64 = row.get(1);
      let pair = StationTime {station: station.to_string(), time: time};
      log::debug!("Database station {} {}", station, time);
      stations.push(pair);
   }
   return Ok(stations);
}

#[allow(dead_code)]
pub fn create_stations(connection_uri : &str, 
                       schema : &str,
                       stations_to_create : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   let mut created_stations : Vec<StationTime> = Vec::new();
   if !stations_to_create.is_empty() {
       let mut client = postgres::Client::connect(connection_uri, postgres::NoTls)?;
       if !schema.is_empty() {
          let _ = client.execute("SET search_path TO production", &[])?;
       }
       for station in stations_to_create.iter() {
          let time : f64 = station.time as f64;
          let result = client.execute(
                       "INSERT INTO xml_update (xml_file, last_modified) VALUES($1, TO_TIMESTAMP($2))", 
                       &[&station.station, &time],
                       );  
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
      log::info!("Created {} out of {} stations in database", 
                 created_stations.len(), stations_to_create.len());
   }   
   return Ok(created_stations);
}

#[allow(dead_code)]
pub fn update_stations(connection_uri : &str,
                       schema : &str,
                       stations_to_update : &Vec<StationTime>) ->
   Result<Vec<StationTime>, Box<dyn std::error::Error>> {

   let mut updated_stations : Vec<StationTime> = Vec::new();
   if !stations_to_update.is_empty() {
       let mut client = postgres::Client::connect(connection_uri, postgres::NoTls)?;
       if !schema.is_empty() {
          let _ = client.execute("SET search_path TO production", &[])?;
       }
       for station in stations_to_update.iter() {
          let time : f64 = station.time as f64;
          let result = client.execute(
                       "UPDATE xml_update SET last_modified = TO_TIMESTAMP($1) WHERE xml_file = $2",
                       &[&time, &station.station],
                       );
          match result {
             Ok(result) => {
                log::debug!("Successful update -> updated {} row", &result);
                updated_stations.push(station.clone());
             }   
             Err(result) => {
                log::warn!("update failed -> {}", &result);
             }   
         }
      }   
      log::info!("Updated {} out of {} stations in database",
                 updated_stations.len(), stations_to_update.len());
   }   
   return Ok(updated_stations);
}

