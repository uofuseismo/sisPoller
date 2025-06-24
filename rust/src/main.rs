#[derive(Clone)]
struct StationTime {
   station : String,
   time : i64,
}

fn get_stations_from_pg(connection_uri : &str, schema : &str) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
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

fn get_page(uri : &str) -> Result<String, Box<dyn std::error::Error>> {
   let response = reqwest::blocking::get(uri)?;
   // If I got a 200 code then return a win
   if response.status() == 200 {
      log::info!("Successfully hit URL");
      let document_text = response.text()?.clone();
      return Ok(document_text);
   }
   return Err("No text to return".into());
} 

fn parse_string(timestamp : &str) -> i64 {
   use chrono::{TimeZone, Utc};
   let (year, month, day, hour, minute) = scan_fmt::scan_fmt!(timestamp, "{d}-{d}-{d} {d}:{d}", i32, u32, u32, u32, u32).unwrap();
   let second : u32 = 0;
   //println!("{} {} {} {} {} {}", year, month, day, hour, minute, second);
   let d : chrono::DateTime<chrono::Utc>
        = Utc.with_ymd_and_hms(year, month, day, hour, minute, second).unwrap();
   let epochal_time : i64 = d.timestamp();
   return epochal_time;
}

fn parse_page(document_text : &str,
              network : &str,
              keeper_stations : &Vec<&str>) -> Vec<StationTime> {
   let mut stations : Vec<StationTime> = Vec::new();
   // Initialize search string e.g., UU_
   let mut search_string : String = network.to_string();
   search_string.push_str("_");
   // Parse the table
   let table = table_extract::Table::find_first(&document_text.to_string()).unwrap();
   for row in &table {
       if row.is_empty() {
          continue;
       }
       let row_slice = row.as_slice();
       if row_slice.len() == 5 {
          let text = row_slice.get(1).unwrap();//ok_or(&str, Ok(""));
          // Does this row contain something like "UU_", e.g., "UU_ALP.xml"
          if text.contains(&search_string) {
             // Now let's parse the tag <a href="UU_ALP.xml">UU_ALP.xml></a>
             let selector = scraper::Selector::parse(r#"a"#).unwrap();
             let table_element_fragment = scraper::Html::parse_fragment(&text);
             let station_anchor = table_element_fragment.select(&selector).next().unwrap();
             let station_xml_file = station_anchor.inner_html().to_string();
             let time = row_slice.get(2).unwrap(); 
             let timestamp = parse_string(time);
             let pair = StationTime {station: station_xml_file.clone(), time: timestamp};
             let mut keep = false;
             if !keeper_stations.is_empty() {
                if keeper_stations.iter().any(|e| station_xml_file.contains(e)) {
                   keep = true;
                }
             }
             else {
                keep = true;
             }
             if keep {         
                stations.push(pair);
             }
             /*
             for station in table_element_fragment.select(&selector) {
                 let station_name = station.value().attr("href").expect("href not found").to_string();
                 let time = row_slice.get(2).unwrap();
                 let timestamp = parse_string(time); 
                 let pair = StationTime {station: station_name.clone(), time: timestamp};
                 stations.push(pair); 
             }
             */
          }
       }
   }
   return stations;
}

fn find_stations_to_create(database_stations : &Vec<StationTime>,
                           sis_stations : &Vec<StationTime>) -> Vec<StationTime> {
   // If there are no stations in the database then we create everything
   if database_stations.is_empty() {
       return sis_stations.clone();
   }
   // Stations to be created do not exist in database
   let mut result : Vec<StationTime> = Vec::new();
   for sis_station in sis_stations.iter() {
       let station_name = sis_station.station.to_string();
       if !database_stations.iter().any(|e| station_name.contains(&e.station)) {
          log::debug!("Candidate insert {}", station_name);
          result.push(sis_station.clone());
       }
   }
   return result;
}

fn find_stations_to_update(database_stations : &Vec<StationTime>,
                           sis_stations : &Vec<StationTime>) -> Vec<StationTime> {
   // Stations to be updated exist in the database but have old load dates
   let mut result : Vec<StationTime> = Vec::new();
   for sis_station in sis_stations.iter() {
       let station_name = sis_station.station.to_string();
       let last_sis_update = sis_station.time;
       if database_stations.iter().any(|e| station_name.contains(&e.station) && last_sis_update > e.time) {
          log::debug!("Candidate update {} {}", station_name, last_sis_update);
          result.push(sis_station.clone());
       }
   }
   return result;
}

fn main() {
   // Lift the read-write database parameters
   let database_read_write_user = std::env::var("SIS_POLLER_DATABASE_READ_WRITE_USER")
       .expect("Cannot find SIS_POLLER_DATABASE_READ_WRITE_USER environment variable");
   let database_read_write_password = std::env::var("SIS_POLLER_DATABASE_READ_WRITE_PASSWORD")
       .expect("Cannot find SIS_POLLER_DATABASE_READ_WRITE_PASSWORD environent variable");
   let database_name = std::env::var("SIS_POLLER_DATABASE_NAME")
       .expect("Cannot find SIS_POLLER_DATABASE_NAME environment variable");
   let database_host = std::env::var("SIS_POLLER_DATABASE_HOST")
       .unwrap_or("localhost".to_string()); //expect("Cannot find SIS_POLLER_DATABASE_HOST environment variable");
   let database_port = std::env::var("SIS_POLLER_DATABASE_PORT")
       .unwrap_or("5432".to_string());//expect("Cannot find SIS_POLLER_DATABASE_PORT environment variable");
   let database_schema = std::env::var("SIS_POLLER_DATABASE_SCHEMA")
       .unwrap_or("".to_string());
   let database_connection_uri : String
       = std::format!("postgresql://{}:{}@{}:{}/{}",
                           database_read_write_user,
                           database_read_write_password,
                           database_host,
                           database_port,
                           database_name);

   // Initializing my logger
   env_logger::init();

   // Make sure I understand UTC time
   let ts = parse_string("2023-05-30 09:29");
   assert!(ts == 1685438940);

   log::info!("Fetching stations from database");
   let database_stations : Vec<StationTime>;
   let database_stations_result
      = get_stations_from_pg(database_connection_uri.as_str(),
                             database_schema.as_str());
   match database_stations_result {
      Ok(result) => {
         database_stations = result.clone();
      }
      Err(error) => {
         log::warn!("Error in getting database stations: {error:?}");
         return;
      }
   }
   
   log::info!("Got {} stations from database",database_stations.len());

   let base_uri = String::from("https://files.anss-sis.scsn.org/production/FDSNStationXML1.1/");
   let networks = vec!["UU"];//["UU", "WY", "IW", "UW"];
   let iw_keeper_stations = vec!["FLWY", "IMW", "LOHW", "MOOW", "REDW", "RWWY", "SNOW", "TPAW"];
   let us_keeper_stations = vec!["AHID", "BOZ", "BW06", "DUG",  "ELK",  "HLID", "HWUT", "ISCO", "LWKY", "MVCO", "TPNV", "WUAZ"];
   let mut sis_stations : Vec<StationTime> = Vec::new();
   for network in networks.iter() {
       let mut uri : String = base_uri.clone();
       if !uri.ends_with('/') {
          uri.push('/');
       }
       uri.push_str(network);
       log::info!("Fetching data from URI: {}", uri);
       let html_text_result = get_page(&uri);
       match html_text_result {
          Ok(html_text) => {
             log::debug!("Parsing HTML...");
             let mut keeper_stations : Vec<&str> = Vec::new();
             if *network == "IW" {
                keeper_stations = iw_keeper_stations.clone();
             }
             else if *network == "US" {
                keeper_stations = us_keeper_stations.clone();
             }
             let stations = parse_page(&html_text, &network, &keeper_stations);
             log::info!("Unpacked {} stations for network {}", stations.len(), network); 
             sis_stations.extend(stations); 
             //let new_stations = stations_to_create(&database_stations, &sis_stations); 
          }
          Err(error) => {
             log::warn!("Error in getting HTML: {error:?}");
             continue;
          }
       }
   } 
   log::debug!("Returned {} stations from SIS", sis_stations.len());
   let stations_to_create = find_stations_to_create(&database_stations, &sis_stations);
   log::info!("Will create {} stations", stations_to_create.len());
   let stations_to_update = find_stations_to_update(&database_stations, &sis_stations);
   log::info!("Will update {} stations", stations_to_update.len());
}
