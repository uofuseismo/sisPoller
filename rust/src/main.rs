use clap::Parser;

static DEFAULT_INI_FILE: &str = "./sisPoller.ini"; 

/*
#[derive(Clone)]
#[derive(Debug)]
struct StationTime {
   station : String,
   time : i64,
}
*/

mod database;
mod datatypes;
use crate::datatypes::station_time::StationTime;

#[derive(Clone)]
struct Parameters {
   sqlite3_file : String,
   database_host : String,
   database_port : i64,
   database_name : String,
   database_schema : String,
   database_user : String,
   database_password : String,
   api_uri : String,
   api_key : String,
   api_notification_topic : String,
   api_notification_type : String,
}

#[derive(Parser)]
#[command(name = "sisPoller")]
#[command(version)]
#[command(about = "Polls the SIS XML page to detect station updates")]
#[command(long_about = None)]
struct CommandLineArguments {
   #[arg(short, long, default_value = DEFAULT_INI_FILE)]
   ini_file: String,
   #[arg(long, default_value_t = true)]
   use_sqlite3: bool,
   #[arg(long, default_value_t = false)]
   initialize: bool, 
}

/*
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

fn get_stations_from_sqlite3(sqlite3_file : &str) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   // Make sure the sqlite3 file exists
   create_xml_table_sqlite3(sqlite3_file);
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
*/

/*
fn create_xml_table_sqlite3(sqlite3_file : &str ) {
   let sqlite_database_exists = std::fs::exists(sqlite3_file).unwrap();
   if !sqlite_database_exists {
      log::info!("Creating sqlite3 database {}", sqlite3_file);
      let connection = rusqlite::Connection::open(sqlite3_file).unwrap();
      connection.execute("CREATE TABLE xml_update (xml_file TEXT, last_modified TEXT)", (), ).unwrap();
   }
   else {
      log::debug!("sqlite3 database {} already exists", sqlite3_file);
   }
}

fn create_stations_in_sqlite3(sqlite3_file : &str,
                              stations_to_create : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   create_xml_table_sqlite3(sqlite3_file);
   let mut created_stations : Vec<StationTime> = Vec::new();
   if !stations_to_create.is_empty() {
      let connection = rusqlite::Connection::open(sqlite3_file).unwrap();
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
      log::info!("Created {} out of {} stations in sqlite3 database", 
                 created_stations.len(), stations_to_create.len());
   }
   else {
      log::debug!("No stations to add to sqlite3");
   }
   return Ok(created_stations);
}

fn update_stations_in_sqlite3(sqlite3_file : &str,
                              stations_to_update : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
   //assert!(std::fs::exists(sqlite3_file).unwrap()); // Has to exist
   let mut updated_stations : Vec<StationTime> = Vec::new();
   if !stations_to_update.is_empty() {
      let connection = rusqlite::Connection::open(sqlite3_file).unwrap();
      for station in stations_to_update.iter() {
         let time : i64 = station.time as i64;
         let result = connection.execute(
             "UPDATE xml_update SET last_modified TO DATETIME(?1, 'unixepoch') WHERE xml_file = ?2",
             (&time, &station.station), );
         match result {
            Ok(result) => {
               log::debug!("Successful updated -> station {} row", &result);
               updated_stations.push(station.clone());
            }
            Err(result) => {
               log::warn!("Insert failed -> {}", &result);
            }
         }
      }
      log::info!("Updated {} out of {} stations in sqlite3 database",
                 updated_stations.len(), stations_to_update.len());
   }
   else {
      log::debug!("No stations to add to sqlite3");
   }
   return Ok(updated_stations);
}
*/

/*
fn create_stations_in_pg(connection_uri : &str, 
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

fn update_stations_in_pg(connection_uri : &str, 
                         schema : &str,
                         stations_to_update : &Vec<StationTime>) -> Result<Vec<StationTime>, Box<dyn std::error::Error>> {
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
*/

fn post_to_api(uri : &str,
               api_key : &str,
               subject : &str,
               message : &str,
               topic : &str,
               notification_type : &str,
               message_identifier : &str) -> Result<String, Box<dyn std::error::Error>> {
   //let source = format!("{:?}", gethostname::gethostname());
   let source = String::from("rustSISPoller"); 
   let payload
       = serde_json::json!({
            "payload": {"subject": subject,
                        "message": message,
                        "topic": topic,
                        "notificationType": notification_type,
                        "messageIdentifier": message_identifier,
                        "source": source}
                       });
   log::debug!("Sending payload: {}", payload.to_string());
   let client = reqwest::blocking::Client::new();
   let response = client.put(uri)
                 .header("x-api-key", api_key)
                 .header("Content-Type", "application/json")
                 .header("Accept", "application/json")
                 .body(payload.to_string())
                 .send()?;

   if response.status() == 200 {
      log::debug!("Successfully put to API");
      let document_text = response.text()?.clone();
      return Ok(document_text);
   }
   log::warn!("Errors detected while putting message to API");
   return Err("Failed to post message".into());

}

fn create_email_message(stations_to_create : &Vec<StationTime>,
                        stations_to_update : &Vec<StationTime>) -> String {
   let mut result = String::from("");
   if stations_to_create.is_empty() && stations_to_update.is_empty() {
      return result;
   }
   for station in stations_to_create.iter() {
      let create_string : String = format!("Added {}\n", station.station);
      result.push_str(&create_string);
   }
   for station in stations_to_update.iter() {
      let update_string : String = format!("Updated {}\n", station.station);
      result.push_str(&update_string);
   }
   return result;
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

fn load_configuration(configuration_file : &String,
                      use_sqlite3 : bool,
                      initialize : bool) -> Result<Parameters, Box<dyn std::error::Error>> {
   use configparser::ini::Ini;
   let mut config = Ini::new();
   let _map = config.load(configuration_file)?;

   let mut sqlite3_file : String = String::from("./sisPoller.sqlite3");
   let mut pg_database_host : String = String::from("localhost");
   let mut pg_database_port : i64 = 5432;
   let mut pg_database_name : String = String::from("");
   let mut pg_database_schema : String = String::from("");
   let mut pg_database_user : String = String::from("");
   let mut pg_database_password : String = String::from(""); 
   if use_sqlite3 {
      let sqlite3_database_section = String::from("SISSqlite3Database");
      let sqlite3_file_result = config.get(sqlite3_database_section.as_str(), "file_name");
      match sqlite3_file_result {
         Some(value) => sqlite3_file = value,
         None => sqlite3_file = String::from("./sisPoller.sqlite3"),
      }
   }
   else {
      let pg_database_section = String::from("SISPostgresDatabase");
      pg_database_host = config.get(pg_database_section.as_str(), "host").unwrap();
      pg_database_port = config.getint(pg_database_section.as_str(), "port").unwrap().unwrap();
      pg_database_name = config.get(pg_database_section.as_str(), "name").unwrap();
      //let database_schema = config.get(pg_database_section.as_str(), "schema").unwrap();
      //let database_schema : String;
      let database_schema_result = config.get(pg_database_section.as_str(), "schema");
      match database_schema_result {
         Some(value) => pg_database_schema = value,
         None => pg_database_schema = String::from(""),
      } 
      pg_database_user = config.get(pg_database_section.as_str(), "user").unwrap();
      pg_database_password = config.get(pg_database_section.as_str(), "password").unwrap();
   }

   let mut api_uri : String = String::from("");
   let mut api_key : String = String::from("");
   let mut api_notification_topic : String = String::from("production");
   let mut api_notification_type : String = String::from("update_email");
   if !initialize {
      let api_section = String::from("AWSDistributionAPI");
      api_uri = config.get(api_section.as_str(), "uri").unwrap();
      api_key = config.get(api_section.as_str(), "key").unwrap();
      let api_notification_topic_result = config.get(api_section.as_str(), "notificationTopic");
      match api_notification_topic_result {
         Some(value) => api_notification_topic = value,
         None => api_notification_topic = String::from("production"),
      }

      let api_notification_type_result = config.get(api_section.as_str(), "notificationType");
      match api_notification_type_result {
         Some(value) => api_notification_type = value,
         None => api_notification_type = String::from("update_email"),
      }
   }

   let result = Parameters{
                             sqlite3_file: sqlite3_file.to_string(),
                             database_host: pg_database_host.to_string(),
                             database_port: pg_database_port,
                             database_name: pg_database_name.to_string(),
                             database_schema: pg_database_schema.to_string(),
                             database_user: pg_database_user.to_string(),
                             database_password: pg_database_password.to_string(),
                             api_uri: api_uri.to_string(),
                             api_key: api_key.to_string(),
                             api_notification_topic: api_notification_topic.to_string(),
                             api_notification_type: api_notification_type.to_string(),
                          };
   return Ok(result);
}

#[cfg(test)]
mod tests {
   // Import names from outer (for mod tests) scope)
   use super::*;
   //let ts = parse_string("2023-05-30 09:29");
   assert_eq!(parse_string("2023-05-30 09:29"), 1685438940);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
   // Get command line arguments
   let command_line_arguments = CommandLineArguments::parse();

   // Initializing my logger
   env_logger::init();

   //let args: Vec<String> = std::env::args().collect();
   //let ini_file : String = String::from("sisPoller.ini");
   let parameters_result = load_configuration(&command_line_arguments.ini_file,
                                              command_line_arguments.use_sqlite3,
                                              command_line_arguments.initialize);
   let parameters : Parameters;
   match parameters_result {
      Ok(result) => {
         parameters = result.clone();
      }   
      Err(error) => {
         log::warn!("Error loading parameters from initialization file: {error:?}");
         return Err("Failed to load parameters from initialization file".into());
      }
   }

   // Load the command line arguments
   let database_connection_uri : String
      = std::format!("postgresql://{}:{}@{}:{}/{}",
                     parameters.database_user,
                     parameters.database_password,
                     parameters.database_host,
                     parameters.database_port,
                     parameters.database_name);


   // Get the command line arguments
   //let args: Vec<String> = std::env::args().collect();
   // Lift the read-write database parameters
   //let database_read_write_user = std::env::var("SIS_POLLER_DATABASE_READ_WRITE_USER")
   //    .expect("Cannot find SIS_POLLER_DATABASE_READ_WRITE_USER environment variable");
   //let database_read_write_password = std::env::var("SIS_POLLER_DATABASE_READ_WRITE_PASSWORD")
   //    .expect("Cannot find SIS_POLLER_DATABASE_READ_WRITE_PASSWORD environent variable");
   //let database_name = std::env::var("SIS_POLLER_DATABASE_NAME")
   //    .expect("Cannot find SIS_POLLER_DATABASE_NAME environment variable");
   //let database_host = std::env::var("SIS_POLLER_DATABASE_HOST")
   //    .unwrap_or("localhost".to_string()); //expect("Cannot find SIS_POLLER_DATABASE_HOST environment variable");
   //let database_port = std::env::var("SIS_POLLER_DATABASE_PORT")
   //    .unwrap_or("5432".to_string());//expect("Cannot find SIS_POLLER_DATABASE_PORT environment variable");
   //let database_schema = std::env::var("SIS_POLLER_DATABASE_SCHEMA")
   //    .unwrap_or("".to_string());
   //let database_connection_uri : String
   //   = std::format!("postgresql://{}:{}@{}:{}/{}",
   //                  database_read_write_user,
   //                  database_read_write_password,
   //                  database_host,
   //                  database_port,
   //                  database_name);
   // Lift the API endpoint and key
   //let notification_api_uri = std::env::var("SIS_NOTIFICATION_API_URI")
   //   .expect("Cannot find SIS_NOTIFICATION_API_URI");
   //let notification_api_key = std::env::var("SIS_NOTIFICATION_API_KEY")
   //   .unwrap_or("".to_string()); //expect("Cannot find SIS_NOTIFICATION_API_KEY");
   //let notification_topic = std::env::var("SIS_NOTIFICATION_API_TOPIC")
   //   .unwrap_or("production".to_string()); // Can be production or test
   //let notification_type = std::env::var("SIS_NOTIFICATION_API_TYPE")
   //   .unwrap_or("update_email".to_string()); // Can be test_email or update_email
   //let notification_topic : String = "test".to_string(); // Can be production or test
   //let notification_type : String = "update_email".to_string(); // Can be test_email or update_email

   // Make sure I understand UTC time
   let ts = parse_string("2023-05-30 09:29");
   assert!(ts == 1685438940);

   let database_stations : Vec<StationTime>;
   if command_line_arguments.use_sqlite3 {
      let database_stations_result
         = database::sqlite3::get_stations(parameters.sqlite3_file.as_str());
      match database_stations_result {
         Ok(result) => {
            database_stations = result.clone();
         }
         Err(error) => {
            log::warn!("Error in getting database stations from sqlite3: {error:?}");
            return Err("Failed getting database stations from database sqlite3".into());
         }
      }   
   }
   else {
      log::info!("Fetching stations from postgres database");
      let database_stations_result
         = database::postgres::get_stations(database_connection_uri.as_str(),
                                            parameters.database_schema.as_str());
      match database_stations_result {
         Ok(result) => {
            database_stations = result.clone();
         }
         Err(error) => {
            log::warn!("Error in getting database stations from postgres: {error:?}");
            return Err("Failed getting database stations from postgres database".into());
         }
      }
   }

   log::info!("Got {} stations from database", database_stations.len());


   let base_uri = String::from("https://files.anss-sis.scsn.org/production/FDSNStationXML1.1/");
   //let networks = vec!["UU"];
   let networks = vec!["UU", "WY", "IW", "US", "C0"];
   let iw_keeper_stations = vec!["FLWY", "IMW", "LOHW", "MOOW", "REDW", "RWWY", "SNOW", "TPAW"];
   let us_keeper_stations = vec!["AHID", "BOZ", "BW06", "DUG",  "ELK",  "HLID", "HWUT", "ISCO", "LKWY", "MVCO", "TPNV", "WUAZ"];
   let c0_keeper_stations = vec!["MOFF"];
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
             else if *network == "C0" {
                keeper_stations = c0_keeper_stations.clone();
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

   let candidate_stations_to_create = find_stations_to_create(&database_stations, &sis_stations);
   log::info!("Will attempt to create {} stations", 
              candidate_stations_to_create.len());
   let stations_to_create : Vec<StationTime>;
   if command_line_arguments.use_sqlite3 {
      let stations_to_create_result
          = database::sqlite3::create_stations(parameters.sqlite3_file.as_str(), 
                                               &candidate_stations_to_create);
      match stations_to_create_result {
         Ok(result) => {
            stations_to_create = result.clone();
         }
         Err(error) => {
            log::warn!("Error adding stations to sqlite3: {error:?}");
            return Err("Failed to add stations to sqlite3 database".into());
         }   
      }
      log::info!("Created {} stations in sqlite3", stations_to_create.len());
   }
   else {
      stations_to_create
          = database::postgres::create_stations(database_connection_uri.as_str(),
                                                parameters.database_schema.as_str(),
                                                &candidate_stations_to_create)?;
      log::info!("Created {} stations in postgres", stations_to_create.len());
   }

   let candidate_stations_to_update = find_stations_to_update(&database_stations, &sis_stations);
   log::info!("Will attempt to update {} stations", 
              candidate_stations_to_update.len());

   let stations_to_update : Vec<StationTime>;
   if command_line_arguments.use_sqlite3 {
      let stations_to_update_result
          = database::sqlite3::update_stations(parameters.sqlite3_file.as_str(), 
                                               &candidate_stations_to_update);
      match stations_to_update_result {
         Ok(result) => {
            stations_to_update = result.clone();
         }
         Err(error) => {
            log::warn!("Error updating stations to sqlite3: {error:?}");
            return Err("Failed to add updating to sqlite3 database".into());
         }
      }   
      log::info!("Updated {} stations in sqlite3", stations_to_update.len());
   }
   else {
      stations_to_update
         = database::postgres::update_stations(database_connection_uri.as_str(),
                                               parameters.database_schema.as_str(),
                                               &candidate_stations_to_update)?;
      log::info!("Updated {} stations in postgres", stations_to_update.len());
   }

   if !command_line_arguments.initialize {
      let message : String = create_email_message(&stations_to_create, &stations_to_update);
      if !message.is_empty() {
         let subject : String = "SIS poller notification".to_string();
         let random_number : u32 = rand::random_range(0..=100000);
         let message_identifier : String = "sisUpdateMessage_".to_string()
                                         + &random_number.to_string(); // Could also be sisTestMessage
         let post_result = post_to_api(&parameters.api_uri,
                                       &parameters.api_key,
                                       &subject,
                                       &message,
                                       &parameters.api_notification_topic,
                                       &parameters.api_notification_type,
                                       &message_identifier);
         match post_result {
            Ok(post_result) => {
               log::info!("Succesfully put message to API {post_result:?}");
            }
            Err(error) => {
               log::warn!("Failed to post message to API: {error:?}");
               return Err("Failed to post message to API".into());
            }
         }
      }
      else {
         log::info!("No updates detected");
      }
   }
   else {
      log::info!("Initialization mode - no updates posted to API");
   }
   Ok(())
}
