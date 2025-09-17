#[derive(Clone)]
#[derive(Debug)]
pub struct StationTime {
   pub station : String,
   pub time : i64,
}

impl StationTime {
   #[allow(dead_code)]
   fn new(station: String, time: i64) -> StationTime {
      StationTime {station, time}
   }
}
