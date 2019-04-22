use std::hash::{Hash, Hasher};
use uuid::Uuid;

#[derive(Clone)]
pub struct Point {
    uuid: Uuid,
    longitude: f64,
    latitude: f64,
    sog: f64,
    cog: f64,
    nps: usize,
    round: usize
}

impl PartialEq for Point {
    fn eq(&self, other: &Point) -> bool {
        self.uuid == other.uuid
    }
}

impl Eq for Point {}

impl Hash for Point {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl Point {
    pub fn new(longitude: f64, latitude: f64, sog: f64, cog: f64) -> Self {
        Self {
            uuid: Uuid::new_v4(),
            longitude,
            latitude,
            sog,
            cog,
            nps: 0,
            round: 0
        }
    }

    pub fn inc_nps(mut self, round: usize) -> Self {
        self.round = round;
        self.nps += 1;
        self
    }

    pub fn set_nps(&mut self, nps: usize) {
        self.nps = nps;
    }

    pub fn get_uuid(&self) -> &Uuid {
        &self.uuid
    }

    pub fn get_longitude(&self) -> f64 {
        self.longitude
    }

    pub fn get_latitude(&self) -> f64 {
        self.latitude
    }

    pub fn get_sog(&self) -> f64 {
        self.sog
    }

    pub fn get_cog(&self) -> f64 {
        self.cog
    }

    pub fn get_nps(&self) -> usize {
        self.nps
    }

    pub fn get_round(&self) -> usize {
        self.round
    }
}
