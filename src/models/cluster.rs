use crate::models::point::Point;

pub struct Cluster {
    points: Vec<Point>,
}

impl Cluster {
    pub fn new() -> Self {
        Self { points: Vec::new() }
    }

    pub fn new_with_point(points: Vec<Point>) -> Self {
        Self { points }
    }

    pub fn add_point(&mut self, point: Point) {
        self.points.push(point);
    }

    pub fn has(&self, point: &Point) -> bool {
        for p in self.points.iter() {
            if p == point {
                return true;
            }
        }

        false
    }

    pub fn get_points(self) -> Vec<Point> {
        self.points
    }

    pub fn concat_points(mut self, points: Vec<Point>) -> Self {
        self.points = self.points.into_iter().chain(points.into_iter()).collect();

        Self {
            points: self.points,
        }
    }
}
