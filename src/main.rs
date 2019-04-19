extern crate inc_dbscan;

use inc_dbscan::{
    file_io::{read_csv_file, write_csv_file},
    inc_dbscan::inc_dbscan,
};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() == 8 {
        let eps: f64 = args[3].parse().expect("eps isn't a Double!");
        let min_pts: usize = args[4].parse().expect("minPts isn't a Number!");
        let max_spd: f64 = args[5].parse().expect("maxSpd isn't a Double!");
        let max_dir: f64 = args[6].parse().expect("maxDir isn't a Double!");
        let is_stop_point: bool = args[7].parse().expect("isStopPoint isn't a Bool!");

        let points = read_csv_file(&args[1], is_stop_point).expect("read file error");
        let clusters = inc_dbscan(points, eps, min_pts, max_spd, max_dir, is_stop_point);

        write_csv_file(&args[2], clusters);
    } else {
        println!("Wrong Usage");
    }
}
