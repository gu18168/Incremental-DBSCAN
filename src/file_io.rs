use crate::models::{cluster::Cluster, point::Point};
use csv::Reader;
use failure::Error;
use std::fs::OpenOptions;
use std::io::Write;

pub fn read_csv_file(path: &str, is_stop_point: bool) -> Result<Vec<Point>, Error> {
    let mut rdr = Reader::from_path(path)?;
    let mut points = Vec::new();

    for record in rdr.records().filter_map(|result| result.ok()) {
        let sog: f64 = record.get(1).unwrap().parse().expect("SOG must be a f64!");
        let longitude: f64 = record
            .get(2)
            .unwrap()
            .parse()
            .expect("longitude must be a f64!");
        let latitude: f64 = record
            .get(3)
            .unwrap()
            .parse()
            .expect("latitude must be a f64!");
        let cog: f64 = record.get(4).unwrap().parse().expect("COG must be a f64!");

        // 暂停点但是速度太快 & 移动点但是速度太慢
        if !is_stop_point && sog <= 0.5 {
            continue;
        };
        if is_stop_point && sog > 0.5 {
            continue;
        };

        let point = Point::new(longitude, latitude, sog, cog);
        points.push(point);
    }

    Ok(points)
}

pub fn write_csv_file(path: &str, clusters: Vec<Cluster>) {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .expect("File can't write");

    // 写入 csv 文件首行
    file.write_all(b"clusterIndex,Longitude,Latitude,SOG,COG\n")
        .expect("File can't write");

    for (index, cluster) in clusters.into_iter().enumerate() {
        for p in cluster.get_points() {
            let line = index.to_string()
                + ","
                + &p.get_longitude().to_string()
                + ","
                + &p.get_latitude().to_string()
                + ","
                + &p.get_sog().to_string()
                + ","
                + &p.get_cog().to_string()
                + "\n";
            file.write_all(line.as_bytes()).expect("File can't write");
        }
    }
}
