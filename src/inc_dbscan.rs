use crate::models::{cluster::Cluster, point::Point};
use rayon::prelude::*;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::sync::mpsc::channel;

static NOISE: i32 = -1;
static BOUND: i32 = 0;
static NEW: i32 = 1;
static ABSORB: i32 = 2;
static MERGED: i32 = 3;

pub fn inc_dbscan(
    points: Vec<Point>,
    eps: f64,
    min_pts: usize,
    max_spd: f64,
    max_dir: f64,
    is_stop_point: bool,
) -> Vec<Cluster> {
    // 模拟当前所有的点
    let mut now_points: Vec<Point> = vec![];

    // 簇集合
    let mut clusters: Vec<Cluster> = vec![];
    // 噪音集合，第一个 usize 表示在所有点中的索引
    let mut noises: Vec<(usize, Point)> = vec![];

    let sum_points = points.len();

    for point in points {
        println!("{} : {}", now_points.len(), sum_points);

        now_points.push(point.clone());
        let round = now_points.len();

        now_points = inc_nps(
            now_points,
            &point,
            round,
            eps,
            max_spd,
            max_dir,
            is_stop_point,
        );

        let (opt, infu_points) = inc_add(
            &now_points,
            &clusters,
            &point,
            round,
            eps,
            min_pts,
            max_spd,
            max_dir,
            is_stop_point,
        );

        if opt == NOISE {
            noises.push((now_points.len() - 1, point));
        } else if opt == BOUND {
            clusters[infu_points[0][0]].add_point(point);
        } else if opt == NEW {
            // 先将 point 加入到噪音点
            // 但是他会在之后的循环中加到第一个新簇中
            // 如果不进行这一步，point 将会丢失
            noises.push((now_points.len() - 1, point));

            for sub_infus in infu_points {
                // 获得索引对应的点
                let new_cores: Vec<&Point> = now_points
                    .par_iter()
                    .enumerate()
                    .filter_map(|(index, point)| {
                        if sub_infus.contains(&index) {
                            return Some(point);
                        }

                        None
                    })
                    .collect();

                let (cluster_opt, (mut infu_clusters, new_cluster_point_indexs)) = inc_cluster(
                    &now_points,
                    &clusters,
                    &new_cores,
                    round,
                    eps,
                    min_pts,
                    max_spd,
                    max_dir,
                    is_stop_point,
                );

                // 只把还是噪音点的点加入到新簇中
                let mut new_cluster_points: Vec<Point> = vec![];
                let mut new_noises: Vec<(usize, Point)> = vec![];
                for (index, point) in noises {
                    if new_cluster_point_indexs.contains(&index) {
                        new_cluster_points.push(point);
                    } else {
                        new_noises.push((index, point));
                    }
                }
                noises = new_noises;

                // 创建新簇
                if cluster_opt == NEW {
                    clusters.push(Cluster::new_with_point(new_cluster_points));
                } else if cluster_opt == ABSORB {
                    let mut cluster = clusters.remove(infu_clusters[0]);
                    cluster = cluster.concat_points(new_cluster_points);
                    clusters.push(cluster);
                } else if cluster_opt == MERGED {
                    // 规范化索引
                    let main_cluster_index = infu_clusters[0];
                    infu_clusters = infu_clusters
                        .into_par_iter()
                        .filter_map(|infu_cluster| {
                            if infu_cluster > main_cluster_index {
                                return Some(infu_cluster - 1);
                            }
                            if infu_cluster == main_cluster_index {
                                return None;
                            }
                            Some(infu_cluster)
                        })
                        .collect();
                    infu_clusters.par_sort();

                    // 合并旧簇
                    let mut main_cluster = clusters.remove(main_cluster_index);
                    for i in (0..infu_clusters.len()).rev() {
                        let cluster = clusters.remove(infu_clusters[i]);
                        let points = cluster.get_points();

                        main_cluster = main_cluster.concat_points(points);
                    }

                    // 吸收新簇
                    main_cluster = main_cluster.concat_points(new_cluster_points);

                    clusters.push(main_cluster);
                }
            }
        }
    }

    clusters
}

/// 修改 point 邻居集内的点属性
fn inc_nps(
    points: Vec<Point>,
    point: &Point,
    round: usize,
    eps: f64,
    max_spd: f64,
    max_dir: f64,
    is_stop_point: bool,
) -> Vec<Point> {
    let (sender, receiver) = channel();
    let mut points: Vec<Point> = points
        .into_par_iter()
        .map_with(sender, |s, p| {
            if is_reachable(&p, point, eps, max_spd, max_dir, is_stop_point) {
                s.send(1).unwrap();
                return p.inc_nps(round);
            }

            p
        })
        .collect();

    // point 必须也要正确设置，上面只会让 point nps 为 1
    let n_len = receiver.iter().sum();
    points[round - 1].set_nps(n_len);

    points
}

/// 新建一个簇；合并到一个簇；几个簇合并
/// 第一个参数暗示操作，第二个暗示操作涉及的簇索引集合和新簇含有的点索引集合
/// 1. 只生成了一个新簇，第二个参数为该簇内的所有点索引
/// 2. 合并到了一个旧簇，第二个参数表示要合并到的簇索引
/// 3. 多个旧簇合并并吸收新簇，第三个参数表示多个要合并到一起的簇索引
fn inc_cluster(
    points: &Vec<Point>,
    clusters: &Vec<Cluster>,
    new_cores: &Vec<&Point>,
    round: usize,
    eps: f64,
    min_pts: usize,
    max_spd: f64,
    max_dir: f64,
    is_stop_point: bool,
) -> (i32, (Vec<usize>, Vec<usize>)) {
    // 获得所有新核心点的大邻居集
    let neighbours: Vec<(usize, &Point)> = points
        .par_iter()
        .enumerate()
        .filter(|(_, p)| {
            for new_core in new_cores.iter() {
                if is_reachable(p, new_core, eps, max_spd, max_dir, is_stop_point) {
                    return true;
                }
            }
            false
        })
        .collect();

    // 选择旧核心点
    let upd_seed_ins: Vec<&&Point> = neighbours
        .par_iter()
        .filter_map(|(_, pt)| {
            // 如果该点是 point 邻居集内，旧核心点邻居集长度大于 min_pts
            if pt.get_round() == round && pt.get_nps() > min_pts {
                return Some(pt);
            }
            // 如果该点不在其邻居集内，长度大于等于 min_pts 即可
            else if pt.get_round() < round && pt.get_nps() >= min_pts {
                return Some(pt);
            }

            None
        })
        .collect();

    // 大邻居集内的旧核心点对应的簇索引，利用 HashSet 去重
    let cluster_indexs: HashSet<usize> = upd_seed_ins
        .into_par_iter()
        .filter_map(|p| {
            for (index, cluster) in clusters.iter().enumerate() {
                if cluster.has(p) {
                    return Some(index);
                }
            }

            None
        })
        .collect();
    let cluster_indexs = Vec::from_iter(cluster_indexs.into_iter());

    // 获得所有邻居的索引
    let neighbour_indexs: Vec<usize> = neighbours.into_par_iter().map(|(index, _)| index).collect();

    let result = match cluster_indexs.len() {
        // 没有旧核心点，说明新建了一个簇
        0 => NEW,
        // 只有一个旧簇的话就是吸收
        1 => ABSORB,
        // 多个则是多个簇合并
        _ => MERGED,
    };

    (result, (cluster_indexs, neighbour_indexs))
}

/// 第一个参数暗示操作，第二个参数暗示操作涉及的簇索引或点索引
/// 1. 新加入的点是噪音，没有产生影响，第二个参数无作用
/// 2. 新加入的点是边界点，被一个现有的簇给吸收，第二个参数表示吸收该点的簇索引
/// 3. 新加入的点产生了新的核心点，第二个参数表示新生成的核心点索引
fn inc_add(
    points: &Vec<Point>,
    clusters: &Vec<Cluster>,
    point: &Point,
    round: usize,
    eps: f64,
    min_pts: usize,
    max_spd: f64,
    max_dir: f64,
    is_stop_point: bool,
) -> (i32, Vec<Vec<usize>>) {
    let neighbours: Vec<(usize, &Point)> = points
        .par_iter()
        .enumerate()
        .filter(|(_, p)| p.get_round() == round)
        .collect();
    let n_len = neighbours.len();

    // 寻找核心点，第一个 bool 表示是否是新生成的核心点
    let mut cores: Vec<(bool, (usize, &Point))> = neighbours
        .into_par_iter()
        .filter_map(|(index, pt)| {
            if pt == point {
                return None;
            }

            if pt.get_nps() == min_pts {
                return Some((true, (index, pt)));
            } else if pt.get_nps() > min_pts {
                return Some((false, (index, pt)));
            }

            None
        })
        .collect();
    if n_len >= min_pts {
        cores.push((true, (points.len() - 1, point)));
    }

    // 区分新旧核心点
    let mut new_cores: Vec<(usize, &Point)> = vec![];
    let mut old_cores: Vec<&Point> = vec![];
    for (is_new, (index, point)) in cores {
        if is_new {
            new_cores.push((index, point));
        } else {
            old_cores.push(point);
        }
    }

    let new_len = new_cores.len();
    let old_len = old_cores.len();

    // 如果 point 周围没有新的核心点以及旧的核心点，说明 point 是一个噪音点
    if new_len == 0 && old_len == 0 {
        return (NOISE, vec![]);
    }
    // 如果 point 周围没有新的核心点但是有旧的核心点，说明 point 是一个边界点
    else if new_len == 0 && old_len > 0 {
        // 把该点加入到第一个簇
        let first_core = old_cores.first().unwrap();
        for (index, cluster) in clusters.iter().enumerate() {
            if cluster.has(first_core) {
                return (BOUND, vec![vec![index]]);
            }
        }
    }
    // 如果 point 周围出现了新的核心点
    else if new_len > 0 {
        // 将 new_cores 融合一次，减少未来时间度
        // 这里基本不耗时间，新生成的核心点数量没有这么多
        let mut merge_cores: Vec<Vec<(usize, &Point)>> = Vec::new();
        for (index, core) in new_cores {
            let mut merge_indexs: Vec<usize> = merge_cores
                .par_iter()
                .enumerate()
                .filter_map(|(i, merged_cores)| {
                    for (_, p) in merged_cores {
                        if is_reachable(p, core, eps, max_spd, max_dir, is_stop_point) {
                            return Some(i);
                        }
                    }

                    None
                })
                .collect();

            if merge_indexs.len() == 0 {
                merge_cores.push(vec![(index, core)]);
            } else if merge_indexs.len() == 1 {
                merge_cores[merge_indexs[0]].push((index, core));
            } else {
                // 规范化索引
                // 由于第一步是删除 main_cores 会导致后面的索引往前一个
                let main_index = merge_indexs[0];
                merge_indexs = merge_indexs
                    .into_par_iter()
                    .filter_map(|index| {
                        if index > main_index {
                            return Some(index - 1);
                        }
                        if index == main_index {
                            return None;
                        }

                        Some(index)
                    })
                    .collect();
                merge_indexs.par_sort();

                // 合并到第一个中
                let mut main_cores = merge_cores.remove(main_index);
                for i in (0..merge_indexs.len()).rev() {
                    let sub_cores = merge_cores.remove(merge_indexs[i]);

                    main_cores = main_cores
                        .into_iter()
                        .chain(sub_cores.into_iter())
                        .collect();
                }
                merge_cores.push(main_cores);
            }
        }

        // 提取出索引即可
        let new_uni_cores: Vec<Vec<usize>> = merge_cores
            .into_par_iter()
            .map(|cores| cores.into_iter().map(|(index, _)| index).collect())
            .collect();

        return (NEW, new_uni_cores);
    }

    (NOISE, vec![])
}

/// 两点是否可达
fn is_reachable(
    p1: &Point,
    p2: &Point,
    eps: f64,
    max_spd: f64,
    max_dir: f64,
    is_stop_point: bool,
) -> bool {
    if cal_distance_bwt_two_points(p1, p2) <= eps {
        if is_stop_point {
            return true;
        }

        if (p1.get_cog() - p2.get_cog()).abs() < max_dir {
            if (p1.get_sog() - p2.get_sog()).abs() < max_spd {
                return true;
            }
        }
    }

    false
}

/// 两点之间的距离
fn cal_distance_bwt_two_points(p1: &Point, p2: &Point) -> f64 {
    let dx: f64 = p1.get_longitude() - p2.get_longitude();
    let dy: f64 = p1.get_latitude() - p2.get_latitude();

    let distance = (dx * dx + dy * dy).sqrt();

    distance
}
