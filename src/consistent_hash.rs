use std::cmp::Ordering;
use std::io::Write;
use std::net::SocketAddr;
struct Bucket {
    node: SocketAddr,
    weight: u32,
}

impl Bucket {
    pub fn new(node: SocketAddr, weight: u32) -> Self {
        assert!(weight > 0, "weight should be positive");
        Self { node, weight }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Point {
    // node index to actual node
    node_index: u32,
    hash: u32,
}

impl Ord for Point {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}

impl PartialOrd for Point {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Continuum {
    ring: Box<[Point]>,
    addrs: Box<[SocketAddr]>,
}

impl Continuum {
    pub const DEFAULT_NUM_VIRTUAL_POINTS: usize = 160;

    fn with_default_points(buckets: &[Bucket]) -> Self {
        Self::new(buckets, Self::DEFAULT_NUM_VIRTUAL_POINTS)
    }

    fn new(buckets: &[Bucket], num_virtual_points: usize) -> Self {
        if buckets.is_empty() {
            return Continuum {
                ring: Box::new([]),
                addrs: Box::new([]),
            };
        }

        let total_weights = buckets.iter().fold(0, |acc, item| acc + item.weight);
        let mut ring = Vec::with_capacity(num_virtual_points * total_weights as usize);
        let mut addrs = Vec::with_capacity(buckets.len());

        for bucket in buckets {
            // HOST + NULL BYTE + PORT + PREV_HASH
            addrs.push(bucket.node);

            let mut hasher = crc32fast::Hasher::new();
            // max_len(ipv6)(39) + len(null)(1) + max_len(port)(5)
            let mut hash_bytes = Vec::with_capacity(39 + 1 + 5);
            write!(&mut hash_bytes, "{}", bucket.node.ip()).unwrap();
            write!(&mut hash_bytes, "\0").unwrap();
            write!(&mut hash_bytes, "{}", bucket.node.port()).unwrap();
            hasher.update(&hash_bytes);

            let node_index = addrs.len() - 1;
            let mut prev_hash = 0u32;
            let num_points = num_virtual_points * bucket.weight as usize;

            for _ in 0..num_points {
                let mut hasher = hasher.clone();
                hasher.update(&prev_hash.to_le_bytes());

                let hash = hasher.finalize();
                ring.push(Point {
                    node_index: node_index as u32,
                    hash,
                });
                prev_hash = hash;
            }
        }

        ring.sort_unstable();
        ring.dedup_by(|a, b| a.hash == b.hash);

        Self {
            ring: ring.into_boxed_slice(),
            addrs: addrs.into_boxed_slice(),
        }
    }

    pub fn node_idx(&self, input: &[u8]) -> usize {
        let hash = crc32fast::hash(input);

        match self.ring.binary_search_by(|p| p.hash.cmp(&hash)) {
            Ok(i) => i,
            Err(i) => {
                if i == self.ring.len() {
                    0
                } else {
                    i
                }
            }
        }
    }

    pub fn node(&self, hash_key: &[u8]) -> Option<SocketAddr> {
        self.ring
            .get(self.node_idx(hash_key))
            .map(|x| self.addrs[x.node_index as usize])
    }

    pub fn node_iter(&self, hash_key: &[u8]) -> NodeIterator {
        NodeIterator {
            idx: self.node_idx(hash_key),
            continuum: self,
        }
    }

    pub fn get_addr(&self, point_index: &mut usize) -> Option<&SocketAddr> {
        let point = self.ring.get(*point_index);
        if point.is_some() {
            // move to next node
            *point_index = (*point_index + 1) % self.ring.len();
        }

        point.map(|p| &self.addrs[p.node_index as usize])
    }
}

pub struct NodeIterator<'a> {
    idx: usize,
    continuum: &'a Continuum,
}

impl<'a> Iterator for NodeIterator<'a> {
    type Item = &'a SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.continuum.get_addr(&mut self.idx)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::Path;

    use super::{Bucket, Continuum};

    fn get_sockaddr(ip: &str) -> SocketAddr {
        ip.parse().unwrap()
    }

    #[test]
    fn consistency_after_adding_host() {
        fn assert_hosts(c: &Continuum) {
            assert_eq!(c.node(b"a"), Some(get_sockaddr("127.0.0.10:6443")));
            assert_eq!(c.node(b"b"), Some(get_sockaddr("127.0.0.5:6443")));
        }

        let buckets: Vec<_> = (1..11)
            .map(|u| Bucket::new(get_sockaddr(&format!("127.0.0.{u}:6443")), 1))
            .collect();
        let c = Continuum::with_default_points(&buckets);
        assert_hosts(&c);

        // Now add a new host and ensure that the hosts don't get shuffled.
        let buckets: Vec<_> = (1..12)
            .map(|u| Bucket::new(get_sockaddr(&format!("127.0.0.{u}:6443")), 1))
            .collect();

        let c = Continuum::with_default_points(&buckets);
        assert_hosts(&c);
    }

    #[test]
    fn matches_nginx_sample() {
        let upstream_hosts = ["127.0.0.1:7777", "127.0.0.1:7778"];
        let upstream_hosts = upstream_hosts.iter().map(|i| get_sockaddr(i));

        let mut buckets = Vec::new();
        for upstream in upstream_hosts {
            buckets.push(Bucket::new(upstream, 1));
        }

        let c = Continuum::with_default_points(&buckets);

        assert_eq!(c.node(b"/some/path"), Some(get_sockaddr("127.0.0.1:7778")));
        assert_eq!(
            c.node(b"/some/longer/path"),
            Some(get_sockaddr("127.0.0.1:7777"))
        );
        assert_eq!(
            c.node(b"/sad/zaidoon"),
            Some(get_sockaddr("127.0.0.1:7778"))
        );
        assert_eq!(c.node(b"/g"), Some(get_sockaddr("127.0.0.1:7777")));
        assert_eq!(
            c.node(b"/pingora/team/is/cool/and/this/is/a/long/uri"),
            Some(get_sockaddr("127.0.0.1:7778"))
        );
        assert_eq!(
            c.node(b"/i/am/not/confident/in/this/code"),
            Some(get_sockaddr("127.0.0.1:7777"))
        );
    }

    #[test]
    fn matches_nginx_sample_data() {
        let upstream_hosts = [
            "10.0.0.1:443",
            "10.0.0.2:443",
            "10.0.0.3:443",
            "10.0.0.4:443",
            "10.0.0.5:443",
            "10.0.0.6:443",
            "10.0.0.7:443",
            "10.0.0.8:443",
            "10.0.0.9:443",
        ];
        let upstream_hosts = upstream_hosts.iter().map(|i| get_sockaddr(i));

        let mut buckets = Vec::new();
        for upstream in upstream_hosts {
            buckets.push(Bucket::new(upstream, 100));
        }

        let c = Continuum::with_default_points(&buckets);

        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test-data")
            .join("sample-nginx-upstream.csv");

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(path)
            .unwrap();

        for pair in rdr.records() {
            let pair = pair.unwrap();
            let uri = pair.get(0).unwrap();
            let upstream = pair.get(1).unwrap();

            let got = c.node(uri.as_bytes()).unwrap();
            assert_eq!(got, get_sockaddr(upstream));
        }
    }

    #[test]
    fn node_iter() {
        let upstream_hosts = ["127.0.0.1:7777", "127.0.0.1:7778", "127.0.0.1:7779"];
        let upstream_hosts = upstream_hosts.iter().map(|i| get_sockaddr(i));

        let mut buckets = Vec::new();
        for upstream in upstream_hosts {
            buckets.push(Bucket::new(upstream, 1));
        }

        let c = Continuum::with_default_points(&buckets);
        let mut iter = c.node_iter(b"doghash");
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));

        // drop 127.0.0.1:7777
        let upstream_hosts = ["127.0.0.1:7777", "127.0.0.1:7779"];
        let upstream_hosts = upstream_hosts.iter().map(|i| get_sockaddr(i));

        let mut buckets = Vec::new();
        for upstream in upstream_hosts {
            buckets.push(Bucket::new(upstream, 1));
        }

        let c = Continuum::with_default_points(&buckets);
        let mut iter = c.node_iter(b"doghash");
        // 127.0.0.1:7778 nodes are gone now
        // assert_eq!(iter.next(), Some("127.0.0.1:7778"));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7777")));
        // assert_eq!(iter.next(), Some("127.0.0.1:7778"));
        // assert_eq!(iter.next(), Some("127.0.0.1:7778"));
        assert_eq!(iter.next(), Some(&get_sockaddr("127.0.0.1:7779")));

        // assert infinite cycle
        let c = Continuum::with_default_points(&[Bucket::new(get_sockaddr("127.0.0.1:7777"), 1)]);
        let mut iter = c.node_iter(b"doghash");

        let start_idx = iter.idx;
        for _ in 0..c.ring.len() {
            assert!(iter.next().is_some());
        }
        // assert wrap around
        assert_eq!(start_idx, iter.idx);
    }

    #[test]
    fn test_empty() {
        let c = Continuum::with_default_points(&[]);
        assert!(c.node(b"doghash").is_none());

        let mut iter = c.node_iter(b"doghash");
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_ipv6_ring() {
        let upstream_hosts = ["[::1]:7777", "[::1]:7778", "[::1]:7779"];
        let upstream_hosts = upstream_hosts.iter().map(|i| get_sockaddr(i));

        let mut buckets = Vec::new();
        for upstream in upstream_hosts {
            buckets.push(Bucket::new(upstream, 1));
        }

        let c = Continuum::with_default_points(&buckets);
        let mut iter = c.node_iter(b"doghash");
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7778")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7777")));
        assert_eq!(iter.next(), Some(&get_sockaddr("[::1]:7779")));
    }
}
