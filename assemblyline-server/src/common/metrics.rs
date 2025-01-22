
/// Path where we expect to find cgroup cpu information within a container
const STAT_PATH: &str = "/sys/fs/cgroup/cpu.stat";

/// Load cpu load information from the cgroup fs interface or return 0
async fn load_cgroup_cpu_usage() -> u64 {
    let body = match tokio::fs::read_to_string(&STAT_PATH).await {
        Ok(body) => body,
        Err(_) => return 0,
    };

    for line in body.lines() {
        if line.starts_with("usage_usec") {
            return match &line[11..].parse() {
                Ok(value) => *value,
                Err(_) => 0,
            }
        }
    }
    0
}


pub struct CPUTracker {
    last_reading: f64,
}

impl CPUTracker {
    pub async fn new() -> Self {
        Self {
            last_reading: Self::read_instant().await
        }
    }

    async fn read_instant() -> f64 {
        (load_cgroup_cpu_usage().await as f64)/1_000_000.0f64
    }

    /// Reports the seconds of cpu time used since the previous call
    pub async fn read(&mut self) -> f64 {
        let previous = self.last_reading;
        self.last_reading = Self::read_instant().await;
        self.last_reading - previous
    }
} 
