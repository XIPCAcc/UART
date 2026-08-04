use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Benchmarks {
    total_start: Instant,
    single_start: Instant,
    minimum: Duration,
    maximum: Duration,
    sum: Duration,
    squared_sum: f64,
    count: usize,
    latencies: Vec<u64>,
    start_cpu_time: u64,
    end_cpu_time: u64,
}

impl Benchmarks {
    pub fn new() -> Self {
        Benchmarks {
            total_start: Instant::now(),
            single_start: Instant::now(),
            minimum: Duration::from_secs(u64::MAX),
            maximum: Duration::from_nanos(0),
            sum: Duration::from_nanos(0),
            squared_sum: 0.0,
            count: 0,
            latencies: Vec::new(),
            start_cpu_time: 0,
            end_cpu_time: 0,
        }
    }

    pub fn reset_total_start(&mut self) {
        self.total_start = Instant::now();
        self.start_cpu_time = self.get_cpu_time();
    }

    pub fn start_operation(&mut self) {
        self.single_start = Instant::now();
    }

    pub fn end_operation(&mut self) {
        let duration = self.single_start.elapsed();
        self.update(duration);
    }

    fn update(&mut self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;
        self.latencies.push(nanos);
        self.minimum = self.minimum.min(duration);
        self.maximum = self.maximum.max(duration);
        self.sum += duration;
        self.squared_sum += nanos as f64 * nanos as f64;
        self.count += 1;
    }

    fn get_cpu_time(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        let index = (sorted.len() as f64 * p / 100.0).floor() as usize;
        sorted[index.min(sorted.len() - 1)]
    }
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub message_count: usize,
    pub total_duration_ms: f64,
    pub average_duration_us: f64,
    pub minimum_duration_us: f64,
    pub maximum_duration_us: f64,
    pub standard_deviation_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub message_rate: f64,
    pub cpu_usage: f64,
}

impl Benchmarks {
    pub fn evaluate(&mut self) -> BenchmarkResult {
        let total_time = self.total_start.elapsed();
        let average = self.sum / (self.count as u32);

        let sigma = self.squared_sum / self.count as f64;
        let sigma = (sigma - (average.as_nanos() as f64).powi(2)).sqrt();

        let message_rate = (self.count as f64) / total_time.as_secs_f64();

        self.end_cpu_time = self.get_cpu_time();
        let cpu_time_used = self.end_cpu_time.saturating_sub(self.start_cpu_time);
        let cpu_usage = (cpu_time_used as f64 / total_time.as_nanos() as f64) * 100.0;

        let p50 = self.percentile(50.0);
        let p90 = self.percentile(90.0);
        let p99 = self.percentile(99.0);

        let total_time_ms = total_time.as_secs_f64() * 1000.0;
        let average_us = average.as_nanos() as f64 / 1000.0;
        let minimum_us = self.minimum.as_nanos() as f64 / 1000.0;
        let maximum_us = self.maximum.as_nanos() as f64 / 1000.0;
        let sigma_us = sigma / 1000.0;
        let p50_us = p50 as f64 / 1000.0;
        let p90_us = p90 as f64 / 1000.0;
        let p99_us = p99 as f64 / 1000.0;

        BenchmarkResult {
            message_count: self.count,
            total_duration_ms: total_time_ms,
            average_duration_us: average_us,
            minimum_duration_us: minimum_us,
            maximum_duration_us: maximum_us,
            standard_deviation_us: sigma_us,
            p50_us,
            p90_us,
            p99_us,
            message_rate,
            cpu_usage,
        }
    }

    pub fn print_results(&self, result: &BenchmarkResult) {
        println!("\n============ RESULTS ================");
        println!("Message count:      {}", result.message_count);
        println!("Total duration:     {:.6} ms", result.total_duration_ms);
        println!("Average duration:   {:.6} us", result.average_duration_us);
        println!("Minimum duration:   {:.6} us", result.minimum_duration_us);
        println!("Maximum duration:   {:.6} us", result.maximum_duration_us);
        println!("Standard deviation: {:.6} us", result.standard_deviation_us);
        println!("Latency P50:        {:.6} us", result.p50_us);
        println!("Latency P90:        {:.6} us", result.p90_us);
        println!("Latency P99:        {:.6} us", result.p99_us);
        println!("Message rate:       {:.0} msg/s", result.message_rate);
        println!("CPU usage:          {:.2}%", result.cpu_usage);
        println!("=====================================");
    }
}
