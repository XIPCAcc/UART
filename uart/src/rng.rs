/// Simple LCG pseudo-random generator (no rand crate needed)
pub struct Lcg {
    state: u32,
}

impl Lcg {
    pub fn new(seed: u32) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_mul(1664525).wrapping_add(1013904223);
        self.state
    }

    pub fn next_f32(&mut self) -> f32 {
        (self.next_u32() & 0x007FFFFF) as f32 / 8388608.0
    }
}