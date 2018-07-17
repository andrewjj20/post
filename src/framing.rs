
struct Msg {
    version: u16,
    msg_type: u16,
    flags: u32,
}

struct DataMsg {
    generation: u64,
    start: usize,
    packet_size: usize,
    complete_size: usize,
}

