pub mod net;
pub mod shm;
pub mod uds;

/// Version of the worker-facing IPC contract: the shm/slot layouts, the
/// control-packet fields, and the tcp-raw wire format, as one number. Carried
/// in every config packet so a worker built against a different contract can
/// refuse to attach instead of misreading shared memory. Bump it on any
/// change a deployed worker could not survive.
pub const IPC_CONTRACT_VERSION: u32 = 1;
