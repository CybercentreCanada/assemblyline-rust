use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use self::queue::PriorityQueue;
pub use self::queue::Queue;
pub use self::quota::QuotaGuard;
pub use self::events::EventWatcher;
pub use self::hashmap::Hashmap;

mod queue;
mod quota;
mod events;
mod hashmap;

pub struct StructureStore {

}

impl StructureStore {

    pub fn priority_queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> PriorityQueue<T> {
        PriorityQueue::new(name, self.clone())
    }

    pub fn queue<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> Queue<T> {
        Queue::new(name, self.clone())
    }

    pub fn hashmap<T: Serialize + DeserializeOwned>(self: &Arc<Self>, name: String) -> Hashmap<T> {
        Hashmap::new(name, self.clone())
    }
}