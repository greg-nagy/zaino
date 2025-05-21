use std::{collections::HashMap, mem, sync::Arc};

use crate::chain_index::types::{Hash, Height};
use tokio::sync::{mpsc, RwLock};

use crate::ChainBlock;

/// Holds the block cache
struct ConcurrentBlockCache {
    staged: RwLock<mpsc::UnboundedReceiver<ChainBlock>>,
    staging_sender: mpsc::UnboundedSender<ChainBlock>,
    /// This lock should not be exposed to consumers. Rather,
    /// clone the Arc and offer that. This means we can overwrite the arc
    /// without interfering with readers, who will hold a stale copy
    current: RwLock<Arc<BlockCacheSnapshot>>,
}

pub(crate) struct BlockCacheSnapshot {
    blocks: HashMap<Hash, ChainBlock>,
    // Do we need height here?
    best_tip: (Hash, Height),
}

/// This is the core of the concurrent block cache.
impl ConcurrentBlockCache {
    pub async fn update(&self, finalized_height: Height) -> Result<(), ()> {
        let mut new = HashMap::<Hash, ChainBlock>::new();
        let mut staged = self.staged.write().await;
        loop {
            match staged.try_recv() {
                Ok(chain_block) => {
                    new.insert(*chain_block.index().hash(), chain_block);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Err(()),
            }
        }
        // at this point, we've collected everything in the staging area
        // we can drop the stage lock, and more blocks can be staged while we finish setting current
        mem::drop(staged);
        let snapshot = self.get_snapshot().await;
        new.extend(
            snapshot
                .blocks
                .iter()
                .map(|(hash, block)| (hash.clone(), block.clone())),
        );
        let (newly_finalzed, blocks): (HashMap<_, _>, HashMap<Hash, _>) = new
            .into_iter()
            .partition(|(_hash, block)| match block.index().height() {
                Some(height) => height <= finalized_height,
                None => false,
            });
        let best_tip = blocks.iter().fold(snapshot.best_tip, |acc, (hash, block)| {
            match block.index().height() {
                Some(height) if height > acc.1 => ((*hash).clone(), height),
                _ => acc,
            }
        });
        // Need to get best hash at some point in this process
        *self.current.write().await = Arc::new(BlockCacheSnapshot { blocks, best_tip });

        Ok(())
    }

    pub async fn get_snapshot(&self) -> Arc<BlockCacheSnapshot> {
        self.current.read().await.clone()
    }
}
