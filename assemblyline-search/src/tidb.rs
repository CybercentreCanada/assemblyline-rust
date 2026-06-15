use anyhow::Result;
use log::{debug, info};

use crate::tables::Table;


pub struct TiDb {

}

impl TiDb {
    pub async fn connect(url: &str) -> Result<Self> {
        todo!()
    }

    pub async fn development(random_database: bool) -> Result<Self> {
        todo!()
    }

    pub async fn create_table(&self, table: &Table, wipe: bool) -> Result<()> {
        info!("Creating table {} ...", table.name);
        let (create_table, create_indices) = table.create_table_command();
        debug!("{create_table}");
        if wipe {
            self.client.execute(&format!("drop table if exists {}", table.name), &[]).await?;
        }
        self.client.execute(&create_table, &[]).await?;

        for create_index in create_indices {
            debug!("{create_index}");
            self.client.execute(&create_index, &[]).await?;
        }
        Ok(())
    }
}