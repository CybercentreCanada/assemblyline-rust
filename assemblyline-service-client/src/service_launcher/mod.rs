//! Module for launching Assemblyline service process

use log::debug;
use tokio::process::{Child, Command};

use crate::types::errors::ServiceClientError;

/// Interface for launching the service process
pub trait ServiceLauncher {
    /// Launch service process
    /// :return: Child struct with process information and control
    fn launch_service(
        &self,
    ) -> impl std::future::Future<Output = Result<Child, ServiceClientError>> + Send;
}

/// Default service launcher
pub struct DefaultServiceLauncher {
    /// the directory that the service is running in
    pub service_dir: Option<String>,
}

impl ServiceLauncher for DefaultServiceLauncher {
    async fn launch_service(&self) -> Result<Child, ServiceClientError> {
        let mut cmd = Command::new("python3");
        cmd.args(["-m", "assemblyline_v4_service.run_service"]);

        if let Some(dir) = &self.service_dir {
            cmd.current_dir(dir);
        }

        let service_process = cmd.kill_on_drop(true).spawn()?;

        debug!("Service launched in the background");

        Ok(service_process)
    }
}
