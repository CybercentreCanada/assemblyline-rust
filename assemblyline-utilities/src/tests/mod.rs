#[cfg(test)]
pub(crate) mod mock_api_server;

#[cfg(test)]
pub(crate) mod test_connection;

pub fn init() {
    // let _ = env_logger::builder()
    //     .target(env_logger::Target::Stdout)
    //     .filter_level(log::LevelFilter::Trace)
    //     .is_test(true)
    //     .try_init();
}
