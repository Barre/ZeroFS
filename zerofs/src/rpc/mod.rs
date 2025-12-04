pub mod client;
pub mod convert;
pub mod server;

pub mod proto {
    tonic::include_proto!("zerofs.admin");
}
