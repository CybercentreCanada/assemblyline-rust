use struct_metadata::Described;

use crate::ElasticMeta;



// /// Validated ip type
// #[derive(SerializeDisplay, DeserializeFromStr, Described, PartialEq, Eq, Debug, Clone)]
// #[metadata_type(ElasticMeta)]
// #[metadata(mapping="ip")]
// pub struct IP(std::net::IpAddr);


impl Described<ElasticMeta> for std::net::IpAddr {
    fn metadata() -> struct_metadata::Descriptor<ElasticMeta> {
        struct_metadata::Descriptor { 
            docs: None, 
            metadata: ElasticMeta {
                mapping: Some("ip"),
                ..Default::default()
            }, 
            kind: struct_metadata::Kind::String
        }
    }
}

// impl std::fmt::Display for IP {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str(&self.0)
//     }
// }

// impl FromStr for IP {
//     type Err;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         todo!()
//     }
// }

