//! Messages about configuration changes internal to assemblyline.

use serde::{Deserialize, Serialize};

#[derive(Debug, strum::FromRepr, strum::EnumIs, PartialEq, Eq, Deserialize, Clone, Copy)]
#[repr(u8)]
pub enum Operation {
    Added = 1,
    Removed = 2,
    Modified = 3,
    Incompatible = 4
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer {
        (*self as u8).serialize(serializer)
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceChange {
    pub name: String,
    pub operation: Operation,
}


// @dataclass
// class SignatureChange:
//     signature_id: str
//     signature_type: str
//     source: str
//     operation: Operation

//     @staticmethod
//     def serialize(obj: SignatureChange) -> str:
//         return json.dumps(asdict(obj))

//     @staticmethod
//     def deserialize(data: str) -> SignatureChange:
//         return SignatureChange(**json.loads(data))