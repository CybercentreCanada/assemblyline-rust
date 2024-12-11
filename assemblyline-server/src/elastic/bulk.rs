// import json
// import typing
// from copy import deepcopy

use std::marker::PhantomData;

use serde::Serialize;
use serde_json::json;


pub struct TypedBulkPlan<T> {
    indexes: Vec<String>,
    operations: Vec<String>,
    _phantom: PhantomData<T>
}

impl<T: Serialize> TypedBulkPlan<T> {
    pub (super) fn new(indexes: Vec<String>) -> Self {
        Self {
            indexes,
            operations: vec![],
            _phantom: Default::default(),
        }
    }

    // @property
    // def empty(self):
    //     return len(self.operations) == 0

    // def add_delete_operation(self, doc_id, index=None):
    //     if index:
    //         self.operations.append(json.dumps({"delete": {"_index": index, "_id": doc_id}}))
    //     else:
    //         for cur_index in self.indexes:
    //             self.operations.append(json.dumps({"delete": {"_index": cur_index, "_id": doc_id}}))

    // def add_insert_operation(self, doc_id, doc, index=None):
    //     if self.model and isinstance(doc, self.model):
    //         saved_doc = doc.as_primitives(hidden_fields=True)
    //     elif self.model:
    //         saved_doc = self.model(doc).as_primitives(hidden_fields=True)
    //     else:
    //         if not isinstance(doc, dict):
    //             saved_doc = {'__non_doc_raw__': doc}
    //         else:
    //             saved_doc = deepcopy(doc)
    //     saved_doc['id'] = doc_id

    //     self.operations.append(json.dumps({"create": {"_index": index or self.indexes[0], "_id": doc_id}}))
    //     self.operations.append(json.dumps(saved_doc))

    pub fn add_upsert_operation(&mut self, doc_id: &str, doc: &T, index: Option<String>) -> Result<(), super::ElasticError> { // , index=None
        // if self.model and isinstance(doc, self.model):
        //     saved_doc = doc.as_primitives(hidden_fields=True)
        // elif self.model:
        //     saved_doc = self.model(doc).as_primitives(hidden_fields=True)
        // else:
        //     if not isinstance(doc, dict):
        //         saved_doc = {'__non_doc_raw__': doc}
        //     else:
        //         saved_doc = deepcopy(doc)
        let mut saved_doc = serde_json::to_value(doc)?;
        if let Some(obj) = saved_doc.as_object_mut() {
            obj.insert("id".to_string(), json!(doc_id));
        }

        let indices = if let Some(index) = index {
            &[index]
        } else {
            &self.indexes[..]
        };

        self.operations.push(serde_json::to_string(&json!({"update": {"_index": indices[0], "_id": doc_id}}))?);
        self.operations.push(serde_json::to_string(&json!({"doc": saved_doc, "doc_as_upsert": true}))?);
        Ok(())
    }

    // def add_update_operation(self, doc_id, doc, index=None):

    //     if self.model and isinstance(doc, self.model):
    //         saved_doc = doc.as_primitives(hidden_fields=True)
    //     elif self.model:
    //         saved_doc = self.model(doc, mask=list(doc.keys())).as_primitives(hidden_fields=True)
    //     else:
    //         if not isinstance(doc, dict):
    //             saved_doc = {'__non_doc_raw__': doc}
    //         else:
    //             saved_doc = deepcopy(doc)

    //     if index:
    //         self.operations.append(json.dumps({"update": {"_index": index, "_id": doc_id}}))
    //         self.operations.append(json.dumps({"doc": saved_doc}))
    //     else:
    //         for cur_index in self.indexes:
    //             self.operations.append(json.dumps({"update": {"_index": cur_index, "_id": doc_id}}))
    //             self.operations.append(json.dumps({"doc": saved_doc}))

    pub fn get_plan_data(&self) -> String {
        return self.operations.join("\n") + "\n"
    }
}