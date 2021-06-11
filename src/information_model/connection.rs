use opcua_server::prelude::*;


pub struct PubSubConnectionModel{

}


impl PubSubConnectionModel{
    pub fn new(nid: &NodeId, cfg: PubSubConnectionDataType, addr: &mut AddressSpace) -> Self{
        
        ObjectBuilder::new(nid,
             QualifiedName::new(0, cfg.name.to_string()),
             LocalizedText::from(cfg.name.to_string()))
             .organized_by(NodeId::new(0, 14443))
             .has_type_definition(ObjectTypeId::PubSubConnectionType)
             .insert(addr);

        addr.insert_reference(&NodeId::new(0, 14443), nid, ReferenceTypeId::HasPubSubConnection);        
        VariableBuilder::new(
            &NodeId::next_numeric(0),
            QualifiedName::new(0, "PublisherId"), 
            LocalizedText::new("en", "PublisherId"))
            .property_of(nid)
            .data_type(DataTypeId::BaseDataType)
            .has_type_definition(VariableTypeId::PropertyType)
            .value(cfg.publisher_id)
            .insert(addr);
        
        Self{}
    }    
}