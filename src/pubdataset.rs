// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2020 Alexander Schrode
use opcua_types::{ConfigurationVersionDataType, PublishedVariableDataType};
use opcua_types::NodeId;
use opcua_types::status_code::StatusCode;
use opcua_types::{Variant, DataValue};
use opcua_types::{AttributeId, DeadbandType, DateTime};
use opcua_types::string::UAString;
use crate::connection::PubSubDataSource;

pub struct Promoted(pub bool);

pub trait DataSetInfo{
    fn collect_values(&self, name: &UAString) -> Vec::<(Promoted, DataValue)>;
    fn get_config_version(&self, name: &UAString) -> ConfigurationVersionDataType;
}

pub struct PublishedDataSet{
    pub name: UAString,
    config_version: ConfigurationVersionDataType,
    dataset_fields: Vec::<DataSetField>,
}
#[allow(dead_code)]
pub struct DataSetField{
    field_name_alias: UAString,
    config_version: ConfigurationVersionDataType,
    promoted_field: bool,
    published_variable_cfg: PublishedVariableDataType,
}



pub struct DataSetFieldBuilder{
    alias: UAString,
    promoted: bool,
    published_variable: NodeId,
}

impl DataSetField{
    pub fn get_data(&self, addr: & dyn PubSubDataSource ) -> (Promoted, DataValue){
        let v = addr.get_pubsub_value(&self.published_variable_cfg.published_variable);
        match v {
            Ok(dv) => (Promoted(self.promoted_field), dv),
            Err(_) => {
                let mut dv = DataValue::null();
                dv.status = Some(StatusCode::BadNodeIdInvalid);
                (Promoted(self.promoted_field), dv)
            } 
        }
    }
}

impl DataSetFieldBuilder{
    pub fn new() -> Self{
        DataSetFieldBuilder{ alias: UAString::null(), promoted: false, published_variable: NodeId::null() }
    }

    pub fn set_target_variable<'a>(&'a mut self, node_id: NodeId) -> &'a mut Self{
        self.published_variable = node_id;
        self
    }

    pub fn set_alias<'a>(&'a mut self, alias: UAString) -> &'a mut Self{
        self.alias = alias;
        self
    }

    pub fn set_promoted<'a>(&'a mut self, promoted: bool) -> &'a mut Self{
        self.promoted = promoted;
        self
    }

    fn create_cfg(&self) -> PublishedVariableDataType{
        PublishedVariableDataType{
            published_variable: self.published_variable.clone(),
            attribute_id: AttributeId::Value as u32,
            sampling_interval_hint: 0.0,
            deadband_type: DeadbandType::None as u32,
            deadband_value: 0.0,
            index_range: UAString::null(),
            substitute_value: Variant::Empty,
            meta_data_properties: None
        }
    }

    pub fn insert(&self, pds: &mut PublishedDataSet){
        let cfg = self.create_cfg();
        let dsf = DataSetField{
                field_name_alias: self.alias.clone(),
                config_version: ConfigurationVersionDataType{minor_version: generate_version_time(), major_version: generate_version_time()},
                promoted_field: self.promoted,
                published_variable_cfg: cfg
            };
        pds.add_field(dsf);
    }
}

pub fn generate_version_time() -> u32{
    (DateTime::now().as_chrono().timestamp() - DateTime::ymd(2000, 1, 1).as_chrono().timestamp()) as u32
}

impl PublishedDataSet{
    pub fn new(name: UAString) -> Self{
        PublishedDataSet{
            name, config_version: ConfigurationVersionDataType{minor_version: generate_version_time(), major_version: generate_version_time()},
            dataset_fields: Vec::new(),
        }
    }

    pub fn get_name(&self) -> UAString{
        self.name.clone()
    }

    pub fn get_config_version(&self) -> ConfigurationVersionDataType{
        self.config_version.clone()
    }

    pub fn add_field(&mut self, dsf: DataSetField){
        self.dataset_fields.push(dsf);
    }

    pub fn get_data(&self, addr: &dyn PubSubDataSource) -> Vec::<(Promoted, DataValue)>{
        self.dataset_fields.iter().map(|d| d.get_data(addr)).collect()
    }
}
