// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::connection::PubSubDataSource;
use crate::connection::PubSubDataSourceT;
use crate::reader::DataSetReader;
#[cfg(feature = "server-integration")]
use opcua_server::prelude::*;
use opcua_types::status_code::StatusCode;
use opcua_types::string::UAString;
use opcua_types::Guid;
use opcua_types::VariantTypeId;
use opcua_types::{AttributeId, DateTime, DeadbandType, LocalizedText};
use opcua_types::{ConfigurationVersionDataType, PublishedVariableDataType};
use opcua_types::{DataValue, FieldMetaData, Variant};
use opcua_types::{FieldTargetDataType, NodeId, OverrideValueHandling};
use std::convert::TryFrom;
use std::sync::{Arc, RwLock};

pub struct Promoted(pub bool);
/// Trait to implement for data access
pub trait DataSetInfo {
    fn collect_values(&self, name: &UAString) -> Vec<(Promoted, DataValue)>;
    fn get_config_version(&self, name: &UAString) -> ConfigurationVersionDataType;
}
/// Published Dataset, connects server variables to data writer
pub struct PublishedDataSet {
    pub name: UAString,
    config_version: ConfigurationVersionDataType,
    dataset_fields: Vec<DataSetField>,
}
/// Configures one variable in a dataset
#[allow(dead_code)]
pub struct DataSetField {
    field_name_alias: UAString,
    config_version: ConfigurationVersionDataType,
    promoted_field: bool,
    published_variable_cfg: PublishedVariableDataType,
}

/// Build a variable for a dataset
pub struct DataSetFieldBuilder {
    alias: UAString,
    promoted: bool,
    published_variable: NodeId,
}

fn create_empty_field_meta_data() -> FieldMetaData {
    FieldMetaData {
        name: UAString::null(),
        description: LocalizedText::null(),
        field_flags: opcua_types::DataSetFieldFlags::None,
        built_in_type: 0,
        data_type: NodeId::null(),
        value_rank: -1, // Scalar
        array_dimensions: None,
        max_string_length: 0,
        data_set_field_id: Guid::new(), // always generate new guid for each fmd
        properties: None,
    }
}

/// Wraps FieldMetaData Type
pub struct PubSubFieldMetaData(FieldMetaData);
impl PubSubFieldMetaData {
    pub fn name(&self) -> &UAString {
        &self.0.name
    }

    pub fn data_set_field_id(&self) -> &Guid {
        &self.0.data_set_field_id
    }
    /// Generates a the configuration from a existing server variable
    #[cfg(feature = "server-integration")]
    pub fn new_from_var(var: &Variable, dt: &DataTypeId) -> Self {
        let mut fmd = create_empty_field_meta_data();
        fmd.name = var.display_name().text;
        fmd.description = var.description().unwrap_or(LocalizedText::null());
        fmd.data_type = dt.into();
        fmd.built_in_type = VariantTypeId::try_from(&fmd.data_type)
            .unwrap_or(VariantTypeId::ExtensionObject)
            .precedence();
        fmd.value_rank = var.value_rank();
        fmd.array_dimensions = var.array_dimensions();
        PubSubFieldMetaData(fmd)
    }
}

pub struct PubSubFieldMetaDataBuilder {
    data: FieldMetaData,
}

impl PubSubFieldMetaDataBuilder {
    pub fn new() -> Self {
        PubSubFieldMetaDataBuilder {
            data: create_empty_field_meta_data(),
        }
    }

    pub fn name(mut self, name: UAString) -> Self {
        self.data.name = name;
        self
    }

    pub fn description(mut self, desc: LocalizedText) -> Self {
        self.data.description = desc;
        self
    }

    pub fn data_type(mut self, dt: &DataTypeId) -> Self {
        self.data.data_type = dt.into();
        self.data.built_in_type = VariantTypeId::try_from(&self.data.data_type)
            .unwrap_or(VariantTypeId::ExtensionObject)
            .precedence();
        self
    }

    pub fn value_rank(mut self, value_rank: i32) -> Self {
        self.data.value_rank = value_rank;
        self
    }

    pub fn array_dimensions(mut self, dims: Option<Vec<u32>>) -> Self {
        self.data.array_dimensions = dims;
        self
    }

    pub fn build(self) -> PubSubFieldMetaData {
        PubSubFieldMetaData(self.data)
    }

    pub fn insert(self, reader: &mut DataSetReader) -> Guid{
        let guid = self.data.data_set_field_id.clone();
        reader.add_field(PubSubFieldMetaData(self.data));
        return guid;
    }
}

impl DataSetField {
    /// converts data from a source to datavalue
    pub fn get_data(&self, addr: &dyn PubSubDataSource) -> (Promoted, DataValue) {
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

impl DataSetFieldBuilder {
    pub fn new() -> Self {
        DataSetFieldBuilder {
            alias: UAString::null(),
            promoted: false,
            published_variable: NodeId::null(),
        }
    }
    /// Sets the nodeid for the variable tobe read
    pub fn set_target_variable<'a>(&'a mut self, node_id: NodeId) -> &'a mut Self {
        self.published_variable = node_id;
        self
    }
    /// Sets the alias name
    pub fn set_alias<'a>(&'a mut self, alias: UAString) -> &'a mut Self {
        self.alias = alias;
        self
    }
    /// Sets if the field is promoted, the purpose of promoted fields is filtering,
    /// because they are always send in plaintext. On the other hand only promoted
    /// can send if only one dataset is send per message. Either set WriterGroup ording
    /// or only use one dataset writer per writergroup
    pub fn set_promoted<'a>(&'a mut self, promoted: bool) -> &'a mut Self {
        self.promoted = promoted;
        self
    }

    fn create_cfg(&self) -> PublishedVariableDataType {
        PublishedVariableDataType {
            published_variable: self.published_variable.clone(),
            attribute_id: AttributeId::Value as u32,
            sampling_interval_hint: 0.0,
            deadband_type: DeadbandType::None as u32,
            deadband_value: 0.0,
            index_range: UAString::null(),
            substitute_value: Variant::Empty,
            meta_data_properties: None,
        }
    }
    /// insert a field
    pub fn insert(&self, pds: &mut PublishedDataSet) {
        let cfg = self.create_cfg();
        let dsf = DataSetField {
            field_name_alias: self.alias.clone(),
            config_version: ConfigurationVersionDataType {
                minor_version: generate_version_time(),
                major_version: generate_version_time(),
            },
            promoted_field: self.promoted,
            published_variable_cfg: cfg,
        };
        pds.add_field(dsf);
    }
}

/// Generates the VersionTime, VersionTime is seconds since 1.1.2020. Always use the current time as value
pub fn generate_version_time() -> u32 {
    (DateTime::now().as_chrono().timestamp() - DateTime::ymd(2000, 1, 1).as_chrono().timestamp())
        as u32
}

impl PublishedDataSet {
    pub fn new(name: UAString) -> Self {
        PublishedDataSet {
            name,
            config_version: ConfigurationVersionDataType {
                minor_version: generate_version_time(),
                major_version: generate_version_time(),
            },
            dataset_fields: Vec::new(),
        }
    }

    pub fn get_name(&self) -> UAString {
        self.name.clone()
    }

    pub fn get_config_version(&self) -> ConfigurationVersionDataType {
        self.config_version.clone()
    }

    pub fn add_field(&mut self, dsf: DataSetField) {
        self.dataset_fields.push(dsf);
    }

    pub fn get_data(&self, addr: &dyn PubSubDataSource) -> Vec<(Promoted, DataValue)> {
        self.dataset_fields
            .iter()
            .map(|d| d.get_data(addr))
            .collect()
    }
}

/// DataSetTarget wraps FiedlTargetDataType
pub struct DataSetTarget(pub FieldTargetDataType);

impl DataSetTarget {
    pub fn update_dv(&self, dv: DataValue) -> Result<DataValue, StatusCode> {
        //@TODO implement correct handle override values usw.
        Ok(dv)
    }

    #[cfg(feature = "server-integration")]
    pub fn update_variable(&self, dv: DataValue, var: &mut Variable) -> Result<(), StatusCode> {
        //@TODO implement correct handle override values usw.
        var.set_value_direct(
            dv.value.unwrap_or_default(),
            dv.status.unwrap_or(StatusCode::Good),
            &dv.server_timestamp.unwrap_or(DateTime::now()),
            &dv.source_timestamp.unwrap_or(DateTime::now()),
        )
    }
}

pub struct DataSetTargetBuilder {
    data: FieldTargetDataType,
}

impl DataSetTargetBuilder {
    /// Generates build from an guid of field metadata
    pub fn new_from_guid(guid: Guid) -> Self{
        DataSetTargetBuilder {
            data: FieldTargetDataType {
                data_set_field_id: guid,
                receiver_index_range: UAString::null(),
                target_node_id: NodeId::null(),
                attribute_id: AttributeId::Value as u32,
                write_index_range: UAString::null(),
                override_value_handling: OverrideValueHandling::LastUsableValue,
                override_value: Variant::Empty,
            },
        }
    }
    /// Needs guid from field metadata for linking of read values to the variable
    pub fn new(ds: &PubSubFieldMetaData) -> Self {
        Self::new_from_guid(ds.data_set_field_id().clone())
    }

    pub fn writer_index_range<'a>(&'a mut self, range: &UAString) -> &'a mut Self {
        self.data.write_index_range = range.clone();
        self
    }

    pub fn reader_index_range<'a>(&'a mut self, range: &UAString) -> &'a mut Self {
        self.data.write_index_range = range.clone();
        self
    }

    pub fn target_node_id<'a>(&'a mut self, target: &NodeId) -> &'a mut Self {
        self.data.target_node_id = target.clone();
        self
    }

    pub fn attribute_id<'a>(&'a mut self, attr: AttributeId) -> &'a mut Self {
        self.data.attribute_id = attr as u32;
        self
    }

    pub fn override_value_handling<'a>(
        &'a mut self,
        ovhandling: OverrideValueHandling,
    ) -> &'a mut Self {
        self.data.override_value_handling = ovhandling;
        self
    }

    pub fn override_value<'a>(&'a mut self, var: Variant) -> &'a mut Self {
        self.data.override_value = var;
        self
    }

    pub fn build(&self) -> DataSetTarget {
        DataSetTarget(self.data.clone())
    }

    pub fn insert(&self, reader: &mut DataSetReader) {
        reader.sub_data_set().add_target(DataSetTarget(self.data.clone()))
    }
}

/// Contains the information what todo with variables
pub struct SubscribedDataSet {
    targets: Vec<DataSetTarget>,
}

pub struct UpdateTarget<'a>(pub Guid, pub DataValue, pub &'a PubSubFieldMetaData);

impl SubscribedDataSet {
    pub fn new() -> Self {
        SubscribedDataSet {
            targets: Vec::new(),
        }
    }

    pub fn add_target(&mut self, target: DataSetTarget) {
        self.targets.push(target);
    }

    pub fn update_targets(
        &self,
        data: Vec<UpdateTarget>,
        data_source: &Arc<RwLock<PubSubDataSourceT>>,
    ) {
        let mut source = data_source.write().unwrap();
        for UpdateTarget(guid, value, meta) in data {
            if let Some(t) = self.targets.iter().find(|t| t.0.data_set_field_id == guid) {
                source.set_pubsub_value(t, value, meta);
            }
        }
    }
}
