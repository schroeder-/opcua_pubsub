// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::address_space::{PubSubDataSource, PubSubDataSourceT};
use crate::reader::DataSetReader;
use crate::until::decode_extension;
use log::{error, warn};
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

/// Id for a pubsubconnection
#[derive(Debug, PartialEq, Clone)]
pub struct PublishedDataSetId(pub u32);

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
    dataset_id: PublishedDataSetId,
    guid: Guid,
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
    pub const fn name(&self) -> &UAString {
        &self.0.name
    }

    pub const fn new(cfg: FieldMetaData) -> Self {
        Self(cfg)
    }
    pub const fn data_set_field_id(&self) -> &Guid {
        &self.0.data_set_field_id
    }

    pub const fn get_meta(&self) -> &FieldMetaData {
        &self.0
    }
    /// Generates a the configuration from a existing server variable
    #[cfg(feature = "server-integration")]
    pub fn new_from_var(var: &Variable, dt: &DataTypeId) -> Self {
        let mut fmd = create_empty_field_meta_data();
        fmd.name = var.display_name().text;
        fmd.description = var.description().unwrap_or_else(LocalizedText::null);
        fmd.data_type = dt.into();
        fmd.built_in_type = VariantTypeId::try_from(&fmd.data_type)
            .unwrap_or(VariantTypeId::ExtensionObject)
            .precedence();
        fmd.value_rank = var.value_rank();
        fmd.array_dimensions = var.array_dimensions();
        Self(fmd)
    }
}

pub struct PubSubFieldMetaDataBuilder {
    data: FieldMetaData,
}

impl PubSubFieldMetaDataBuilder {
    pub fn new() -> Self {
        Self {
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

    pub fn insert(self, reader: &mut DataSetReader) -> Guid {
        let guid = self.data.data_set_field_id.clone();
        reader.add_field(PubSubFieldMetaData(self.data));
        guid
    }
}

impl Default for PubSubFieldMetaDataBuilder {
    fn default() -> Self {
        Self::new()
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
        Self {
            alias: UAString::null(),
            promoted: false,
            published_variable: NodeId::null(),
        }
    }
    /// Sets the nodeid for the variable tobe read
    pub fn set_target_variable(&mut self, node_id: NodeId) -> &mut Self {
        self.published_variable = node_id;
        self
    }
    /// Sets the alias name
    pub fn set_alias(&mut self, alias: UAString) -> &mut Self {
        self.alias = alias;
        self
    }
    /// Sets if the field is promoted, the purpose of promoted fields is filtering,
    /// because they are always send in plaintext. On the other hand only promoted
    /// can send if only one dataset is send per message. Either set WriterGroup ording
    /// or only use one dataset writer per writergroup
    pub fn set_promoted(&mut self, promoted: bool) -> &mut Self {
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

impl Default for DataSetFieldBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Generates the VersionTime, VersionTime is seconds since 1.1.2020. Always use the current time as value
pub fn generate_version_time() -> u32 {
    (DateTime::now().as_chrono().timestamp() - DateTime::ymd(2000, 1, 1).as_chrono().timestamp())
        as u32
}

impl PublishedDataSet {
    pub fn new(name: UAString) -> Self {
        Self {
            name,
            config_version: ConfigurationVersionDataType {
                minor_version: generate_version_time(),
                major_version: generate_version_time(),
            },
            dataset_fields: Vec::new(),
            dataset_id: PublishedDataSetId(0),
            guid: Guid::new(),
        }
    }
    pub fn generate_meta_data(&self) -> DataSetMetaDataType {
        DataSetMetaDataType {
            namespaces: None,
            structure_data_types: None,
            enum_data_types: None,
            simple_data_types: None,
            name: self.name.clone(),
            description: "".into(), // @TODO add description
            fields: None,           //self.dataset_fields.iter().map(|f| ),
            // @TODO generate meta data
            data_set_class_id: self.guid.clone(),
            configuration_version: self.config_version.clone(),
        }
    }

    pub fn update(&mut self, cfg: &PublishedDataSetDataType) -> Result<(), StatusCode> {
        self.name = cfg.name.clone();
        self.config_version = cfg.data_set_meta_data.configuration_version.clone();
        self.guid = cfg.data_set_meta_data.data_set_class_id.clone();
        if let Some(meta) = &cfg.data_set_meta_data.fields {
            let pds = if let Ok(pd) = decode_extension::<PublishedDataItemsDataType>(
                &cfg.data_set_source,
                ObjectId::PublishedDataItemsDataType_Encoding_DefaultBinary,
                &DecodingOptions::default(),
            ) {
                pd.published_data.unwrap_or_default()
            } else if let Ok(ev) = decode_extension::<PublishedEventsDataType>(
                &cfg.data_set_source,
                ObjectId::PublishedEventsDataType_Encoding_DefaultBinary,
                &DecodingOptions::default(),
            ) {
                error!(
                    "PublishedEventsDataType not implemented {}",
                    ev.event_notifier
                );
                return Err(StatusCode::BadNotImplemented);
            } else {
                warn!("PublishedDataSet config incomplete no know Subtype of PublishedDataSetSourceDataType found");
                return Err(StatusCode::BadInvalidArgument);
            };
            //@TODO sperate publisherdata and meta
            self.dataset_fields = meta
                .iter()
                .zip(pds)
                .map(|(x, y)| DataSetField {
                    field_name_alias: x.name.clone(),
                    config_version: ConfigurationVersionDataType {
                        major_version: 0,
                        minor_version: 0,
                    },
                    promoted_field: x.field_flags == DataSetFieldFlags::PromotedField,
                    published_variable_cfg: y,
                })
                .collect();
        }
        Ok(())
    }

    pub fn generate_cfg(&self) -> Result<PublishedDataSetDataType, StatusCode> {
        let meta = self.generate_meta_data();

        let pds = {
            PublishedDataItemsDataType {
                published_data: Some(
                    self.dataset_fields
                        .iter()
                        .map(|x| x.published_variable_cfg.clone())
                        .collect(),
                ),
            }
        };
        let source = ExtensionObject::from_encodable(
            ObjectId::PublishedEventsDataType_Encoding_DefaultBinary,
            &pds,
        );
        Ok(PublishedDataSetDataType {
            name: self.name.clone(),
            data_set_folder: None,
            data_set_meta_data: meta,
            extension_fields: None,
            data_set_source: source,
        })
    }

    pub fn from_cfg(cfg: &PublishedDataSetDataType) -> Result<Self, StatusCode> {
        let mut s = Self::new(cfg.name.clone());
        s.update(cfg)?;
        Ok(s)
    }

    pub fn name(&self) -> UAString {
        self.name.clone()
    }

    pub fn config_version(&self) -> ConfigurationVersionDataType {
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
    pub const fn id(&self) -> &PublishedDataSetId {
        &self.dataset_id
    }

    // @Fixme change creation?
    pub(crate) fn set_id(&mut self, id: PublishedDataSetId) {
        self.dataset_id = id;
    }
}

/// DataSetTarget wraps FiedlTargetDataType
pub struct DataSetTarget(pub FieldTargetDataType);

impl DataSetTarget {
    pub const fn update_dv(&self, dv: DataValue) -> Result<DataValue, StatusCode> {
        //@TODO implement correct handle override values usw.
        Ok(dv)
    }

    #[cfg(feature = "server-integration")]
    pub fn update_variable(&self, dv: DataValue, var: &mut Variable) -> Result<(), StatusCode> {
        //@TODO implement correct handle override values usw.
        var.set_value_direct(
            dv.value.unwrap_or_default(),
            dv.status.unwrap_or(StatusCode::Good),
            &dv.server_timestamp.unwrap_or_else(DateTime::now),
            &dv.source_timestamp.unwrap_or_else(DateTime::now),
        )
    }
}

pub struct DataSetTargetBuilder {
    data: FieldTargetDataType,
}

impl DataSetTargetBuilder {
    /// Generates build from an guid of field metadata
    pub fn new_from_guid(guid: Guid) -> Self {
        Self {
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

    pub fn writer_index_range(&mut self, range: &UAString) -> &mut Self {
        self.data.write_index_range = range.clone();
        self
    }

    pub fn reader_index_range(&mut self, range: &UAString) -> &mut Self {
        self.data.write_index_range = range.clone();
        self
    }

    pub fn target_node_id(&mut self, target: &NodeId) -> &mut Self {
        self.data.target_node_id = target.clone();
        self
    }

    pub fn attribute_id(&mut self, attr: AttributeId) -> &mut Self {
        self.data.attribute_id = attr as u32;
        self
    }

    pub fn override_value_handling(&mut self, ovhandling: OverrideValueHandling) -> &mut Self {
        self.data.override_value_handling = ovhandling;
        self
    }

    pub fn override_value(&mut self, var: Variant) -> &mut Self {
        self.data.override_value = var;
        self
    }

    pub fn build(&self) -> DataSetTarget {
        DataSetTarget(self.data.clone())
    }

    pub fn insert(&self, reader: &mut DataSetReader) {
        reader
            .sub_data_set()
            .add_target(DataSetTarget(self.data.clone()))
    }
}

/// Contains the information what todo with variables
pub struct SubscribedDataSet {
    targets: Vec<DataSetTarget>,
}

pub struct UpdateTarget<'a>(pub Guid, pub DataValue, pub &'a PubSubFieldMetaData);

impl SubscribedDataSet {
    pub const fn new() -> Self {
        Self {
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

    pub fn generate_cfg(&self) -> Vec<FieldTargetDataType> {
        self.targets.iter().map(|t| t.0.clone()).collect()
    }
}

impl Default for SubscribedDataSet {
    fn default() -> Self {
        Self::new()
    }
}
