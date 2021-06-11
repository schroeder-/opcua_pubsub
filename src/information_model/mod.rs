// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use opcua_server::{callbacks, prelude::*, session::Session};
use connection::PubSubConnectionModel;
use crate::{app::PubSubApp, connection::PubSubConnectionId, constants::PubSubTransportProfile};
use std::sync::{Arc, RwLock};
use crate::connection::PubSubConnection;
mod connection;
struct AddConnectionHandler{
    pubsub: Arc<RwLock<PubSubApp>>,
    information_model: Arc<RwLock<InformationModel>>
}

impl AddConnectionHandler{
    fn decode_args(input_arguments: &Option<Vec<Variant>>) -> Result<PubSubConnectionDataType, StatusCode>{
        let args = if let Some(args) = input_arguments{
            args
        } else {
            return Err(StatusCode::BadArgumentsMissing);
        };
        if args.len() < 1{
            return Err(StatusCode::BadArgumentsMissing);
        }
        if let Variant::ExtensionObject(obj) = args[0].clone(){
            if let Ok(id) = obj.object_id(){
                if id == ObjectId::PubSubConnectionDataType_Encoding_DefaultBinary{
                    let v = obj.decode_inner::<PubSubConnectionDataType>(&DecodingOptions::default())?;
                    return Ok(v);
                }               
            }
        }
        Err(StatusCode::BadInvalidArgument)
    }
}


// Bad_InvalidArgument	The Server is not able to apply the name. The name may be too long or may contain invalid character.
// Bad_BrowseNameDuplicated	An Object with the name already exists.
// Bad_ResourceUnavailable	The Server has not enough resources to add the PubSubConnection Object.
// Bad_UserAccessDenied	The Session user is not allowed to create a PubSubConnection Object.
impl callbacks::Method for AddConnectionHandler {
    fn call(
        &mut self,
        _session: &mut Session,
        request: &CallMethodRequest,
    ) -> Result<CallMethodResult, StatusCode> {        
        let cfg = Self::decode_args(&request.input_arguments)?;
        // @TODO add correct DataSource
        // @TODO check if connection allready exits
        let con = PubSubConnection::from_cfg(&cfg, None)?;
        let (id, cfg) = {
            let mut ps = self.pubsub.write().unwrap();
            let cfg = con.generate_cfg()?;
            (ps.add_connection(con)?, cfg)
        };
        let model = self.information_model.write().unwrap();
        let nid = NodeId::new(0, format!("PubSubConnection{}", id.0));
        model.add_update_connection(cfg, &nid)?;
        Ok(CallMethodResult {
            status_code: StatusCode::Good,
            input_argument_results: Some(vec![StatusCode::Good]),
            input_argument_diagnostic_infos: None,
            output_arguments: Some(vec![nid.into()]),
        })
    }
}
struct RemoveConnectionHandler{
    pubsub: Arc<RwLock<PubSubApp>>,
    information_model: Arc<RwLock<InformationModel>>
}


impl callbacks::Method for RemoveConnectionHandler {
    fn call(
        &mut self,
        _session: &mut Session,
        request: &CallMethodRequest,
    ) -> Result<CallMethodResult, StatusCode> {        
        if let Some(args) = &request.input_arguments{
            if args.len() > 1{
                if let Variant::NodeId(id) = &args[0]{
                    let res=    {
                        let mut ps = self.pubsub.write().unwrap();
                        // @TODO Connection ids
                        let connection_id = PubSubConnectionId(0);
                        let res = if let Some(con) = ps.get_connection(&connection_id){
                            con.disable();
                            ps.remove_connection(connection_id)?;
                            Ok(CallMethodResult {
                                status_code: StatusCode::Good,
                                input_argument_results: Some(vec![StatusCode::Good, StatusCode::Good]),
                                input_argument_diagnostic_infos: None,
                                output_arguments: None,
                            })
                        } else {
                            Err(StatusCode::BadNodeIdUnknown)
                        };
                        res
                    };
                    if res.is_ok(){
                        self.information_model.write().unwrap().remove_connection(&id);
                    } 
                    res
                } else {
                    Err(StatusCode::BadInvalidArgument)
                }
            } else {
                Err(StatusCode::BadArgumentsMissing)
            }
        } else {
            Err(StatusCode::BadArgumentsMissing)
        }
    }
}


/// Handling of the Models
pub(crate) struct InformationModel{
    addr: Arc<RwLock<AddressSpace>>
}

impl InformationModel{
    fn add_update_connection(&self, cfg: PubSubConnectionDataType, nid: &NodeId) -> Result<(), StatusCode>{
        let mut addr = self.addr.write().unwrap();
        let found = if let Some(n) = addr.find_node(nid){
            if let NodeType::Object(_) = n{
                if !addr.has_reference(&nid, &ObjectTypeId::PubSubConnectionType.into(), ReferenceTypeId::HasTypeDefinition){
                    return Err(StatusCode::BadInvalidArgument)
                }
                nid.clone()
            } else {
                return Err(StatusCode::BadInvalidArgument)
            }
        } else {
            NodeId::null()
        };
        if found.is_null() {
            PubSubConnectionModel::new(nid, cfg, &mut addr);
        } else {
            //@TODO update
        }
        Ok(())
    }

    fn remove_connection(&mut self, nid: &NodeId){
        //@TODO check if instance of pubsubconnection
        let mut a = self.addr.write().unwrap();
        if let Some(_) = a.find_node(nid){
            a.delete(nid, true);
        }
    }
}

/// Implements the PubSubInformationModel for an OPC UA Server
pub struct PubSubInformationModel{
    pubsub: Arc<RwLock<PubSubApp>>,
    information_model: Arc<RwLock<InformationModel>>
}

// @TODO implement PubSub Status (Enabled/Disabled) 
//                 Implement Enable/Disable

impl PubSubInformationModel{
    pub fn new(server: &Server, pubsub: Arc<RwLock<PubSubApp>>) -> Result<Self, StatusCode>{
        let model = InformationModel{ addr: server.address_space().clone() };
        let addr_space = server.address_space();
        let mut addr = addr_space.write().unwrap();
        
        let s = Self{ pubsub, information_model: Arc::new(RwLock::new(model)) };
        s.add_connection_methods(&mut addr);
        s.add_profiles(&mut addr);
        Ok(s)
    }

    /// Adds Connection Methods 
    fn add_connection_methods(&self, addr: &mut AddressSpace){
        let add_connection_handler = Box::new(AddConnectionHandler{ pubsub: self.pubsub.clone(), information_model: self.information_model.clone() });       
        addr.register_method_handler(MethodId::PublishSubscribe_AddConnection, add_connection_handler);
        let remove_connection_handler = Box::new(RemoveConnectionHandler{ pubsub: self.pubsub.clone(), information_model: self.information_model.clone() });
        addr.register_method_handler(MethodId::PublishSubscribe_RemoveConnection, remove_connection_handler);
    }

    /// Adds supported Transport Profiles
    fn add_profiles(&self, addr: &mut AddressSpace){
        let profiles = [
            PubSubTransportProfile::UdpUadp.to_string().to_string(),
            PubSubTransportProfile::MqttUadp.to_string().to_string()
        ];
        let profiles = Variant::from(&profiles[..]);
        addr.set_variable_value(VariableId::PublishSubscribe_SupportedTransportProfiles, profiles, &DateTime::now(), &DateTime::now());
    }

}