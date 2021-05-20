use opcua_types::{Variant, string::UAString};
use crate::pubdataset::PubSubFieldMetaData;
use crate::message::{UadpNetworkMessage, UadpDataSetMessage, UadpMessageType};
use log::{error};
/// Reader group 
pub struct ReaderGroup{
    pub name: UAString,
    reader: Vec::<DataSetReader>
}

/// Builds a dataset
pub struct DataSetReaderBuilder{
    name: UAString,
    publisher_id: Variant,
    writer_group_id: u16,
    dataset_writer_id: u16,
}

/// DataSetReader targets a publisher and the dataset
/// if publisher_id = Variant::Empty every publisher is read
pub struct DataSetReader{
    name: UAString,
    publisher_id: Variant,
    writer_group_id: u16,
    dataset_writer_id: u16,
    fields: Vec::<PubSubFieldMetaData>
}

impl DataSetReaderBuilder{
    pub fn new() -> Self{
        DataSetReaderBuilder{    
            name: "DataSet Reader".into(),
            publisher_id: Variant::Empty,
            writer_group_id: 0_u16,
            dataset_writer_id: 0_u16,
        }
    }
    pub fn name<'a>(&'a mut self, name: UAString) -> &'a mut Self{
        self.name = name;
        self
    }
    /// Sets the targeted publisher id
    pub fn publisher_id<'a>(&'a mut self, pub_id: Variant) -> &'a mut Self{
        self.publisher_id = pub_id;
        self
    }
    /// Sets the targeted writer_group_id
    pub fn writer_group_id<'a>(&'a mut self, writer_group_id: u16) -> &'a mut Self{
        self.writer_group_id = writer_group_id;
        self
    }
    /// Sets the targeted dataset_writer
    pub fn dataset_writer_id<'a>(&'a mut self, dataset_writer_id: u16) -> &'a mut Self{
        self.dataset_writer_id = dataset_writer_id;
        self
    }

    pub fn build(&self) -> DataSetReader{
        DataSetReader{
            name: self.name.clone(),
            publisher_id: self.publisher_id.clone(),
            writer_group_id: self.writer_group_id,
            dataset_writer_id: self.dataset_writer_id,
            fields: Vec::new()
        }
    }
}

impl ReaderGroup{
    pub fn new(name: UAString) -> Self{
        ReaderGroup{name, reader: Vec::new()}
    }
    /// Check the message and forward it to the datasets
    pub fn handle_message(&self, msg: &UadpNetworkMessage){
        for r in self.reader.iter(){
            r.handle_message(msg);
        }
    }
    /// Adds Dataset to the group
    pub fn add_dataset_reader(&mut self, dataset_reader: DataSetReader) {
        self.reader.push(dataset_reader);
    }
}

impl DataSetReader{
    fn check_message(&self, msg: &UadpNetworkMessage) -> Option<usize>{
        // Check if message is for this publisher_id
        if self.publisher_id != Variant::Empty{
            match &msg.header.publisher_id {
                Some(pub_id) => if &self.publisher_id != pub_id{
                    return None;
                },
                None => return None,  
            } 
        } 
        // Find out if the writer_groupe is contained in the message
        if let Some(gp) = &msg.group_header{
            if let Some(wg_id) = gp.writer_group_id {
                if wg_id == self.writer_group_id{
                    // Find the dataset if contained
                    if let Some(idx) = 
                        msg.dataset_payload.iter().position(|ds| ds == &self.dataset_writer_id){
                        return Some(idx)
                    }
                }
            }
        }
        return None
    }

    fn handle_fields(&self, ds: &UadpDataSetMessage){
        match &ds.data{
            UadpMessageType::KeyDeltaFrameRaw(_) => {
                error!("Raw Frames not implemented");
            }, 
            UadpMessageType::KeyDeltaFrameValue(vals) => {
                for (id, v) in vals.iter(){
                    let p = *id as usize;
                    if p < self.fields.len(){
                        let f = &self.fields[p];
                        println!("Field {}: {}", f.name(), v.value.as_ref().unwrap_or(&Variant::String("Error".into()))); 
                    } else{
                        println!("Unknown field {} : value: {}", id, v.value.as_ref().unwrap_or(&Variant::String("Error".into())));
                    }
                }
            },
            UadpMessageType::KeyDeltaFrameVariant(vals) => {
                for (id, v) in vals.iter(){
                    let p = *id as usize;
                    if p < self.fields.len(){
                        let f = &self.fields[p];
                        println!("Field {}: {}", f.name(), v); 
                    } else{
                        println!("Unknown field {} : value: {}", p, v);
                    }
                }
            },
            UadpMessageType::KeyFrameDataValue(vals) => {
                for (p, v) in vals.iter().enumerate(){
                    if p < self.fields.len(){
                        let f = &self.fields[p];
                        println!("Field {}: {}", f.name(), v.value.as_ref().unwrap_or(&Variant::String("Error".into()))); 
                    } else{
                        println!("Unknown field {} : value: {}", p, v.value.as_ref().unwrap_or(&Variant::String("Error".into())));
                    }
                }
            },
            UadpMessageType::KeyFrameRaw(_) => {
                //@TODO parse raw missing
                error!("Raw Frames not implemented");
            },
            UadpMessageType::KeyFrameVariant(vals) => {
                for (p, v) in vals.iter().enumerate(){
                    if p < self.fields.len(){
                        let f = &self.fields[p];
                        println!("Field {}: {}", f.name(), v); 
                    } else{
                        println!("Unknown field {} : value: {}", p, v);
                    }
                }
            },
            // Ignore keep alive this message is for the pusubconnection
            UadpMessageType::KeepAlive => {},
            // Support is missing @TODO implement Events 
            UadpMessageType::Event(_) => {
                error!("Event Message not supported in Dataset {}!", self.name);
            },
        }
    }
    /// Handle a message if it matches the writer
    pub fn handle_message(&self, msg: &UadpNetworkMessage){
        if let Some(idx) = self.check_message(msg){
            if idx < msg.dataset.len(){
                let ds = &msg.dataset[idx];
                self.handle_fields(ds);
            }
        }               
    }

    pub fn add_field(&mut self, field: PubSubFieldMetaData){
        self.fields.push(field);
    }
}