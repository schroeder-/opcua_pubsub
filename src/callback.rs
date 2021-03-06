// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::dataset::UpdateTarget;
use crate::reader::DataSetReader;
use std::sync::Arc;
use std::sync::Mutex;

/// Trait to receive subscribed values from a DataSetReader
pub trait OnPubSubReceiveValues {
    /// Received values from a DataSetReader
    fn data_received(&mut self, reader: &DataSetReader, dataset: &[UpdateTarget]);
}

/// Wrapper to wrap function and closures to be consumed as OnPubReceiveValues
/// ```
/// use opcua_pubsub::prelude::*;
/// let cb = OnReceiveValueFn::new_boxed(|reader, dataset|{
///    println!("#### Got Dataset from reader: {}", reader.name());
///    for UpdateTarget(_, dv, meta) in dataset {
///        println!("#### Variable: {} Value: {:?}", meta.name(), dv);
///    }
/// });
/// ```
pub struct OnReceiveValueFn<T>
where
    T: Fn(&DataSetReader, &[UpdateTarget]) + Send,
{
    recv: Box<Mutex<T>>,
}

impl<T> OnReceiveValueFn<T>
where
    T: Fn(&DataSetReader, &[UpdateTarget]) + Send,
{
    /// wraps function or closure
    pub fn new(func: T) -> Self {
        Self {
            recv: Box::new(Mutex::new(func)),
        }
    }
    /// wraps function or closure boxed
    pub fn new_boxed(func: T) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new(func)))
    }
}

impl<T> OnPubSubReceiveValues for OnReceiveValueFn<T>
where
    T: Fn(&DataSetReader, &[UpdateTarget]) + Send,
{
    fn data_received(&mut self, reader: &DataSetReader, dataset: &[UpdateTarget]) {
        (self.recv).lock().unwrap()(reader, dataset);
    }
}

impl<T> From<T> for OnReceiveValueFn<T>
where
    T: Fn(&DataSetReader, &[UpdateTarget]) + Send,
{
    fn from(func: T) -> Self {
        Self::new(func)
    }
}
