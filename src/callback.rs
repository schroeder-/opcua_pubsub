// OPC UA Pubsub implementation for Rust
// SPDX-License-Identifier: MPL-2.0
// Copyright (C) 2021 Alexander Schrode
use crate::dataset::UpdateTarget;
use crate::reader::DataSetReader;
use std::sync::Arc;
use std::sync::Mutex;

/// Trait to recive subscribed values from a DataSetReader
pub trait OnPubSubReciveValues {
    /// Recived values from a DataSetReader
    fn data_recived(&mut self, reader: &DataSetReader, dataset: &[UpdateTarget]);
}

/// Wrapper to wrap function and cloures to be consumed as OnPubReciveValues
/// ```
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
    pub fn new(func: T) -> Self {
        OnReceiveValueFn {
            recv: Box::new(Mutex::new(func)),
        }
    }

    pub fn new_boxed(func: T) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new(func)))
    }
}

impl<T> OnPubSubReciveValues for OnReceiveValueFn<T>
where
    T: Fn(&DataSetReader, &[UpdateTarget]) + Send,
{
    fn data_recived(&mut self, reader: &DataSetReader, dataset: &[UpdateTarget]) {
        (self.recv).lock().unwrap()(reader, &dataset);
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
