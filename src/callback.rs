use crate::pubdataset::UpdateTarget;
use crate::reader::DataSetReader;
use std::sync::Arc;
use std::sync::Mutex;
pub trait OnPubSubReciveValues {
    fn data_recived(&mut self, reader: &DataSetReader, dataset: &[UpdateTarget]);
}

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
