use crate::pubdataset::UpdateTarget;
use crate::reader::DataSetReader;

pub trait OnPubSubReciveValues {
    fn data_recived(&mut self, reader: &DataSetReader, dataset: Vec<UpdateTarget>);
}
