use futures::TryStreamExt;
use mongodb::Client;
use reqwest;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::{thread, time};

#[derive(Debug, Serialize, Deserialize)]
pub struct MqttData {
    mqtt_lock_to_uid: String,
    mqtt_ip: String,
    mqtt_topic: String,
    mqtt_topic_modif: i32,
    mqtt_rw: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MqttWriteVal {
    node_uid: String,
    node_val: String,
}

async fn loop_through_data(mqtt_datas: Vec<MqttData>) {
    for mqtt_data in mqtt_datas {
        // Handle modbus reads. Or other words reads out the modbus register, and stores the data in the database
        if mqtt_data.mqtt_rw == "r" {
            let read_data = data_concentrator_mqtt::mqtt_read(
                mqtt_data.mqtt_ip,
                mqtt_data.mqtt_topic,
                mqtt_data.mqtt_topic_modif as f32,
            )
            .unwrap();

            let first_element = read_data;

            let mut map = HashMap::new();
            map.insert("node_val", first_element.to_string());
            map.insert("node_uid", mqtt_data.mqtt_lock_to_uid);

            let rw_client = reqwest::Client::new();

            let _res = rw_client
                .post("http://127.0.0.1:8000/w")
                .json(&map)
                .send()
                .await
                .unwrap();
        } else if mqtt_data.mqtt_rw == "w" {
            // Handle modbus writes. Or in other words, reads out the value from the data base, and writes it to the given register.
            let mut get_url = "http://127.0.0.1:8000/r/".to_string();
            get_url.push_str(&mqtt_data.mqtt_lock_to_uid);

            let resp = reqwest::get(get_url)
                .await
                .unwrap()
                .json::<MqttWriteVal>()
                .await
                .unwrap();
            data_concentrator_mqtt::mqtt_write(
                mqtt_data.mqtt_ip,
                mqtt_data.mqtt_topic,
                resp.node_val,
            )
            .unwrap();
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Mongo client
    let client = Client::with_uri_str("mongodb://localhost:27017")
        .await
        .unwrap();
    // Mongo db
    let db = client.database("dconc");
    // Mongo collection
    let collection = db.collection::<MqttData>("mqttstuff");
    // To sleep
    let ten_millis = time::Duration::from_secs(10);

    loop {
        let cursor = collection
            .find(None, None)
            .await
            .ok()
            .expect("Can't find data in coll");

        let mb_datas: Vec<MqttData> = cursor.try_collect().await.unwrap();
        // Loop thrugh the read out data.
        loop_through_data(mb_datas).await;

        thread::sleep(ten_millis);
    }
}
