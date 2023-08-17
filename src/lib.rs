use paho_mqtt::*;
use std::thread;

fn mqtt_cli(uri: String) -> Client {
    // Configure the MQTT client
    let client_opts = CreateOptionsBuilder::new()
        .mqtt_version(MQTT_VERSION_5)
        .server_uri(uri)
        .finalize();

    // Connect options
    let options = ConnectOptionsBuilder::new()
        .ssl_options(SslOptions::default())
        .clean_start(true)
        .password("")
        .finalize();

    // Create the MQTT client
    let cli = Client::new(client_opts).expect("Error creating client");

    // Connect to your broker
    cli.connect(options).expect("Error connecting to broker");

    cli
}

pub fn mqtt_read(mqtt_ip: String, mqtt_topic: String, mqtt_topic_modif: f32) -> Result<i32> {
    // Create the MQTT client
    let cli = mqtt_cli(mqtt_ip);

    // Message receiver
    let rx = cli.start_consuming();

    let topic = mqtt_topic;
    // Subscribe to a topic
    cli.subscribe(&topic, 0)
        .expect("Error subscribing to topic");

    // Start waiting for messages
    let reader = thread::spawn(move || match rx.recv().expect("Error receiving message") {
        Some(message) => {
            let tmp_value: f32 = message.payload_str().parse().unwrap();
            let payload_value = (tmp_value * mqtt_topic_modif) as i32;
            payload_value
        }
        None => 0,
    });
    // Wait until we have received our message
    let payload_v = reader.join().unwrap();
    // Good-Bye
    cli.disconnect(DisconnectOptions::default())
        .expect("Error disconnecting");

    Ok(payload_v)
}

pub fn mqtt_write(mqtt_ip: String, mqtt_topic: String, mqtt_data_to_write: String) -> Result<()> {
    // Create the MQTT client
    let cli = mqtt_cli(mqtt_ip);

    let new_topic = mqtt_topic;
    // Publish a message!
    cli.publish(Message::new(new_topic, mqtt_data_to_write.to_string(), 0))
        .expect("Error publishing");

    // Good-Bye
    cli.disconnect(DisconnectOptions::default())
        .expect("Error disconnecting");

    Ok(())
}
