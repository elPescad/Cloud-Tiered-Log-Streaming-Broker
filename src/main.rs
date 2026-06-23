use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};
use reqwest::Client;
use dotenvy::dotenv;
use std::env;
use std::fmt::format;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io::{Read, Write};

#[derive(Clone, Debug)]

enum Message
{
    Text(String),
    Binary(Vec<u8>),
}
//Box acts essentially as a pointer but without the need to manually dereference
//here main runs asynchronously and returns type () -> good or it returns
//a dyanmic error type as a pointer Box
#[tokio::main]

async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    //load .env into file
    dotenv().ok();

    println!("Starting cloud tiered broker...");
    //the '?' lets us error check each statement

    //creates channel that can hold 100 unread messages in RAM
    //tx is transmitter
    //rx is reciever
    let (tx, _rx) = broadcast::channel::<Message>(100);

    //bind producer socket to port
    let producer_listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Producer listening on port 127.0.0.1:8080");
    
    let mut disk_rx = tx.subscribe();

    //sole purpose is to write to log file and rotate files once it reaches
    //transfer phase

    /* ---------------------------------------------------------
     *                     DISK SEGMENT
     * ---------------------------------------------------------
     *  */
    tokio::spawn(async move {
        println!("Disk manager task running in background");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("hot_tier.log")
            .await
            .expect("Failed to open hot_tier.log");

        //wait for messages
        loop {
            match disk_rx.recv().await {
                Ok(msg) => {
                    let data = match &msg {
                        Message::Text(t) =>t.as_bytes(),
                        Message::Binary(b) => b.as_slice(), 
                    };

                    //write and sync
                    if let Err(e) = file.write_all(data).await {
                        eprintln!("Disk failed to write");
                        continue;
                    }
                    //write data
                    let _ = file.write_all(b"\n").await;
                    //drop letover bytes in kernel
                    let _ = file.sync_all().await;

                    //file rotation
                    if let Ok(meta) = tokio::fs::metadata("hot_tier.log").await {
                        //10MB (10KB for now)
                        if meta.len() >= 10 * 1024 {
                            println!("Log reached threshold. rotating and uploading...");

                            //get time since Unix epoch to get unique file name for every file
                            let timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();

                            //creates new names in order to store unique values in cloud
                            //name of file to compress
                            let archive_name = format!("archive_{}.log", timestamp);
                            //name of file to upload
                            let cloud_name = format!("segment_{}.log.gz", timestamp);

                            //renames files
                            if let Err(e) = tokio::fs::rename("hot_tier.log", &archive_name).await {
                                eprintln!("Failed to rotate log");
                                continue;
                            }

                            //Opens new file to write
                            file = OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open("hot_tier.log")
                                .await
                                .expect("Failed to create fresh hot_tier.log");

                            //Gets bucket and key from env file
                            let bucket = env::var("GCP_BUCKET_NAME").unwrap();
                            let key = env::var("GCP_KEY_PATH").unwrap();

                            //uploads in the background
                            tokio::spawn(async move {
                                match compress_and_upload_log(&archive_name, &bucket, &cloud_name, &key).await {
                                    Ok(_) => println!("Segment {} securely stored in cloud", cloud_name),
                                    Err(e) => eprintln!("Upload failed for segment {}: {}", cloud_name, e),
                                } 
                            });

                        }
                    }
                }
                Err(_) => {
                    continue;
                }
            }
        }
    });

    //clone transmiter for producer

    /* ---------------------------------------------------------
     *                    PRODUCER SEGMENT
     * ---------------------------------------------------------
     *  */
    let tx_producer = tx.clone();

    tokio::spawn(async move
    {
        //infinite loop
        loop
        {
            //in rust vars are immutable
            //you must declare mut if you want to change it.

            //wait for producer application to connect
            let (mut socket, addr) = producer_listener.accept().await.expect("Failed to accept");
            println!("New producer connected at {}", addr);

            let tx_inner = tx_producer.clone();

            //tokio::spawn says to run this task in the background
            //however async move prevents overwriting the data
            //thus this code block immedietly returns and runs in the
            //background and allows the line above to run again.

            //producer engine moved entierly to background
            tokio::spawn(async move
            {
                //1KB buffer for chunking data.
                let mut buffer = [0; 1024];

                loop
                {
                    //read data from buffer
                    match socket.read(&mut buffer).await
                    {
                        //producer disconnected
                        Ok(0) =>
                        {
                            println!("Producer {} Disconnected cleanly", addr);
                            break;
                        }
                        //bytes were read. Store them as param
                        Ok(bytes_read) =>
                        {
                            //store raw data in data var
                            let data = &buffer[0..bytes_read];

                            //print out data in terminal
                            let msg = if let Ok(text) = std::str::from_utf8(data)
                                {
                                    //if data is a text str
                                    println!("Recieved Transaction log from [{}]: {}", addr, text.trim());
                                    Message::Text(text.to_string())
                                }
                                else
                                {
                                    //if data is anything else
                                    println!("Recieved {} raw binary bytes from [{}]", bytes_read, addr);
                                    Message::Binary(data.to_vec())
                                };

                            let _ = tx_inner.send(msg);
                        }
                        Err(e) =>
                        {
                            println!("Error reading data from socket [{}]: {}", addr, e);
                            break;
                        }
                    }
                }
            });
        }
    });


    //bind consumer socket to port
    let consumer_listener = TcpListener::bind("127.0.0.1:8081").await?;
    println!("Consumer listening on port 127.0.0.1:8081");

    /* ---------------------------------------------------------
     *                     CONSUMER SEGMENT
     * ---------------------------------------------------------
     */
    //Consumer Enginer runs on main thread
    loop
    {
        let (mut socket, addr) = consumer_listener.accept().await?;
        println!("New consumer connected at {}", addr);

        //create a reciever for this consumer
        let mut my_rx = tx.subscribe();

        tokio::spawn(async move {
            loop {
                match my_rx.recv().await
                {
                    Ok(msg) => 
                    {
                        match msg
                        {    
                            Message::Text(text) =>
                            {
                                println!("Recieved text: {}", text);
                                let _ = socket.write_all(text.as_bytes()).await;
                            }
                            Message::Binary(bytes) =>
                            {
                                println!("Recieved {} binary bytes", bytes.len());
                                let _ = socket.write_all(&bytes).await;
                            }
                        }

                        let _ = socket.write_all(b"\n").await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed_count)) =>
                    {
                        eprintln!("Warning: Reciever lagged behind. Missed {} messages", missed_count);
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) =>
                    {
                        eprintln!("Channel closed. the sender was dropped.");
                        break;
                    }
                }
            }
        });
    }
}

//compresses file and uploads
async fn compress_and_upload_log(local_filename: &str, bucket_name: &str, object_name: &str, key_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Compressing {}...", local_filename);

    //creates new .gz file
    let compressed_filename = format!("{}.gz", local_filename);
    //anon block. Acts as sandbox to ensure coder finishes and closes file cleanly
    {
        let mut input_file = File::open(local_filename)?;
        //create new file to store compressed data whose path is the new .gz file name we created
        let compressed_file = File::create(&compressed_filename)?;
        let mut encoder = GzEncoder::new(compressed_file, Compression::default());

        //create storage for bytes
        let mut buffer = Vec::new();
        input_file.read_to_end(&mut buffer)?;
        //write stored bytes into new compression file
        encoder.write_all(&buffer)?;
        encoder.finish()?;
    }

    //Authenticate with Google Cloud using JSON key
    println!("Authenticating with GCP...");
    let secret = read_service_account_key(key_path).await?;
    let auth = ServiceAccountAuthenticator::builder(secret).build().await?;
    let scopes = &["https://www.googleapis.com/auth/devstorage.read_write"];
    let token = auth.token(scopes).await?;

    //Upload compressed file to google cloud bucket
    println!("Uploading {} to Google Cloud...", compressed_filename);
    let file_bytes = tokio::fs::read(&compressed_filename).await?;
    let client = Client::new();

    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        bucket_name, object_name
    );

    let response = client
        .post(&url)
        .bearer_auth(token.token().unwrap())
        .header("Content-Type", "application/gzip")
        .body(file_bytes)
        .send()
        .await?;

    //Verify Delivery and Cleanup local drive
    if response.status().is_success() {
        println!("Success. File {} safely stored in bucket.", object_name);

        //safely wipe local data because google confirmed reciept
        tokio::fs::remove_file(local_filename).await?;
        tokio::fs::remove_file(&compressed_filename).await?;
        println!("Local files wiped cleanly");
        Ok(())
    } else {
        let error_msg = response.text().await?;
        Err(format!("GCP rejected the upload: {}", error_msg).into())
    }
}