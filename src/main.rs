use tokio::net::TcpListener;
use tokio::io::AsyncReadExt;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

#[tokio::main]
//Box acts essentially as a pointer but without the need to manually dereference
//here main runs asynchronously and returns type () -> good or it returns
//a dyanmic error type as a pointer Box
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    println!("Starting cloud tiered broker...");
    //the '?' lets us error check each statement

    //bind server to local machine on port 8080
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("socket bound to port 127.0.0.1.8080");

    //infinite loop
    loop
    {
        //in rust vars are immutable
        //you must declare mut if you want to change it.

        //wait for producer application to connect
        let (mut socket, addr) = listener.accept().await?;
        println!("New producer connected at {}", addr);


        //tokio::spawn says to run this task in the background
        //however async move prevents overwriting the data
        //thus this code block immedietly returns and runs in the
        //background and allows the line above to run again.
        tokio::spawn(async move
        {
            //1KB buffer for chunking data.
            let mut buffer = [0; 1024];

            let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("hot_tier.log")
                    .await
                    .expect("Failed to open hot_tier.log");

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

                        if let Err(e) = file.write_all(data).await
                        {
                            println!("Failed to write to disk: {}", e);
                            break;
                        }

                        if let Err(e) = file.write_all(b"\n").await
                        {
                            println!("Failed to write newline: {}", e);
                            break;
                        }

                        if let Err(e) = file.sync_all().await
                        {
                            println!("Failed to sync to disk: {}", e);
                            break;
                        }

                        //print out data in terminal
                        if let Ok(text) = std::str::from_utf8(data)
                        {
                            //if data is a text str
                            println!("Recieved Transaction log from [{}]: {}", addr, text.trim());
                        }
                        else
                        {
                            //if data is anything else
                            println!("Recieved {} raw binary bytes from [{}]", bytes_read, addr);
                        }
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
}