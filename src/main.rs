mod conn;
mod packet;
mod io;
mod error;

use mysql_common as myc;

use std::io as stdio;
use std::io::Cursor;
use byteorder::ReadBytesExt;
use byteorder::LittleEndian as LE;

use crate::myc::time;
use crate::myc::constants as consts;
use crate::conn::Conn;
use crate::conn::Opts;
use crate::conn::OptsBuilder;
use crate::myc::params::Params;
use crate::myc::row::Row;
use crate::myc::value::convert::{from_value, from_value_opt};
use crate::myc::value::Value;

struct Event {
    header: EventHeader,
    raw_data: Vec<u8>
}

struct EventHeader {
    timestamp: u32,
    event_type: u8,
    server_id: u32,
    event_size: u32,
    log_position: u32,
    flags: u16
}

fn main() {
    let mut builder = OptsBuilder::new();
    builder.user(Some("root"))
        .pass(Some("pw"))
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(3306);
    let mut conn = Conn::new(builder).unwrap();
    println!("connected: {}", conn.get_is_connected());

    loop {
        let mut line = String::new();
        stdio::stdin().read_line(&mut line).unwrap();

        if line.trim().eq("exit") {
            break;
        }
        else if line.trim().eq("ping") {
            println!("ping: {}", conn.ping());
        }
        else if line.trim().eq("sync") {
            let registered = conn.register_as_slave();
            println!("registered: {}", registered);
            let requested = conn.request_binlog_stream(4);
            println!("requested binlog: {}", requested);
            loop {
                match conn.read_packet() {
                    Ok(data) => {
                        let mut rdr = Cursor::new(data);
                        rdr.read_u8().unwrap();
                        let timestamp = rdr.read_u32::<LE>().unwrap();
                        let event_type = rdr.read_u8().unwrap();
                        let server_id = rdr.read_u32::<LE>().unwrap();
                        let event_size = rdr.read_u32::<LE>().unwrap();
                        let log_position = rdr.read_u32::<LE>().unwrap();
                        let flags = rdr.read_u16::<LE>().unwrap();

                        let header = EventHeader {
                            timestamp: timestamp,
                            event_type: event_type,
                            server_id: server_id,
                            event_size: event_size,
                            log_position: log_position,
                            flags: flags
                        };

                        if header.event_type == 30 { // write row event
                            //println!("timestamp: {}", timestamp);
                            //println!("event type: {}", event_type);
                            //println!("event size: {}", event_size);
                            //println!("log position: {}", log_position);

                            //println!("post header len: {}", header.event_size - 19);
                            let table_id = rdr.read_u32::<LE>().unwrap();
                            rdr.read_u16::<LE>().unwrap();
                            //println!("table id: {}", table_id);
                            let flags = rdr.read_u16::<LE>().unwrap();
                            //println!("flags: {}", flags);
                            let extra_data_len = rdr.read_u16::<LE>().unwrap();
                            //println!("extra data len: {}", extra_data_len);

                            //for x in 0..extra_data_len {
                            //let extra_type = rdr.read_u8().unwrap();
                            //let extra_length = rdr.read_u8().unwrap();
                            //let extra_format = rdr.read_u8().unwrap();
                            //println!("extra type: {}", extra_type);
                            //let data = rdr.read_to_end::<LE>().unwrap();
                            //println!("data: {}", data);
                            //println!("extra length: {}", extra_length);
                            //println!("extra format: {}", extra_format);
                            //}

                            let num_columns = rdr.read_u8().unwrap();
                            if num_columns == 1 {
                                let columns_present_bitmap = rdr.read_u8().unwrap();
                                //println!("num columns: {}", num_columns);
                                //println!("column bitmap: {}", columns_present_bitmap);

                                let nul_bitmap = rdr.read_u8().unwrap();
                                let column_value = rdr.read_u8().unwrap();

                                println!("WRITE_ROWS_EVENTv2 value:{}", column_value);
                                //for x in 0..8 {
                                //let value = rdr.read_u8().unwrap();
                                //println!("value: {}", value);
                                //}
                                println!("");
                            }

                        }
                    },
                    Err(err) => println!("{}", err),
                }
            }
        }
    }

    // TODO: Understand why there are 2 Conn drops
}
