#[macro_use]
extern crate log;

use async_std::io;
use futures::channel::oneshot;
use pg_query::NodeMut;
use sql_proxy::packet::{DatabaseType, Packet};
use sql_proxy::packet_handler::PacketHandler;

struct TransformHandler {
}

impl TransformHandler {
    fn new() -> TransformHandler {
        TransformHandler{}
    }

    fn transform_sql(&mut self, sql: &str) -> String {
        let len = sql.len();
        let stripped = &sql[0..len-1];
        match pg_query::parse(stripped) {
            Ok(mut parsed) => {
                unsafe {
                    for (node, _depth, _context) in parsed.protobuf.nodes_mut().into_iter() {
                        println!("Node: {:?}", node);
                        match node {
                            NodeMut::RangeVar(range) => {
                                (*range).relname = format!("{}{}", (*range).relname, "_aaaa");
                                debug!(
                                    "Range node: {:?} ({})",
                                    (*range).relname,
                                    (*range).relname.len().to_string()
                                );
                            },
                            _ => {}
                        }
                    }
                }
                parsed.deparse().unwrap()
            }
            Err(err) => {
                error!("Parsing error: {:?}", err);
                sql.to_string()
            }
        }
    }
}

#[async_trait::async_trait]
impl PacketHandler for TransformHandler {
    async fn handle_request(&mut self, p: &Packet) -> Packet {
        debug!(
            "req: {:?} packet: {} bytes",
            p.get_packet_type(),
            p.get_size()
        );
        match p.get_query() {
            Ok(sql) => {
                info!("Original SQL: {}", sql);
                let xformed = self.transform_sql(&sql);
                info!("Transformed SQL: {}", xformed);
                let new_packet = Packet::from_query(
                    DatabaseType::PostgresSQL,
                    &xformed
                );
                debug!("Old packet ({}): {:?}", p.bytes.len(), p);
                debug!("New packet ({}): {:?}", new_packet.bytes.len(), new_packet);
                new_packet
            }
            Err(e) => {
                debug!("{:?} packet: {}", p.get_packet_type(), e);
                p.clone()
            }
        }
    }

    async fn handle_response(&mut self, p: &Packet) -> Packet {
        debug!(
            "c<+s: {:?} packet: {} bytes",
            p.get_packet_type(),
            p.get_size()
        );
        p.clone()
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut args = std::env::args().skip(1);
    info!("Table modification proxy test.");
    let bind_addr = args.next().unwrap_or_else(|| "0.0.0.0:5432".to_string());
    let db_addr = args.next().unwrap_or_else(|| "postgres-server:5432".to_string());
    let mut server = sql_proxy::server::Server::new(
        bind_addr.clone(),
        DatabaseType::PostgresSQL,
        db_addr.clone()
    ).await;
    info!("Proxy listening on {}", bind_addr);
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        info!("Proxy listening on {}", bind_addr);
        server.run(TransformHandler::new(), rx).await;
    });

    // Run until use hits enter
    let stdin = io::stdin();
    let mut line = String::new();
    match stdin.read_line(&mut line).await {
        Ok(_) => tx.send(()).unwrap(),
        Err(_) => tx.send(()).unwrap(),
    };
    info!("...exiting");
}