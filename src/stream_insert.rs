// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::Result;
use crate::error::{self, IllegalDatabaseResponseSnafu};
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::{
    greptime_database_client::GreptimeDatabaseClient, InsertRequest, RowInsertRequests,
};
use greptime_proto::v1::{
    greptime_response, AffectedRows, AuthHeader, GreptimeRequest, GreptimeResponse, InsertRequests,
    RequestHeader,
};
use snafu::OptionExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;
use tonic::{Response, Status};

/// A structure that provides some methods for streaming data insert.
///
/// [`StreamInserter`] cannot be constructed via the `StreamInserter::new` method.
/// You can use the following way to obtain [`StreamInserter`].
///
/// ```ignore
/// let grpc_client = Client::with_urls(vec!["127.0.0.1:4002"]);
/// let client = Database::new_with_dbname("db_name", grpc_client);
/// let stream_inserter = client.streaming_inserter().unwrap();
/// ```
///
/// If you want to see a concrete usage example, please see
/// [stream_inserter.rs](https://github.com/GreptimeTeam/greptimedb-client-rust/tree/master/examples/stream_ingest.rs).
pub struct StreamInserter {
    sender: mpsc::Sender<GreptimeRequest>,

    auth_header: Option<AuthHeader>,

    dbname: String,

    join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>>,
}

impl StreamInserter {
    pub(crate) fn new(
        mut client: GreptimeDatabaseClient<Channel>,
        dbname: String,
        auth_header: Option<AuthHeader>,
        channel_size: usize,
        hint: Option<MetadataValue<Ascii>>,
    ) -> Result<StreamInserter> {
        let (send, recv) = mpsc::channel(channel_size);

        let join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>> =
            tokio::spawn(async move {
                let recv_stream = ReceiverStream::new(recv);
                let mut request = tonic::Request::new(recv_stream);
                if let Some(hint) = hint {
                    request.metadata_mut().insert("x-greptime-hints", hint);
                }
                client.handle_requests(request).await
            });

        Ok(StreamInserter {
            sender: send,
            auth_header,
            dbname,
            join,
        })
    }

    #[deprecated(note = "Use row_insert instead.")]
    pub async fn insert(&self, requests: Vec<InsertRequest>) -> Result<()> {
        let inserts = InsertRequests { inserts: requests };
        let request = self.to_rpc_request(Request::Inserts(inserts));

        self.sender.send(request).await.map_err(|e| {
            error::ClientStreamingSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    /// Write Row based insert requests to GreptimeDB with streaming
    pub async fn row_insert(&self, requests: RowInsertRequests) -> Result<()> {
        let request = self.to_rpc_request(Request::RowInserts(requests));

        self.sender.send(request).await.map_err(|e| {
            error::ClientStreamingSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    pub async fn finish(self) -> Result<u32> {
        drop(self.sender);

        let response = self.join.await.unwrap()?;

        let response = response
            .into_inner()
            .response
            .context(IllegalDatabaseResponseSnafu {
                err_msg: "GreptimeResponse is empty",
            })?;

        let greptime_response::Response::AffectedRows(AffectedRows { value }) = response;

        Ok(value)
    }

    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        }
    }
}
