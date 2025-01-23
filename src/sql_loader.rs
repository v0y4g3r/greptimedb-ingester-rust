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

use crate::error;
use snafu::ResultExt;
use sqlx::{Executor, MySqlPool};

/// SQL loader.
pub struct SqlLoader {
    pool: MySqlPool,
}

impl SqlLoader {
    /// Creates SQL loader.
    pub async fn new(database_url: String) -> error::Result<Self> {
        let pool = MySqlPool::connect(&database_url)
            .await
            .context(error::ConnectMysqlSnafu { url: database_url })?;
        Ok(Self { pool })
    }

    /// Run SQL content in give file.
    pub async fn load(&self, path: impl AsRef<str>) -> error::Result<()> {
        let path = path.as_ref();
        let content = tokio::fs::read_to_string(path)
            .await
            .context(error::ReadSqlFileSnafu { path })?;
        let _result = self
            .pool
            .execute(&*content)
            .await
            .context(error::ExecuteSqlSnafu { path })?;
        Ok(())
    }
}
