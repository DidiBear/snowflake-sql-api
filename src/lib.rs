use std::time::Duration;

use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

#[derive(Default)]
pub struct ConnectionParams<'params> {
    pub account_name: &'params str,
    pub username: &'params str,
    pub password: &'params str,
    pub warehouse: &'params str,
    pub role: &'params str,
    pub database: &'params str,
    pub schema: &'params str,
}

pub struct SnowflakeClient {
    host: String,
    session: Session,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Session {
    token: String,
    // session_id: u64,
}

/// Based on https://github.com/joshuataylor/snowflake_elixir/blob/master/lib/snowflake_elixir/http/snowflake_client.ex
impl SnowflakeClient {
    pub async fn login<'params>(
        params: ConnectionParams<'params>,
    ) -> reqwest::Result<SnowflakeClient> {
        let host = format!("https://{}.snowflakecomputing.com", params.account_name);

        let url = Url::parse_with_params(
            &format!("{host}/session/v1/login-request"),
            [
                ("warehouse", params.warehouse),
                ("roleName", params.role),
                ("databaseName", params.database),
                ("schemaName", params.schema),
            ],
        )
        .expect("URL is wrong");

        let body = json!({
            "data": {
                "ACCOUNT_NAME": params.account_name,
                "PASSWORD": params.password,
                "CLIENT_APP_ID": "JavaScript",
                "CLIENT_APP_VERSION": "1.5.3",
                "LOGIN_NAME": params.username,
                "SESSION_PARAMETERS": {
                    "VALIDATE_DEFAULT_PARAMETERS": true,
                    "QUOTED_IDENTIFIERS_IGNORE_CASE": true,
                },
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "SnowflakeEx",
                    "OCSP_MODE": "FAIL_OPEN",
                    "OS": "Linux",
                    "tracing": "DEBUG",
                    "account": params.account_name,
                    "user": params.username,
                    "warehouse": params.warehouse,
                    "database": params.database,
                    "schema": params.schema,
                }
            }
        });

        let response = Client::new()
            .post(url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .json(&body)
            .timeout(Duration::from_secs(120))
            .send()
            .await?;

        #[derive(Deserialize)]
        struct LoginResponse {
            data: Session,
            success: bool,
        }

        let result: LoginResponse = response.json().await?;

        if result.success {
            Ok(SnowflakeClient {
                host,
                session: result.data,
            })
        } else {
            todo!()
        }
    }

    pub async fn query<TRow: DeserializeOwned>(&self, query: &str) -> reqwest::Result<Vec<TRow>> {
        let response = Client::new()
            .post(format!(
                "{}/queries/v1/query-request?requestId={}",
                self.host,
                Uuid::new_v4()
            ))
            .header("Content-Type", "application/json")
            .header("accept", "application/snowflake")
            .header(
                "Authorization",
                format!("Snowflake Token=\"{}\"", self.session.token),
            )
            .json(&json!({
                "sqlText": query,
                "sequenceId": 0,
                "bindings": null,
                "bindStage": null,
                "describeOnly": false,
                "parameters": { "CLIENT_RESULT_CHUNK_SIZE": 48 },
                "describedJobId": null,
                "isInternal": false,
                "asyncExec": false,
            }))
            .send()
            .await?;

        #[derive(Deserialize)]
        struct QueryResponse {
            data: QueryData,
            success: bool,
        }

        #[derive(Deserialize)]
        struct QueryData {
            rowset: Value,
            // rowtype: Vec<Value>,
            // total: u32,
        }

        let result: QueryResponse = response.json().await?;
        if !result.success {
            todo!();
        }
        let rows = Vec::<TRow>::deserialize(result.data.rowset).expect("Rows deserialize");
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    // use super::*;

    #[test]
    fn it_works() {}
}
