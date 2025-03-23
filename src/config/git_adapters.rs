use crate::config::configuration::GitSourceDetails;
use crate::model::Graph;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use reqwest::{Client, Method, Url};
use serde::{Deserialize, Serialize};
use std::sync::Arc;


async fn load_github_content(http_client: Arc<Client>, token: &String, base_url: &String, path: &String) -> Result<Vec<GitHubContentEntry>, String>{
    tracing::info!("Loading GitHub content from: {} and {}", base_url, path);
    let result = http_client.request(Method::GET, Url::parse(&format!("{}/{}", base_url, path)).unwrap())
        .header("Accept", "application/vnd.github+json")
        .header("Authorization", format!("Bearer {}", token))
        .header("User-Agent", "IronFlow")
        .send().await;

    match result {
        Ok(response) => {
            if response.status().is_success() {
                let json = response.text_with_charset("utf-8").await.unwrap();
                if json.starts_with("[") {
                    Ok(serde_json::from_str(json.as_str()).unwrap())
                } else {
                    Ok(vec![serde_json::from_str(json.as_str()).unwrap()])
                }
            } else {
                Err(response.text().await.unwrap())
            }
        }
        Err(err) => {
            Err(err.to_string())
        }
    }
}

async fn load_github_encoded_content(http_client: Arc<Client>, token: &String, base_url: &String, entry: &GitHubContentEntry) -> Vec<String>{
    let mut contents = Vec::<String>::new();
    match entry {
        GitHubContentEntry::File(file_entry) => {
            match &file_entry.content {
                None => {
                    let nested_entries = load_github_content(http_client.clone(), token, base_url, &file_entry.path).await.unwrap();
                    for nested_entry in nested_entries {
                        contents.extend(Box::pin(load_github_encoded_content(http_client.clone(), token, base_url, &nested_entry)).await);
                    }
                }
                Some(content) => {
                    contents.push(content.replace("\n", "").clone());
                }
            }
        }
        GitHubContentEntry::Directory(directory_entry) => {
            if let Some(entries) = &directory_entry.entries {
                for entry in entries {
                    contents.extend(Box::pin(load_github_encoded_content(http_client.clone(), token, base_url, &entry)).await);
                }
            }
        }
    }
    contents
}

pub async fn load_contents_from_github(http_client: Arc<Client>, token: &String, git_source_details: &GitSourceDetails) -> Result<Vec<String>, String> {
    let base_url = format!("{}/contents", git_source_details.url);
    let result = load_github_content(http_client.clone(), token, &base_url, &git_source_details.directory).await;
    match result {
        Ok(entries) => {
            let mut  result: Vec<String> =  vec![];
            for entry in entries {
                let contents = load_github_encoded_content(http_client.clone(), &token, &base_url, &entry).await;
                result.extend(contents);
            }
            Ok(result)
        }
        Err(err) => {
            Err(err.to_string())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum GitHubContentEntry {
    #[serde(rename = "file")]
    File(FileEntry),
    #[serde(rename = "dir")]
    Directory(DirectoryEntry),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FileEntry {
    pub encoding: Option<String>, // Optional, as not every file might be encoded
    pub size: u64,
    pub name: String,
    pub path: String,
    pub content: Option<String>, // For base64 content
    pub sha: String,
    pub url: String,
    pub git_url: String,
    pub html_url: String,
    pub download_url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub size: u64,
    pub name: String,
    pub path: String,
    pub sha: String,
    pub url: String,
    pub git_url: String,
    pub html_url: String,
    pub entries: Option<Vec<GitHubContentEntry>>, // Nested directories or files
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Links {
    pub self_: String,
    pub git: String,
    pub html: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum GitHubContentResponse {
    Single(GitHubContentEntry),
    List(Vec<GitHubContentEntry>),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_load_github_content() {
        let json = r#"[{"name":"opsgenie.yaml","path":"resources/workflows/opsgenie.yaml","sha":"633ef68cdde3f576628e18cb14b8f6fd3162017d","size":2344,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/633ef68cdde3f576628e18cb14b8f6fd3162017d","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/opsgenie.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/633ef68cdde3f576628e18cb14b8f6fd3162017d","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie.yaml"}},{"name":"opsgenie_chain.yaml","path":"resources/workflows/opsgenie_chain.yaml","sha":"ce2a7231496e925fde41b9a090fc170b58fcb7ed","size":113,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie_chain.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie_chain.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/ce2a7231496e925fde41b9a090fc170b58fcb7ed","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/opsgenie_chain.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie_chain.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/ce2a7231496e925fde41b9a090fc170b58fcb7ed","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie_chain.yaml"}},{"name":"opsgenie_loop.yaml","path":"resources/workflows/opsgenie_loop.yaml","sha":"0210c198dbac5395fedd97914e797fcc82f1ac56","size":573,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie_loop.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie_loop.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/0210c198dbac5395fedd97914e797fcc82f1ac56","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/opsgenie_loop.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie_loop.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/0210c198dbac5395fedd97914e797fcc82f1ac56","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie_loop.yaml"}},{"name":"test.yaml","path":"resources/workflows/test.yaml","sha":"48c839a7eab08034dc8c70913eb69c50f9c244bf","size":1360,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/test.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/test.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/48c839a7eab08034dc8c70913eb69c50f9c244bf","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/test.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/test.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/48c839a7eab08034dc8c70913eb69c50f9c244bf","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/test.yaml"}},{"name":"workflow.yaml","path":"resources/workflows/workflow.yaml","sha":"608e369883d269ec60f611d057613fe6d2bdc8ec","size":1366,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/workflow.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/workflow.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/608e369883d269ec60f611d057613fe6d2bdc8ec","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/workflow.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/workflow.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/608e369883d269ec60f611d057613fe6d2bdc8ec","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/workflow.yaml"}}]"#;
        //let json = r#"{"name":"opsgenie.yaml","path":"resources/workflows/opsgenie.yaml","sha":"633ef68cdde3f576628e18cb14b8f6fd3162017d","size":2344,"url":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie.yaml?ref=main","html_url":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie.yaml","git_url":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/633ef68cdde3f576628e18cb14b8f6fd3162017d","download_url":"https://raw.githubusercontent.com/ErenKizilay/ironflow/main/resources/workflows/opsgenie.yaml","type":"file","_links":{"self":"https://api.github.com/repos/ErenKizilay/ironflow/contents/resources/workflows/opsgenie.yaml?ref=main","git":"https://api.github.com/repos/ErenKizilay/ironflow/git/blobs/633ef68cdde3f576628e18cb14b8f6fd3162017d","html":"https://github.com/ErenKizilay/ironflow/blob/main/resources/workflows/opsgenie.yaml"}}"#;
        let result: Vec<GitHubContentEntry> = serde_json::from_str(json).unwrap();
        println!("{:#?}", result);
    }
}