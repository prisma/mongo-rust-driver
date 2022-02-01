use std::{error::Error, path::PathBuf};

use clap::Parser;

mod legacy {
    use serde::Deserialize;
    use serde_yaml::Value;

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Test {
        pub description: String,
        pub min_server_version: String,
        pub fail_point: Option<serde_yaml::Mapping>,
        pub target: Target,
        pub topology: Vec<String>,
        pub change_stream_pipeline: Vec<Value>,
        pub change_stream_options: Option<Value>,
        pub operations: Vec<Operation>,
        pub expectations: Option<Vec<serde_yaml::Mapping>>,
        pub result: TestResult,
    }
    
    #[derive(Debug, Deserialize)]
    pub struct File {
        pub tests: Vec<Test>,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub enum Target {
        Collection,
        Database,
        Client,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(rename_all = "camelCase")]
    pub struct Operation {
        pub name: String,
        pub database: Option<String>,
        pub collection: Option<String>,
        pub arguments: Option<serde_yaml::Mapping>,
    }

    #[derive(Debug, Deserialize, Clone)]
    #[serde(untagged, rename_all = "camelCase")]
    pub enum TestResult {
        Error {
            error: Value,
        },
        Success {
            success: Vec<serde_yaml::Mapping>,
        },
    }
}

mod unified {
    use serde::Serialize;
    use serde_yaml::Value;

    use super::legacy;

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct Test {
        description: String,
        run_on_requirements: RunOnRequirements,
        operations: Vec<Operation>,
    }

    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]    
    struct RunOnRequirements {
        min_server_version: String,
        topologies: Vec<String>,
    }
    
    #[derive(Debug, Serialize)]
    #[serde(rename_all = "camelCase")]    
    struct Operation {
        name: String,
        object: String,
        arguments: Option<serde_yaml::Mapping>,
        save_result_as_entity: Option<String>,
    }

    impl From<legacy::Test> for Test {
        fn from(old: legacy::Test) -> Self {
            Self {
                description: old.description,
                run_on_requirements: RunOnRequirements {
                    min_server_version: old.min_server_version,
                    topologies: old.topology,
                },
                operations: vec![
                    Operation {
                        name: "createChangeStream".to_string(),
                        object: match old.target {
                            legacy::Target::Collection => "collection0".to_string(),
                            legacy::Target::Database => "database0".to_string(),
                            legacy::Target::Client => "client0".to_string(),
                        },
                        arguments: Some({
                            let mut out = vec![
                                (ys("pipeline"), Value::Sequence(old.change_stream_pipeline)),
                            ];
                            if let Some(options) = old.change_stream_options {
                                out.push((ys("options"), options));
                            }
                            out
                        }.into_iter().collect()),
                        save_result_as_entity: Some("changeStream0".to_string()),
                    },
                ],
            }
        }
    }

    fn ys<S: Into<String>>(s: S) -> Value {
        Value::String(s.into())
    }
}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    input: PathBuf,
    #[clap(short, long)]
    test: usize,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let input = std::fs::read_to_string(&args.input)?;
    let file: legacy::File = serde_yaml::from_str(&input)?;
    let test = &file.tests[args.test];

    let out = unified::Test::from(test.clone());
    let text = serde_yaml::to_string(&out)?;
    println!("{}", text);

    Ok(())
}