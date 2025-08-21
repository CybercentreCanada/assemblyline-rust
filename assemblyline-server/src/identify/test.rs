
//     import os
// import pytest

// from cart import unpack_file
// from json import loads
// from pathlib import Path

// from assemblyline.common import forge

use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;

fn get_samples_location() -> String {
    std::env::var("SAMPLES_LOCATION").expect("Define environment variable 'SAMPLES_LOCATION' to the path to the samples directory.")
}


// def test_id_file_base():
//     with forge.get_identify(use_cache=False) as identify:
//         tests_dir = os.path.dirname(__file__)
//         id_file_base = "id_file_base"
//         file_base_dir = os.path.join(tests_dir, id_file_base)
//         map_file = "id_file_base.json"
//         map_path = os.path.join(file_base_dir, map_file)
//         with open(map_path, "r") as f:
//             contents = f.read()
//             json_contents = loads(contents)
//         for _, _, files in os.walk(file_base_dir):
//             for file_name in files:
//                 if file_name == map_file:
//                     continue

//                 file_path = os.path.join(file_base_dir, file_name)
//                 data = identify.fileinfo(file_path, generate_hashes=False)
//                 actual_value = data.get("type", "")
//                 expected_value = json_contents[file_name]
//                 assert actual_value == expected_value


// def get_ids(filepath):
//     if not isinstance(filepath, (str, bytes, os.PathLike)):
//         return "skipped"
//     return "-".join(split_sample(filepath))


// def split_sample(filepath):
//     target_file = os.path.join("/tmp", os.path.basename(filepath).rstrip(".cart"))
//     identify_result = str(filepath.relative_to(Path(SAMPLES_LOCATION)).parent)
//     return (target_file, identify_result)


// @pytest.fixture()
// def sample(request):
//     target_file, identify_result = split_sample(request.param)
//     try:
//         unpack_file(request.param, target_file)
//         yield (target_file, identify_result)
//     finally:
//         if target_file:
//             os.unlink(target_file)


#[tokio::test]
async fn sample_identification() {
    let _ = env_logger::builder().filter_level(log::LevelFilter::Debug).is_test(true).try_init();
    let identify = super::Identify::new_without_cache().await.unwrap();

    let mut directories = vec![(String::new(), PathBuf::from(get_samples_location()))];
    let mut failures  = vec![];
    let mut counter = 0;

    while let Some((type_string, dir)) = directories.pop() {
        let mut cursor = tokio::fs::read_dir(&Path::new(&dir)).await.unwrap();
        while let Some(file) = cursor.next_entry().await.unwrap() {
            let file_name = file.file_name().into_string().unwrap();
            if file.file_type().await.unwrap().is_dir() {
                let type_string = if type_string.is_empty() {
                    file_name
                } else {
                    format!("{type_string}/{file_name}")
                };
                directories.push((type_string, file.path()));
                continue
            }

            if !file_name.ends_with(".cart") {
                continue
            }
            println!("\n{counter}  {type_string}  {file_name}");
            counter += 1;

            let temp = tokio::task::spawn_blocking(move || {
                let istream = std::fs::OpenOptions::new().read(true).open(file.path()).unwrap();
                let temp = NamedTempFile::new().unwrap();
                let (_, _) = cart_container::unpack_stream(istream, temp.as_file(), None).unwrap();
                temp
            }).await.unwrap();


            let ident = identify.fileinfo(temp.path().to_path_buf(), false, false, false).await.unwrap();

            println!("{type_string}  {}", ident.file_type);
            if ident.file_type != type_string {
                failures.push((file_name, type_string.clone(), ident));
            }
        }
    }

    for (file_name, expected_type, ident) in &failures {
        println!("{file_name} expected {expected_type} got {}", ident.file_type);
    }

    assert!(failures.is_empty());

    //     @pytest.mark.parametrize("sample", Path(SAMPLES_LOCATION).rglob("*.cart"), ids=get_ids, indirect=True)
//     def test_identify_samples(sample):
//         with forge.get_identify(use_cache=False) as identify:
//             assert identify.fileinfo(sample[0], generate_hashes=False)["type"] == sample[1]

}

