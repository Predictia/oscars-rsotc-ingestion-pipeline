import zipfile

from ingestion_pipeline.utilities.zip_utils import zip_directory


def test_zip_directory(tmp_path):
    # Create a dummy directory structure
    dir_to_zip = tmp_path / "source"
    dir_to_zip.mkdir()
    (dir_to_zip / "file1.txt").write_text("file1 content")
    sub_dir = dir_to_zip / "subdir"
    sub_dir.mkdir()
    (sub_dir / "file2.txt").write_text("file2 content")

    zip_path = tmp_path / "test.zip"

    zip_directory(dir_to_zip, zip_path)

    assert zip_path.exists()

    # Verify zip content
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        names = zip_ref.namelist()
        assert "file1.txt" in names
        assert "subdir/file2.txt" in names

        assert zip_ref.read("file1.txt").decode() == "file1 content"
        assert zip_ref.read("subdir/file2.txt").decode() == "file2 content"
