
import json

import task_queue_runbook as runbook


def test_expected_evidence_files_are_defined():
    assert len(runbook.REQUIRED_EVIDENCE) == 5


def test_json_validator_accepts_valid_json(tmp_path):
    path = tmp_path / "valid.json"
    path.write_text('{"status": "completed"}', encoding="utf-8")

    valid, message = runbook.validate_json_file(path)

    assert valid is True
    assert message == "valid JSON"


def test_json_validator_rejects_invalid_json(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text("{invalid", encoding="utf-8")

    valid, message = runbook.validate_json_file(path)

    assert valid is False
    assert message != "valid JSON"


def test_json_validator_detects_missing_file(tmp_path):
    path = tmp_path / "missing.json"

    valid, message = runbook.validate_json_file(path)

    assert valid is False
    assert message == "missing"


def test_evidence_report_has_expected_structure():
    report = runbook.build_evidence_report()

    assert report["report_name"] == "Task Queue Evidence Report"
    assert report["expected_evidence_count"] == 5
    assert len(report["evidence"]) == 5
    assert "evidence_complete" in report


def test_runbook_is_generated(tmp_path, monkeypatch):
    monkeypatch.setattr(runbook, "PROJECT_ROOT", tmp_path)

    path = runbook.generate_runbook()

    assert path.exists()

    content = path.read_text(encoding="utf-8")

    assert "Queue lifecycle" in content
    assert "Acknowledgement safety" in content
    assert "Failure and recovery" in content
    assert "Operator checklist" in content


def test_evidence_report_file_is_generated(tmp_path, monkeypatch):
    monkeypatch.setattr(runbook, "PROJECT_ROOT", tmp_path)

    path = runbook.generate_evidence_report()

    assert path.exists()

    report = json.loads(path.read_text(encoding="utf-8"))

    assert report["expected_evidence_count"] == 5
    assert len(report["evidence"]) == 5


def test_operator_output_is_generated(tmp_path, monkeypatch):
    monkeypatch.setattr(runbook, "PROJECT_ROOT", tmp_path)

    path = runbook.generate_operator_output()

    assert path.exists()

    report = json.loads(path.read_text(encoding="utf-8"))

    assert report["report_name"] == "Task Queue Evidence Report"
    assert report["expected_evidence_count"] == 5


def test_evidence_report_detects_missing_artifacts(tmp_path, monkeypatch):
    monkeypatch.setattr(runbook, "PROJECT_ROOT", tmp_path)

    report = runbook.build_evidence_report()

    assert report["found_evidence_count"] == 0
    assert report["valid_json_count"] == 0
    assert report["evidence_complete"] is False
