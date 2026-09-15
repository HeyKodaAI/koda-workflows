"""Run Semgrep locally; publish metadata only, never matching source or secrets."""
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    exceptions_path = ROOT / '.github/security/code-exceptions.json'
    exceptions = json.loads(exceptions_path.read_text()) if exceptions_path.exists() else []
    with tempfile.TemporaryDirectory() as tmp:
        raw = pathlib.Path(tmp) / 'semgrep.json'
        command = ['semgrep', 'scan', '--config', str(ROOT / '.github/security/semgrep.json'),
                   '--no-rewrite-rule-ids', '--metrics=off', '--disable-version-check', '--json', '--output', str(raw),
                   '--max-target-bytes', '2000000', '--timeout', '15', '--jobs', '2']
        for pattern in ['.github/security/semgrep.json', '**/node_modules/**', '**/dist/**', '**/build/**', '**/out/**',
                        '**/*.min.js', '**/*.map', '**/.venv/**', '**/venv/**', '**/*.bak.*']:
            command.extend(['--exclude', pattern])
        command.append('.')
        result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if not raw.exists():
            raise RuntimeError('Semgrep produced no report; the scan cannot pass.')
        data = json.loads(raw.read_text())
    findings, reviewed = [], []
    for item in data.get('results', []):
        path = pathlib.Path(item['path'])
        relative = str(path.relative_to(ROOT)) if path.is_absolute() else str(path)
        record = {'rule': item['check_id'], 'file': relative,
                  'line': item['start']['line'], 'message': item['extra']['message']}
        source = ROOT / relative
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        reason = next((e['reason'] for e in exceptions
                       if e['rule'] == record['rule'] and e['file'] == relative
                       and e['line'] == record['line'] and e['sha256'] == digest), None)
        if reason:
            reviewed.append({**record, 'reason': reason})
        else:
            findings.append(record)
    # Parser warnings are retained as coverage limitations. Fatal scanner errors
    # and a zero-file scan fail closed. No source snippets are emitted.
    errors = [{'code': e.get('code'), 'level': e.get('level'), 'file': e.get('path'),
               'type': e.get('type', ['unknown'])[0] if isinstance(e.get('type'), list)
               else e.get('type')} for e in data.get('errors', [])]
    scanned = len(data.get('paths', {}).get('scanned', []))
    report = {'scanned_files': scanned, 'findings': findings, 'reviewed': reviewed,
              'coverage_warnings': errors, 'scanner_exit': result.returncode}
    (ROOT / 'code-security-report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Scanned {scanned} files; {len(findings)} unreviewed findings; '
          f'{len(reviewed)} exact reviewed exceptions; {len(errors)} coverage warnings.')
    for finding in findings:
        print(f"{finding['file']}:{finding['line']}: {finding['rule']}")
    for warning in errors:
        print(f"Coverage warning: {warning['file']}: {warning['type']}")
    fatal = result.returncode != 0 or any(e['level'] == 'error' for e in errors)
    return 1 if findings or fatal or not scanned else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('Code security scan failed:', type(error).__name__)
        sys.exit(1)
