import pathlib,re,subprocess,sys
root=pathlib.Path(__file__).resolve().parents[2]
report=pathlib.Path("gitleaks-report.json")
cmd=["gitleaks","git",".","--config",str(root/".gitleaks.toml"),"--no-banner","--redact=100","--report-format=json","--report-path",str(report)]
p=subprocess.run(cmd,cwd=root,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
print(p.stdout)
counts=re.findall(r"\b(\d+) commits scanned",p.stdout)
errors=re.search(r"(?mi)(?:\bfatal:|\b(?:ERR|FTL)\b|Invalid revision|unknown revision|bad object)",p.stdout)
if p.returncode or errors or not counts or int(counts[-1])==0:
 print("Secret scan did not pass. Review the redacted report; scanner errors and empty scans fail closed.")
 sys.exit(1)
print("Full-history secret scan passed.")
