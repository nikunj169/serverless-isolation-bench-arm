# Oracle Transfer Instructions

Step 1 — On Oracle instance, clone and setup:

```bash
git clone <repo_url> && cd serverless-bench
bash oracle_setup.sh
# Log out, log back in (docker group change)
bash oracle_setup.sh --skip-install
```

Step 2 — Run full benchmark suite:

```bash
bash run_all.sh --platform oracle_arm64_linux \
                --output-dir ./results/oracle_arm64_linux/
```

Step 3 — Download results to M1 Mac (run locally):

```bash
scp -i ~/.ssh/oracle_key -r \
  opc@<INSTANCE_IP>:~/serverless-bench/results/oracle_arm64_linux/ \
  ./results/oracle_arm64_linux/
```

Step 4 — Verify locally:

```bash
python3 verify_results.py --dir ./results/oracle_arm64_linux/
```
