"""Launch N bidder bots on consecutive ports (9101, 9102, ...).

Usage:
    python bots/run_bidders.py [N] [--base-port 9101]

Each bot runs as its own uvicorn process with a distinct BIDDER_ID and PORT. The
auctioneer's default config fans out to ports 9101..9108, matching ``N = 8``.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run N HypeExchange bidder bots")
    parser.add_argument("n", nargs="?", type=int, default=8, help="number of bots")
    parser.add_argument("--base-port", type=int, default=9101)
    parser.add_argument("--slow-probability", default=os.getenv("SLOW_PROBABILITY", "0.05"))
    args = parser.parse_args()

    procs = []
    for i in range(args.n):
        port = args.base_port + i
        env = dict(os.environ)
        env["PORT"] = str(port)
        env["BIDDER_ID"] = f"bidder-{i + 1}"
        env["SLOW_PROBABILITY"] = str(args.slow_probability)
        cmd = [
            sys.executable, "-m", "uvicorn", "bidder:app",
            "--host", "0.0.0.0", "--port", str(port), "--log-level", "warning",
        ]
        print(f"starting {env['BIDDER_ID']} on :{port}")
        procs.append(subprocess.Popen(cmd, cwd=HERE, env=env))

    def shutdown(*_):
        print("\nstopping bidders...")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"{args.n} bidders running on ports "
          f"{args.base_port}..{args.base_port + args.n - 1}. Ctrl-C to stop.")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
