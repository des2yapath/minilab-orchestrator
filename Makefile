.PHONY: install test lint demo bench clean
install:
	python -m venv .venv && . .venv/bin/activate && pip install -e ".[dev]"
test:
	pytest -q
lint:
	ruff check .
demo:
	python -m minilab.cli run --scenario basic --time-scale 0.5 --log logs/demo.jsonl
bench:
	python -m minilab.cli bench --blocker none --out results/bench_none.json
	python -m minilab.cli bench --blocker sync --out results/bench_sync.json
	python -m minilab.cli bench --blocker thread --out results/bench_thread.json
	python scripts/plot_latency.py results/bench_sync.json results/bench_thread.json --out docs/img/latency.png
clean:
	rm -rf logs results .pytest_cache **/__pycache__

